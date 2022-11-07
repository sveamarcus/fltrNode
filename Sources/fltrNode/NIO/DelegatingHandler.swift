//===----------------------------------------------------------------------===//
//
// This source file is part of the fltrNode open source project
//
// Copyright (c) 2022 fltrWallet AG and the fltrNode project authors
// Licensed under Apache License v2.0
//
// See LICENSE for license information
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//
import var HaByLo.logger
import NIO

final class DelegatingHandler: ChannelInboundHandler {
    fileprivate let delegate: BitcoinCommandHandlerDelegate
    fileprivate var state: ChannelState
    
    init(_ delegate: BitcoinCommandHandlerDelegate) {
        self.delegate = delegate
        self.state = .open
    }
        
    enum InboundIn {
        case incoming(FrameToCommandHandler.IncomingCommand)
        case unsolicited(FrameToCommandHandler.UnsolicitedCommand)
    }
    
    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let unwrap = self.unwrapInboundIn(data)
        switch unwrap {
        case .incoming(let incomingCommand):
            self.delegate.incoming(incomingCommand)
        case .unsolicited(let unsolicitedCommand):
            self.delegate.unsolicited(unsolicitedCommand)
        }
    }
}

protocol BitcoinCommandHandlerDelegate {
    func channelError(_ error: Error)
    func incoming(_ incomingCommand: FrameToCommandHandler.IncomingCommand)
    func unsolicited(_ unsolicitedCommand: FrameToCommandHandler.UnsolicitedCommand)
}

extension DelegatingHandler {
    fileprivate enum ChannelState {
        case open
        case localClosed
        case remoteClosed
        case closingWithError(Error)
        case error(Error)
        case pendingTimeout
        case pendingThrottleTimeout
    }
}

extension DelegatingHandler {
    enum ChannelStateEvent {
        case cleanup(Error)
    }
}

// Channel state handling
extension DelegatingHandler: ChannelOutboundHandler {
    typealias OutboundIn = QueueHandler.OutboundIn
    typealias OutboundOut = QueueHandler.OutboundIn

    func channelInactive(context: ChannelHandlerContext) {
        switch self.state {
        case .open:
            logger.trace("DelegatingHandler", #function, "- Remote unexpected close of channel")
            self.state = .remoteClosed
            context.triggerUserOutboundEvent(ChannelStateEvent.cleanup(ChannelError.eof), promise: nil)
            self.delegate.channelError(ChannelError.eof)
            context.close(mode: .all, promise: nil)
        case .pendingTimeout, .pendingThrottleTimeout:
            preconditionFailure()
        case .closingWithError, .error, .localClosed, .remoteClosed:
            break
        }
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        switch self.state {
        case .open:
            logger.error("DelegatingHandler", #function, "- Error caught on open channel (", error, ")")
            self.state = .closingWithError(error)
            context.triggerUserOutboundEvent(ChannelStateEvent.cleanup(error), promise: nil)
            self.delegate.channelError(error)
            context.channel.close(mode: .all, promise: nil)
        case .pendingTimeout, .pendingThrottleTimeout:
            preconditionFailure()
        case .error(let priorError), .closingWithError(let priorError):
            logger.error("DelegatingHandler", #function,
                         "- Error (", error, ") caught on already failed (", priorError, ") channel")
        case .localClosed, .remoteClosed:
            logger.error("DelegatingHandler", #function, "- Error (", error, ") caught on already closed channel")
        }
        context.fireErrorCaught(error)
    }

    func close(context: ChannelHandlerContext, mode: CloseMode, promise: EventLoopPromise<Void>?) {
        switch self.state {
        case .open:
            logger.trace("DelegatingHandler", #function, "- Close correctly called on open channel")
            self.state = .localClosed
            context.triggerUserOutboundEvent(ChannelStateEvent.cleanup(ChannelError.outputClosed), promise: nil)
        case .pendingThrottleTimeout:
            logger.trace("DelegatingHandler", #function, "- Close channel due to throttle time out")
            self.state = .error(BitcoinNetworkError.pendingCommandStallingTimeout)
            context.triggerUserOutboundEvent(ChannelStateEvent.cleanup(BitcoinNetworkError.pendingCommandStallingTimeout),
                                             promise: nil)
            self.delegate.channelError(BitcoinNetworkError.pendingCommandStallingTimeout)
        case .pendingTimeout:
            logger.trace("DelegatingHandler", #function, "- Close channel due to pending time out")
            self.state = .error(BitcoinNetworkError.pendingCommandTimeout)
            context.triggerUserOutboundEvent(ChannelStateEvent.cleanup(BitcoinNetworkError.pendingCommandTimeout),
                                             promise: nil)
            self.delegate.channelError(BitcoinNetworkError.pendingCommandTimeout)
        case .closingWithError(let error):
            self.state = .error(error)
        case .error(let error):
            logger.trace("DelegatingHandler", #function, "- Close called on channel with error(", error, ")")
        case .localClosed:
            logger.trace("DelegatingHandler", #function, "- Close called on *locally* closed channel")
        case .remoteClosed:
            logger.trace("DelegatingHandler", #function, "- Close called on *remotely* closed channel")
        }
        context.close(promise: promise)
    }
    
    // MARK: Pending Timeouts
    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        switch event {
        case is PendingCommandHandler.PendingThrottleTimeout:
            switch self.state {
            case .open:
                self.state = .pendingThrottleTimeout
                logger.error("DelegatingCommandHandler", #function,
                             "- Pending handler notified *throttle* timeout, closing channel.")
                self.close(context: context, mode: .all, promise: nil)
            case .pendingThrottleTimeout:
                preconditionFailure()
            case .localClosed, .remoteClosed, .pendingTimeout, .closingWithError, .error:
                break
            }
        case is PendingTimeoutHandler.PendingTimeout:
            switch self.state {
            case .open:
                self.state = .pendingTimeout
                logger.error("DelegatingCommandHandler", #function,
                             "- Pending handler notified *command* timeout, closing channel.")
                self.close(context: context, mode: .all, promise: nil)
            case .pendingTimeout:
                preconditionFailure()
            case .localClosed, .remoteClosed, .pendingThrottleTimeout, .closingWithError, .error:
                break
            }
        default:
            context.fireUserInboundEventTriggered(event)
        }
    }
}

extension DelegatingHandler.ChannelState: Equatable {
    static func == (lhs: Self, rhs: Self) -> Bool {
        switch (lhs, rhs) {
        case (.open, .open),
             (.localClosed, .localClosed),
             (.remoteClosed, .remoteClosed),
             (.pendingTimeout, .pendingTimeout),
             (.pendingThrottleTimeout, .pendingThrottleTimeout),
             (.closingWithError, .closingWithError),
             (.error, .error):
            return true
        case (.open, _),
             (.localClosed, _),
             (.remoteClosed, _),
             (.pendingTimeout, _),
             (.pendingThrottleTimeout, _),
             (.closingWithError, _),
             (.error, _):
            return false
        }
    }
}

protocol CleanupLocalStateEvent: ChannelOutboundHandler {
    func cleanup(error: Error)
}

extension CleanupLocalStateEvent {
    func triggerUserOutboundEvent(context: ChannelHandlerContext, event: Any, promise: EventLoopPromise<Void>?) {
        if case let channelEvent as DelegatingHandler.ChannelStateEvent = event,
            case .cleanup(let error) = channelEvent {
            self.cleanup(error: error)
            promise?.succeed(())
            context.triggerUserOutboundEvent(event, promise: nil)
        } else {
            context.triggerUserOutboundEvent(event, promise: promise)
        }
    }
}
