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
import struct Foundation.Date

final class VersionHandshakeHandler: ChannelInboundHandler, RemovableChannelHandler {
    typealias InboundIn = FrameToCommandHandler.InboundOut
    typealias InboundOut = FrameToCommandHandler.InboundOut

    private var stateMachine: StateMachine
    
    init(done versionPromise: Promise<VersionCommand>, outgoing versionCommand: VersionCommand) {
        self.stateMachine = .init()
        try! self.stateMachine.initialize(versionPromise, versionCommand)
    }
    
    func channelActive(context: ChannelHandlerContext) {
        do {
            try self.stateMachine.activate(channel: context.channel)
        } catch {
            self.stateMachine.fail(error, pipeline: context.pipeline)
        }
        context.fireChannelActive()
    }
    
    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let unwrap = self.unwrapInboundIn(data)

        switch unwrap {
        case .pending(let pending as VerackCommand):
            do {
                try self.stateMachine.read(pending, channel: context.channel, handler: self)
            } catch {
                self.stateMachine.fail(error, pipeline: context.pipeline)
            }
        case .incoming(.version(let versionCommand)):
            do {
                try self.stateMachine.read(versionCommand, channel: context.channel, handler: self)
            } catch {
                self.stateMachine.fail(error, pipeline: context.pipeline)
            }
        case .incoming, .pending, .unsolicited:
            logger.error("IncomingVersionHandler", #function, "- Expected version received (", unwrap, ")")
            self.stateMachine.fail(BitcoinNetworkError.illegalStateExpectedVersionCommand,
                                   pipeline: context.pipeline)
        }
    }
    
    func channelInactive(context: ChannelHandlerContext) {
        self.stateMachine.fail(BitcoinNetworkError.channelClosed, pipeline: context.pipeline)
        context.fireChannelInactive()
    }
}

extension VersionHandshakeHandler {
    struct StateMachine {
        enum State {
            case initialized(done: Promise<VersionCommand>, outgoing: VersionCommand)
            case awaitingVersionAndVerack(done: Promise<VersionCommand>, cancel: Scheduled<Void>)
            case awaitingVerackVersionReceived(done: Promise<VersionCommand>, version: VersionCommand, cancel: Scheduled<Void>)
            case awaitingVersionVerackReceived(done: Promise<VersionCommand>, cancel: Scheduled<Void>)
            case done
        }

        @Setting(\.BITCOIN_COMMAND_TIMEOUT)
        private var timeout: TimeAmount

        private var state: State
        
        init() {
            self.state = .done
        }
        
        enum SMError: Error {
            case invalidTransition(state: State, event: StaticString)
        }
        
        mutating func initialize(_ versionPromise: Promise<VersionCommand>, _ versionCommand: VersionCommand) throws {
            switch self.state {
            case .done:
                self.state = .initialized(done: versionPromise, outgoing: versionCommand)
            case .awaitingVerackVersionReceived,
                 .awaitingVersionAndVerack,
                 .awaitingVersionVerackReceived,
                 .initialized:
                throw SMError.invalidTransition(state: self.state, event: #function)
            }
        }
        
        mutating func activate(channel: Channel) throws {
            switch self.state {
            case .initialized(let promise, let versionCommand):
                let cancelTimeout = promise.futureResult.eventLoop.scheduleTask(in: self.timeout) {
                    promise.fail(BitcoinNetworkError.pendingCommandTimeout)
                    channel.pipeline.fireErrorCaught(BitcoinNetworkError.pendingCommandTimeout)
                }
                self.state = .awaitingVersionAndVerack(done: promise, cancel: cancelTimeout)
                channel.writeAndFlush(QueueHandler.OutboundIn.immediate(versionCommand))
                .cascadeFailure(to: promise)
            case .awaitingVersionAndVerack:
                break
            case .done, .awaitingVerackVersionReceived, .awaitingVersionVerackReceived:
                throw SMError.invalidTransition(state: self.state, event: #function)
            }
        }
        
        private func logSuccess(channel: Channel, version command: VersionCommand) {
            let remoteAddress = try! channel.remoteAddress ?? .init(ipAddress: "0.0.0.0", port: 0)
            let ipString: String = {
                if let ipV4 = remoteAddress.ipAddress.asIPv4 {
                    return "\(ipV4)"
                } else {
                    return "\(remoteAddress.ipAddress)"
                }
            }()
            logger.trace("VersionHandshakeHandler - 🦚 Version handshake"
                            + "finished SUCCESSFULLY. Cancelling timeout."
                            + "\n🟫🟫🟫 VERSION(\(ipString):"
                            + "\(String(describing: channel.remoteAddress?.ipPort ?? 0))) "
                            + "[\(String(reflecting: command))]")
        }
        
        mutating func read(_ verack: VerackCommand,
                           channel: Channel,
                           handler: VersionHandshakeHandler) throws {
            switch self.state {
            case .awaitingVersionAndVerack(let promise, let cancel):
                self.state = .awaitingVersionVerackReceived(done: promise, cancel: cancel)
            case .awaitingVerackVersionReceived(let promise, let command, let cancel):
                self.state = .done
                self.logSuccess(channel: channel, version: command)
                cancel.cancel()
                channel.pipeline.removeHandler(handler).map {
                    command
                }
                .cascade(to: promise)
            case .awaitingVersionVerackReceived, .done, .initialized:
                throw SMError.invalidTransition(state: self.state, event: #function)
            }
        }
        
        mutating func read(_ version: VersionCommand,
                           channel: Channel,
                           handler: VersionHandshakeHandler) throws {
            switch self.state {
            case .awaitingVersionAndVerack(let promise, let cancel):
                self.state = .awaitingVerackVersionReceived(done: promise,
                                                            version: version,
                                                            cancel: cancel)
                channel.writeAndFlush(
                    QueueHandler.OutboundIn.immediate(VerackCommand())
                )
                .cascadeFailure(to: promise)
            case .awaitingVersionVerackReceived(let promise, let cancel):
                self.state = .done
                self.logSuccess(channel: channel, version: version)
                cancel.cancel()
                channel.writeAndFlush(
                    QueueHandler.OutboundIn.immediate(VerackCommand())
                )
                .flatMap {
                    channel.pipeline.removeHandler(handler)
                }
                .map {
                    version
                }
                .cascade(to: promise)
            case .awaitingVerackVersionReceived,
                 .done,
                 .initialized:
                throw SMError.invalidTransition(state: self.state, event: #function)
            }
        }
        
        mutating func fail(_ error: Error, pipeline: ChannelPipeline) {
            switch self.state {
            case .awaitingVerackVersionReceived(let promise, _, let cancel),
                 .awaitingVersionAndVerack(let promise, let cancel),
                 .awaitingVersionVerackReceived(let promise, let cancel):
                self.state = .done
                cancel.cancel()
                fallthrough
            case .initialized(let promise, _):
                promise.fail(error)
                pipeline.fireErrorCaught(error)
            case .done:
                break
            }
        }
    }
}
