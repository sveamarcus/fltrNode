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
import NIO

struct PendingBitcoinCommand {
    let promise: EventLoopPromise<BitcoinCommandValue>
    let bitcoinCommandValue: BitcoinCommandValue
}

final class PendingCommandHandler {
    @UnfairLock private var promise: EventLoopPromise<BitcoinCommandValue>? = nil
    var isPending: Bool {
        promise == nil ? false : true
    }
}

extension PendingCommandHandler {
    enum PendingState {
        case dequeue
    }
}

extension PendingCommandHandler: ChannelOutboundHandler {
    typealias OutboundIn = QueueHandler.OutboundIn
    typealias OutboundOut = QueueHandler.OutboundIn
    
    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let unwrap = self.unwrapOutboundIn(data)

        switch unwrap {
        case .immediate://(let bitcoinCommandValue):
            break
        case .pending(let pendingBitcoinCommand):
            guard self.promise == nil else {
                promise!.fail(BitcoinNetworkError.illegalStatePendingPromisesNotNil)
                context.pipeline.fireErrorCaught(BitcoinNetworkError.illegalStatePendingPromisesNotNil)
                return
            }
            pendingBitcoinCommand.promise.futureResult.whenFailure { _ in
                self.promise = nil
            }
            self.promise = pendingBitcoinCommand.promise
        }
        context.write(data, promise: promise)
    }
}

extension PendingCommandHandler: ChannelInboundHandler {
    typealias InboundIn = FrameToCommandHandler.InboundOut
    typealias InboundOut = DelegatingHandler.InboundIn
    
    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let unwrap = unwrapInboundIn(data)
        switch unwrap {
        case .pending(let bitcoinCommandValue):
            guard let promise = self.promise else {
                context.fireErrorCaught(BitcoinNetworkError.illegalStatePendingPromisesNil)
                return
            }
            self.promise = nil
            context.fireUserInboundEventTriggered(PendingState.dequeue)
            promise.succeed(bitcoinCommandValue)
        case .incoming(let incomingCommand):
             context.fireChannelRead(
                self.wrapInboundOut(
                    .incoming(incomingCommand)
                )
             )
        case .unsolicited(let unsolicitedCommand):
            context.fireChannelRead(
                self.wrapInboundOut(
                    .unsolicited(unsolicitedCommand)
                )
            )
        }
    }
}

extension PendingCommandHandler: CleanupLocalStateEvent {
    func cleanup(error: Error) {
        let save = self.promise
        self.promise = nil
        save?.fail(error)
    }
}

extension PendingCommandHandler {
    struct PendingThrottleTimeout {}
    
    static let pendingThrottleTimeout = PendingThrottleTimeout()
    
    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if case let idleStateEvent as IdleStateHandler.IdleStateEvent = event, idleStateEvent == .all {
            if promise != nil {
                context.fireUserInboundEventTriggered(Self.pendingThrottleTimeout)
            }
        } else {
            context.fireUserInboundEventTriggered(event)
        }
    }
}
