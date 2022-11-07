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

final class PendingTimeoutHandler: ChannelOutboundHandler {
    typealias OutboundIn = QueueHandler.OutboundIn
    typealias OutboundOut = QueueHandler.OutboundIn
    
    @Setting(\.BITCOIN_COMMAND_TIMEOUT)
    private var defaultCommandTimeout: TimeAmount
    
    struct PendingTimeout {}
    static let pendingTimeout = PendingTimeout()
    
    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let unwrap = self.unwrapOutboundIn(data)
        switch unwrap {
        case .immediate:
            context.write(data, promise: promise)
        case .pending(let pendingBitcoinCommand):
            promise!.futureResult.whenSuccess { [defaultCommandTimeout] in
                let cancel = context.eventLoop.scheduleTask(
                    in: pendingBitcoinCommand.bitcoinCommandValue.timeout ?? defaultCommandTimeout,
                    { context.fireUserInboundEventTriggered(Self.pendingTimeout) }
                )
                
                pendingBitcoinCommand
                .promise
                .futureResult
                .whenComplete { _ in
                    cancel.cancel()
                }
            }
            context.write(
                data,
                promise: promise
            )
        }
    }
}
