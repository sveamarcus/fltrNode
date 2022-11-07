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

final class PendingToValueHandler: ChannelOutboundHandler {
    typealias OutboundIn = QueueHandler.OutboundIn
    typealias OutboundOut = BitcoinCommandValue
    
    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let unwrap = self.unwrapOutboundIn(data)

        let outboundOut: BitcoinCommandValue
        switch unwrap {
        case .immediate(let bitcoinCommandValue): outboundOut = bitcoinCommandValue
        case .pending(let pendingBitcoinCommand): outboundOut = pendingBitcoinCommand.bitcoinCommandValue
        }
        context.write(self.wrapOutboundOut(outboundOut), promise: promise)
    }
}
