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
import struct Foundation.Date
import struct Foundation.TimeInterval

final class BitcoinCommandTimer: Equatable {
    var boxed: TimeInterval = 0

    static func == (lhs: BitcoinCommandTimer, rhs: BitcoinCommandTimer) -> Bool {
        lhs.boxed == rhs.boxed
    }
}

protocol BitcoinLatencyTimerProperty {
    var _latencyTimer: BitcoinCommandTimer { get }
}

extension BitcoinLatencyTimerProperty {
    var latencyTimer: TimeInterval {
        get {
            self._latencyTimer.boxed
        }
        set {
            self._latencyTimer.boxed = newValue
        }
    }
}

final class LatencyDecoratorHandler: ChannelOutboundHandler {
    typealias OutboundIn = QueueHandler.OutboundIn
    typealias OutboundOut = QueueHandler.OutboundIn
    
    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let unwrap = self.unwrapOutboundIn(data)
        
        switch unwrap {
        case .immediate:
            context.write(data, promise: promise)
        case .pending(let pendingBitcoinCommand):
            var writePromise: EventLoopPromise<Void>
            switch promise {
            case .some(let promise):
                writePromise = promise
            case .none:
                writePromise = context.eventLoop.makePromise(of: Void.self, file: #file, line: #line)
            }
            writePromise.futureResult.whenSuccess {
                pendingBitcoinCommand.promise.futureResult.whenSuccess { [dispatchDate = Date()] pendingReply in
                    if case let timerProperty as BitcoinLatencyTimerProperty = pendingReply {
                        timerProperty._latencyTimer.boxed = Date().timeIntervalSince(dispatchDate)
                    }
                }
            }
            context.write(
                data,
                promise: writePromise
            )
        }
    }
}
