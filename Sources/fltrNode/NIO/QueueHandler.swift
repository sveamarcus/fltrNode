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
import NIOConcurrencyHelpers

struct QueuedBitcoinCommand {
    let pendingBitcoinCommand: PendingBitcoinCommand
    let writePromise: Promise<Void>
    
    let commandWriter: (Promise<Void>) -> Void
}

final class QueueHandler {
    private let lock: NIOConcurrencyHelpers.NIOLock = .init()
    private var fifo = CircularBuffer<QueuedBitcoinCommand>(initialCapacity: 32)
    private var isQueueing: Bool = false

    var count: Int {
        self.lock.withLock {
            self.fifo.count
        }
    }
    
    struct QueueError: Swift.Error {
        var queueItem: QueuedBitcoinCommand
        var errorType: Error
    }
    
    func deQueue() {
        let queued: QueuedBitcoinCommand? = self.lock.withLock {
            self.fifo.popFirst()
        }
        
        queued.map {
            $0.commandWriter($0.writePromise)
        }
    }
}

extension QueueHandler: ChannelOutboundHandler {
    typealias OutboundOut = OutboundIn

    enum OutboundIn {
        case immediate(BitcoinCommandValue)
        case pending(PendingBitcoinCommand)
    }

    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let unwrap = self.unwrapOutboundIn(data)
        switch unwrap {
        case .immediate:
            context.write(data, promise: promise)
        case .pending(let pendingBitcoinCommand):
            if isQueueing {
                let queuedBitcoinCommand = QueuedBitcoinCommand(
                    pendingBitcoinCommand: pendingBitcoinCommand,
                    writePromise: promise!,
                    commandWriter: { promise in
                        self.isQueueing = true
                        context.writeAndFlush(data, promise: promise)
                    }
                )
                
                self.lock.withLockVoid {
                    self.fifo.append(queuedBitcoinCommand)
                }
            } else {
                self.isQueueing = true
                context.write(data, promise: promise)
            }
        }
    }
}

extension QueueHandler: ChannelInboundHandler {
    typealias InboundIn = DelegatingHandler.InboundIn
    typealias InboundOut = DelegatingHandler.InboundIn
    
    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        switch event {
        case let event as PendingCommandHandler.PendingState where event == .dequeue:
            self.isQueueing = false
            self.deQueue()
        default:
            context.fireUserInboundEventTriggered(event)
        }
    }
}

extension QueueHandler: CleanupLocalStateEvent {
    func cleanup(error: Error) {
        let queue: CircularBuffer<QueuedBitcoinCommand>? = self.lock.withLock {
            let copy = self.fifo
            self.fifo.removeAll()
            return copy
        }
        
        self.isQueueing = false

        queue?.forEach {
            $0.writePromise.fail(error)
        }
    }
}
