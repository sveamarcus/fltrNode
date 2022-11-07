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
import Dispatch

extension Future {
    @usableFromInline
    func hopMap<T>(to eventLoop: EventLoop,
                   _ transform: @escaping (Value) throws -> T) -> EventLoopFuture<T> {
        if eventLoop.inEventLoop {
            return self.flatMapThrowing(transform)
        } else {
            let promise = eventLoop.makePromise(of: T.self)
            
            self.whenComplete {
                switch $0 {
                case .success(let value):
                    promise.completeWith(
                        eventLoop.submit { try transform(value) }
                    )
                case .failure(let error):
                    promise.fail(error)
                }
            }
            
            return promise.futureResult
        }
    }

    @usableFromInline
    func hopMap<T>(to eventLoop: EventLoop,
                   by threadPool: NIOThreadPool,
                   _ transform: @escaping (Value) throws -> T) -> EventLoopFuture<T> {
        let promise = eventLoop.makePromise(of: T.self)

        self.whenComplete {
            switch $0 {
            case .success(let value):
                let workItem: NIOThreadPool.WorkItem = { state in
                    switch state {
                    case .active:
                        do {
                            promise.succeed(try transform(value))
                        } catch {
                            promise.fail(error)
                        }
                    case .cancelled:
                        promise.fail(ChannelError.ioOnClosedChannel)
                    }
                }
                threadPool.submit(workItem)
            case .failure(let error):
                promise.fail(error)
            }
        }

        return promise.futureResult
    }
    
    @usableFromInline
    func hopMap<T>(to eventLoop: EventLoop,
                   by queue: DispatchQueue,
                   _ transform: @escaping (Value) throws -> T) -> EventLoopFuture<T> {
        let promise = eventLoop.makePromise(of: T.self)

        self.whenComplete {
            switch $0 {
            case .success(let value):
                queue.async {
                    do {
                        promise.succeed(
                            try transform(value)
                        )
                    } catch {
                        promise.fail(error)
                    }
                }
            case .failure(let error):
                promise.fail(error)
            }
        }

        return promise.futureResult
    }
}
