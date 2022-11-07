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

final class SequenceFuture<InputType, ResponseType> {
    enum FirstFinished {
        case finished
        case waiting

        var isWaiting: Bool {
            switch self {
            case .finished:
                return false
            case .waiting:
                return true
            }
        }
    }

    private var firstFinished: FirstFinished = .waiting
    private let eventLoop: EventLoop
    private let promise: Promise<ResponseType>
    private let lock: NIOConcurrencyHelpers.NIOLock = .init()
    private var queue: CircularBuffer<InputType>
    
    private var futureFactory: ((ResponseType, InputType) -> Future<ResponseType>)?
    
    init<S: Sequence>(_ queue: S,
                      eventLoop: EventLoop,
                      future: @escaping (ResponseType, InputType) -> Future<ResponseType>)
    where S.Element == InputType {
        self.queue = .init()
        self.queue.append(contentsOf: queue)
        self.futureFactory = future
        self.promise = eventLoop.makePromise()
        self.eventLoop = eventLoop
    }

    func start(_ first: ResponseType) -> Future<ResponseType> {
        self.nextInvocation(first)
        
        return promise.futureResult
    }
    
    private func invoke(_ response: ResponseType, _ next: InputType) {
        guard let futureFactory = self.lock.withLock({
            self.futureFactory
        }) else {
            self.promise.fail(EventLoopQueueingCancel())
            return
        }
        
        let future = futureFactory(response, next)
        
        let hasFinished = self.lock.withLock { self.firstFinished }
        switch hasFinished {
        case .waiting:
            future.whenComplete { _ in
                self.lock.withLockVoid {
                    self.firstFinished = .finished
                }
            }
        case .finished:
            break
        }
        
        future.whenComplete {
            switch $0 {
            case .success(let response):
                self.nextInvocation(response)
            case .failure(let error):
                self._cancel()
                self.promise.fail(error)
            }
        }
    }
    
    private func nextInvocation(_ response: ResponseType) {
        guard let next = self.lock.withLock({
            self.queue.popFirst()
        }) else {
            self._cancel()
            self.promise.succeed(response)
            return
        }

        self.invoke(response, next)
    }

    private func _cancel() {
        self.lock.withLockVoid {
            self.queue.removeAll()
            self.firstFinished = .finished
            self.futureFactory = nil
        }
    }
}

typealias SequenceFutureCounted<ResponseType> = SequenceFuture<Void, ResponseType>
extension SequenceFuture where InputType == Void {
    convenience init(count: Int,
                     eventLoop: EventLoop,
                     future: @escaping (Int, ResponseType) -> Future<ResponseType>) {
        precondition(count > 0)
        var i = 0
        self.init(Array<Void>(repeating: (), count: count),
                  eventLoop: eventLoop) { response, _ in
            defer { i += 1}
            return future(i, response)
        }
    }
}

typealias SequenceFutureNonRecursive<InputType> = SequenceFuture<InputType, Void>
extension SequenceFuture where ResponseType == Void {
    convenience init<S: Sequence>(_ queue: S,
                                  eventLoop: EventLoop,
                                  future: @escaping (InputType) -> Future<Void>)
    where S.Element == InputType {
        self.init(queue,
                  eventLoop: eventLoop) { _, input in
            future(input)
        }
    }
    
    func start() -> Future<Void> {
        self.start(())
    }
}

extension SequenceFuture: AlmostCompleteProtocol {
    var almostComplete: Bool {
        self.lock.withLock {
            self.queue.isEmpty && !self.firstFinished.isWaiting
        }
    }
}

extension SequenceFuture: EventLoopQueueingCancellable {
    func cancel() {
        self._cancel()
        self.promise.fail(EventLoopQueueingCancel())
    }
}
