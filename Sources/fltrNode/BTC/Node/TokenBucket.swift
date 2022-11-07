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

final class TokenBucket<ResponseType>: AlmostCompleteProtocol, EventLoopQueueingCancellable {
    private struct PendingTask {
        var task: () -> EventLoopFuture<ResponseType>
        var promise: EventLoopPromise<ResponseType>
    }

    private let eventLoop: EventLoop
    private var queue: CircularBuffer<PendingTask>
    private let maxConcurrentInvocations: Int
    private var currentInvocations: Int

    init(maxConcurrentInvocations: Int,
         eventLoop: EventLoop) {
        self.queue = CircularBuffer(initialCapacity: maxConcurrentInvocations)
        self.maxConcurrentInvocations = maxConcurrentInvocations
        self.eventLoop = eventLoop
        self.currentInvocations = 0
    }
    
    func submitWorkItem(_ workItem: @escaping () -> Future<ResponseType>) -> Future<ResponseType> {
        let task = PendingTask(task: workItem,
                               promise: self.eventLoop.makePromise())
        
        if self.currentInvocations < self.maxConcurrentInvocations {
            self.invoke(task)
        } else {
            self.queue.append(task)
        }
        
        return task.promise.futureResult
    }

    var count: Int {
        self.queue.count
    }
    
    private func invoke(_ pending: PendingTask) {
        assert(self.currentInvocations < self.maxConcurrentInvocations)

        self.currentInvocations += 1
        let future = pending.task()
            
        future.whenComplete { result in
            self.currentInvocations -= 1
            self.eventLoop.execute {
                self.invokeIfNeeded()
            }
            pending.promise.completeWith(result)
        }
    }

    private func invokeIfNeeded() {
        if let first = self.queue.popFirst() {
            self.invoke(first)
        }
    }
    
    var almostComplete: Bool {
        self.queue.isEmpty
    }

    func cancel() {
        self.cancel(EventLoopQueueingCancel())
    }
    
    private func cancel(_ error: Error) {
        let copy = queue
        self.queue.removeAll()
        copy.forEach { $0.promise.fail(error) }
    }
}

