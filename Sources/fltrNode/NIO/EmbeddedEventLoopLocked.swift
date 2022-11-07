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
import Dispatch
import _NIODataStructures
import NIOCore
import NIOConcurrencyHelpers

private final class EmbeddedScheduledTask {
    let id: UInt64
    let task: () -> Void
    let failFn: (Error) -> ()
    let readyTime: NIODeadline
    let insertOrder: UInt64

    init(id: UInt64, readyTime: NIODeadline, insertOrder: UInt64, task: @escaping () -> Void, _ failFn: @escaping (Error) -> ()) {
        self.id = id
        self.readyTime = readyTime
        self.insertOrder = insertOrder
        self.task = task
        self.failFn = failFn
    }

    func fail(_ error: Error) {
        self.failFn(error)
    }
}

extension EmbeddedScheduledTask: Comparable {
    static func < (lhs: EmbeddedScheduledTask, rhs: EmbeddedScheduledTask) -> Bool {
        if lhs.readyTime == rhs.readyTime {
            return lhs.insertOrder < rhs.insertOrder
        } else {
            return lhs.readyTime < rhs.readyTime
        }
    }

    static func == (lhs: EmbeddedScheduledTask, rhs: EmbeddedScheduledTask) -> Bool {
        return lhs.id == rhs.id
    }
}

public final class EmbeddedEventLoopLocked: EventLoop {
    private var lock: NIOConcurrencyHelpers.NIOLock = .init()
    /// The current "time" for this event loop. This is an amount in nanoseconds.
    /* private but tests */ internal var _now: NIODeadline = .uptimeNanoseconds(0)

    private var scheduledTaskCounter: UInt64 = 0
    private var scheduledTasks = PriorityQueue<EmbeddedScheduledTask>()

    /// Keep track of where promises are allocated to ensure we can identify their source if they leak.
    private var _promiseCreationStore: [_NIOEventLoopFutureIdentifier: (file: StaticString, line: UInt)] = [:]

    // The number of the next task to be created. We track the order so that when we execute tasks
    // scheduled at the same time, we may do so in the order in which they were submitted for
    // execution.
    private var taskNumber: UInt64 = 0

    private func nextTaskNumber() -> UInt64 {
        defer {
            self.taskNumber += 1
        }
        return self.taskNumber
    }

    /// - see: `EventLoop.inEventLoop`
    public var inEventLoop: Bool {
        return true
    }

    /// Initialize a new `EmbeddedEventLoop`.
    public init() { }

    /// - see: `EventLoop.scheduleTask(deadline:_:)`
    @discardableResult
    public func scheduleTask<T>(deadline: NIODeadline, _ task: @escaping () throws -> T) -> Scheduled<T> {
        let promise: EventLoopPromise<T> = makePromise()
        self.scheduledTaskCounter += 1
        let task = EmbeddedScheduledTask(id: self.scheduledTaskCounter, readyTime: deadline, insertOrder: self.nextTaskNumber(), task: {
            do {
                promise.succeed(try task())
            } catch let err {
                promise.fail(err)
            }
        }, promise.fail)

        let taskId = task.id
        let scheduled = Scheduled(promise: promise, cancellationTask: {
            self.scheduledTasks.removeFirst { $0.id == taskId }
        })
        scheduledTasks.push(task)
        return scheduled
    }

    /// - see: `EventLoop.scheduleTask(in:_:)`
    @discardableResult
    public func scheduleTask<T>(in: TimeAmount, _ task: @escaping () throws -> T) -> Scheduled<T> {
        return scheduleTask(deadline: self._now + `in`, task)
    }

    /// On an `EmbeddedEventLoop`, `execute` will simply use `scheduleTask` with a deadline of _now_. This means that
    /// `task` will be run the next time you call `EmbeddedEventLoop.run`.
    public func execute(_ task: @escaping () -> Void) {
        self.scheduleTask(deadline: self._now, task)
    }

    /// Run all tasks that have previously been submitted to this `EmbeddedEventLoop`, either by calling `execute` or
    /// events that have been enqueued using `scheduleTask`/`scheduleRepeatedTask`/`scheduleRepeatedAsyncTask` and whose
    /// deadlines have expired.
    ///
    /// - seealso: `EmbeddedEventLoop.advanceTime`.
    public func run() {
        // Execute all tasks that are currently enqueued to be executed *now*.
        self.advanceTime(to: self._now)
    }

    /// Runs the event loop and moves "time" forward by the given amount, running any scheduled
    /// tasks that need to be run.
    public func advanceTime(by increment: TimeAmount) {
        self.advanceTime(to: self._now + increment)
    }

    /// Runs the event loop and moves "time" forward to the given point in time, running any scheduled
    /// tasks that need to be run.
    ///
    /// - Note: If `deadline` is before the current time, the current time will not be advanced.
    public func advanceTime(to deadline: NIODeadline) {
        let newTime = max(deadline, self._now)

        while let nextTask = self.scheduledTasks.peek() {
            guard nextTask.readyTime <= newTime else {
                break
            }

            // Now we want to grab all tasks that are ready to execute at the same
            // time as the first.
            var tasks = Array<EmbeddedScheduledTask>()
            while let candidateTask = self.scheduledTasks.peek(), candidateTask.readyTime == nextTask.readyTime {
                tasks.append(candidateTask)
                self.scheduledTasks.pop()
            }

            // Set the time correctly before we call into user code, then
            // call in for all tasks.
            self._now = nextTask.readyTime

            for task in tasks {
                task.task()
            }
        }

        // Finally ensure we got the time right.
        self._now = newTime
    }

    internal func drainScheduledTasksByRunningAllCurrentlyScheduledTasks() {
        var currentlyScheduledTasks = self.scheduledTasks
        while let nextTask = currentlyScheduledTasks.pop() {
            self._now = nextTask.readyTime
            nextTask.task()
        }
        // Just fail all the remaining scheduled tasks. Despite having run all the tasks that were
        // scheduled when we entered the method this may still contain tasks as running the tasks
        // may have enqueued more tasks.
        while let task = self.scheduledTasks.pop() {
            task.fail(EventLoopError.shutdown)
        }
    }

    /// - see: `EventLoop.close`
    func close() throws {
        // Nothing to do here
    }

    /// - see: `EventLoop.shutdownGracefully`
    public func shutdownGracefully(queue: DispatchQueue, _ callback: @escaping (Error?) -> Void) {
        run()
        queue.sync {
            callback(nil)
        }
    }

    public func _preconditionSafeToWait(file: StaticString, line: UInt) {
        // EmbeddedEventLoop always allows a wait, as waiting will essentially always block
        // wait()
        return
    }

    public func _promiseCreated(futureIdentifier: _NIOEventLoopFutureIdentifier, file: StaticString, line: UInt) {
        precondition(_isDebugAssertConfiguration())
        self.lock.withLockVoid {
            self._promiseCreationStore[futureIdentifier] = (file: file, line: line)
        }
    }

    public func _promiseCompleted(futureIdentifier: _NIOEventLoopFutureIdentifier) -> (file: StaticString, line: UInt)? {
        precondition(_isDebugAssertConfiguration())
        return self.lock.withLock {
            self._promiseCreationStore.removeValue(forKey: futureIdentifier)
        }
    }

    public func _preconditionSafeToSyncShutdown(file: StaticString, line: UInt) {
        // EmbeddedEventLoop always allows a sync shutdown.
        return
    }

    deinit {
        precondition(scheduledTasks.isEmpty, "Embedded event loop freed with unexecuted scheduled tasks!")
    }
}
