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

final class DeferredPriorityFuture<InputType, ResponseType> {
    enum DeferredPriority: Hashable {
        case must
        case may
        case skip
    }

    struct Skip: Swift.Error {}

    private var priorities: ArraySlice<DeferredPriority>
    let skipResult: (priority: DeferredPriority, deferred: () -> Future<ResponseType>)
    let factory: (InputType) -> Future<ResponseType>
    
    private init(priorities: ArraySlice<DeferredPriority>, eventLoop: EventLoop, factory: @escaping (InputType) -> Future<ResponseType>) {
        self.priorities = priorities
        self.skipResult = (priority: .skip, deferred: { eventLoop.makeFailedFuture(Skip()) })
        self.factory = factory
    }
    
    func next(input: InputType) -> (priority: DeferredPriority, deferred: () -> Future<ResponseType>) {
        guard let priority = self.priorities.popFirst()
        else {
            return self.skipResult
        }
        
        let deferred = {
            self.factory(input)
        }
        
        return (priority, deferred)
    }
    
    static func must(then may: Int, eventLoop: EventLoop, deferred: @escaping (InputType) -> Future<ResponseType>) -> Self {
        let priorities = ArraySlice([ [ DeferredPriority.must ], (0..<may).map { _ in DeferredPriority.may } ].joined())
        return Self.init(priorities: priorities,
                         eventLoop: eventLoop,
                         factory: deferred)
    }
    
    static func may(_ may: Int, eventLoop: EventLoop, deferred: @escaping (InputType) -> Future<ResponseType>) -> Self {
        let priorities = ArraySlice((0..<may).map { _ in DeferredPriority.may })
        return Self.init(priorities: priorities,
                         eventLoop: eventLoop,
                         factory: deferred)
    }
    
    static func skip(eventLoop: EventLoop, deferred: @escaping (InputType) -> Future<ResponseType>) -> Self {
        return Self.init(priorities: [][...], eventLoop: eventLoop, factory: deferred)
    }
}

typealias DeferredPriorityFeedback<ResponseType> = DeferredPriorityFuture<ResponseType, ResponseType>

