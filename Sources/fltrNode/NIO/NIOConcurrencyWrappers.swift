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
import Foundation
import NIOConcurrencyHelpers

@propertyWrapper
public struct NIOLock<Value> {
    @usableFromInline
    var value: Value
    @usableFromInline
    let lock = NIOConcurrencyHelpers.NIOLock()
    
    @inlinable
    public init(wrappedValue initialValue: Value) {
        value = initialValue
    }
    
    @inlinable
    public var wrappedValue: Value {
        @inlinable
        get {
            lock.lock()
            defer { lock.unlock() }
            return value
        }
        @inlinable
        set {
            lock.lock()
            defer { lock.unlock() }
            self.value = newValue
        }
    }
}

// Alternative unfair lock implementation
@propertyWrapper
public final class UnfairLock<Value> {
    @usableFromInline
    var mutex = os_unfair_lock()
    @usableFromInline
    var value: Value
    public init(wrappedValue initialValue: Value) {
        value = initialValue
    }
    
    @inlinable
    public var wrappedValue: Value {
        get {
            os_unfair_lock_lock(&mutex)
            defer { os_unfair_lock_unlock(&mutex) }
            return value
        }
        set {
            self.mutate { me in
                me = newValue
            }
        }
    }
    
    public typealias Mutate = (inout Value) -> Void
    @inlinable
    func mutate(_ f: Mutate) {
        os_unfair_lock_lock(&mutex)
        defer { os_unfair_lock_unlock(&mutex) }
        f(&value)
    }
    
    @inlinable
    public var projectedValue: (Mutate) -> Void {
        return mutate(_:)
    }
}

