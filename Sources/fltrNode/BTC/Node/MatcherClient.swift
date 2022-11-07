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
import struct fltrTx.ScriptPubKey
import FastrangeSipHash
import HaByLo
import NIO
#if canImport(UIKit)
import UIKit
#endif
#if canImport(MetalKit)
import MetalKit
#endif

extension CF {
    struct MatcherClient {
        @usableFromInline
        let updateOpcodes0: ([[UInt8]]) -> Void
        
        @inlinable
        public func updateOpcodes(_ opcodes: [[UInt8]]) {
            self.updateOpcodes0(opcodes)
        }
        
        @usableFromInline
        let match0: (_ opcodes: [[UInt8]],
                     _ key: [UInt8],
                     _ f: UInt64,
                     _ filters: [UInt64],
                     _ eventLoop: EventLoop) -> Future<Bool>
        @inlinable
        public func match(opcodes: [[UInt8]],
                          key: [UInt8],
                          f: UInt64,
                          filters: [UInt64],
                          eventLoop: EventLoop) -> Future<Bool> {
            self.match0(opcodes, key, f, filters, eventLoop)
        }
        
        @usableFromInline
        let start0: (_ eventLoop: EventLoop) -> Future<Void>
        @inlinable
        public func start(eventLoop: EventLoop) -> Future<Void> {
            self.start0(eventLoop)
        }

        @usableFromInline
        let stop0: () -> Void
        @inlinable
        public func stop() {
            self.stop0()
        }

        public init(updateOpcodes: @escaping ([[UInt8]]) -> Void,
                    match: @escaping (_ opcodes: [[UInt8]],
                                      _ key: [UInt8],
                                      _ f: UInt64,
                                      _ filters: [UInt64],
                                      _ eventLoop: EventLoop) -> Future<Bool>,
                    start: @escaping (_ eventLoop: EventLoop) -> Future<Void>,
                    stop: @escaping () -> Void)  {
            self.updateOpcodes0 = updateOpcodes
            self.match0 = match
            self.start0 = start
            self.stop0 = stop
        }
        
        public init(updateOpcodes: @escaping ([[UInt8]]) -> Void,
                    match: @escaping (_ opcodes: [[UInt8]],
                                      _ key: [UInt8],
                                      _ f: UInt64,
                                      _ filters: [UInt64],
                                      _ eventLoop: EventLoop) -> Future<Bool>) {
            self.init(updateOpcodes: updateOpcodes,
                      match: match,
                      start: { $0.makeSucceededFuture(()) },
                      stop: { })
        }
    }
}

import Combine
public var SUPPORTS_METAL = CurrentValueSubject<Bool?, Never>(nil)

#if canImport(fltrJET)
import fltrJET
extension CF.MatcherClient {
    static func supportsMetal() -> Bool {
        #if canImport(MetalKit)
        func _supportsMetal() -> Bool {
            guard let device = MTLCreateSystemDefaultDevice()
            else { return false }

            var buffer = device.makeBuffer(length: 16, options: [])
            defer { buffer = nil }

            guard buffer != nil
            else { return false }
            
            #if targetEnvironment(simulator)
            return true
            #else
            return device.supportsFamily(.apple6)
                || device.supportsFamily(.common3)
                || device.supportsFamily(.mac2)
                || device.supportsFamily(.macCatalyst2)
            #endif
        }
        #else
        func _supportsMetal() -> Bool {
            return false
        }
        #endif

        let result = _supportsMetal()
        SUPPORTS_METAL.send(result)
        
        return result
    }
}

extension CF.MatcherClient {
    static var jet: () -> Self = {
        {
            guard Self.supportsMetal()
            else {
                return Self.reference()
            }

            let jetMatcher = JETMatcher()
            let reference = Self.reference()
            
            func fullMatch(opcodes: [[UInt8]],
                           key: [UInt8],
                           f: UInt64,
                           filters: [UInt64],
                           eventLoop: EventLoop) -> Future<Bool> {
                let filters64 = Array(filters)
                let filters32 = filters64.map { UInt32(truncatingIfNeeded: $0) }
                
                let promise = eventLoop.makePromise(of: Bool.self)
                
                let isActive: Bool = {
            #if canImport(UIKit)
                    switch UIApplication.shared.applicationState {
                    case .active, .inactive:
                        return true
                    case .background:
                        return false
                    @unknown default:
                        fatalError()
                    }
            #else
                    return true
            #endif
                }()

                
                if filters64.count < JET.FilterCountCapacity,
                   isActive {
                    jetMatcher.match(
                        filter: filters32,
                        key: key,
                        f: f,
                        callback: { result in
                            switch result {
                            case .success(true):
                                // It's a 32 bit match, we need to ensure it's also a full 64 bit match through
                                // reference matcher
                                reference.match(opcodes: opcodes,
                                                key: key,
                                                f: f,
                                                filters: filters64,
                                                eventLoop: eventLoop)
                                .whenComplete {
                                    switch $0 {
                                    case .success(true):
                                        promise.succeed(true)
                                    case .success(false):
                                        logger.info("MatcherClient.jet() - False positive 🥸 match "
                                        + "for fltrJET matcher")
                                        promise.succeed(false)
                                    case .failure(let error):
                                        promise.fail(error)
                                    }
                                }
                            case .success(false):
                                promise.succeed(false)
                            case .failure(let error):
                                promise.fail(error)
                            }
                        }
                    )
                } else { // If filter count is over fltrJET capacity or backgrounded,
                         // use CPU bound reference method instead
                    promise.completeWith(
                        reference.match(opcodes: opcodes,
                                        key: key,
                                        f: f,
                                        filters: filters64,
                                        eventLoop: eventLoop)
                    )
                }
                
                return promise.futureResult
            }
            
            func start(eventLoop: EventLoop) -> Future<Void> {
                let promise = eventLoop.makePromise(of: Void.self)
                
                jetMatcher.start { result in
                    switch result {
                    case .success:
                        promise.succeed(())
                    case .failure(let error):
                        promise.fail(error)
                    }
                }
                
                return reference.start(eventLoop: eventLoop)
                    .flatMap {
                        promise.futureResult
                    }
            }
            
            func stop() {
                jetMatcher.stop()
                reference.stop()
            }
            
            return .init(updateOpcodes: jetMatcher.reload(opcodes:),
                         match: fullMatch(opcodes:key:f:filters:eventLoop:),
                         start: start(eventLoop:),
                         stop: stop)
        }()
    }
}
#endif


extension CF.MatcherClient {
    static var always: () -> Self = {
        .init(updateOpcodes: { _ in },
              match: { _, _, _, _, eventLoop in eventLoop.makeSucceededFuture(true) })
    }
    static var never: () -> Self = {
        .init(updateOpcodes: { _ in },
              match: { _, _, _, _, eventLoop in eventLoop.makeSucceededFuture(false) })
    }
    static var fail: () -> Self = {
        .init(updateOpcodes: { _ in },
              match: { _, _, _, _, eventLoop in
                struct AlwaysFail: Swift.Error {}
                return eventLoop.makeFailedFuture(AlwaysFail()) })
    }
}

extension CF.MatcherClient {
    @usableFromInline
    static func cfHash(_ bytes: [UInt8], _ key: [UInt8], _ f: UInt64) -> UInt64 {
        let sipHash = siphash(input: bytes, key: key)
        return fastrange(sipHash, f)
    }

    
    static var reference: () -> Self = {
        {
            func gcsMultiMatch(opcodeHashes: [UInt64], filters: [UInt64], key: [UInt8], f: UInt64) -> Bool {
                var index = opcodeHashes.startIndex
                
                outer:
                for value in filters {
                    inner:
                    for target in opcodeHashes[index...] {
                        if target == value {
                            return true
                        }
                        
                        if target > value {
                            break inner
                        }
                        
                        guard index < opcodeHashes.endIndex else {
                            break outer
                        }
                        
                        opcodeHashes.formIndex(after: &index)
                    }
                }
                return false
            }
            
            func match64(opcodes: [[UInt8]],
                         key: [UInt8],
                         f: UInt64,
                         filters: [UInt64],
                         eventLoop: EventLoop) -> Future<Bool> {
                var opcodeHashes: [UInt64] = opcodes.map {
                    Self.cfHash($0, key, f)
                }
                opcodeHashes.sort()
                
                return eventLoop.makeSucceededFuture(
                    gcsMultiMatch(opcodeHashes: opcodeHashes, filters: filters, key: key, f: f)
                )
            }
            
            return .init(updateOpcodes: { _ in return },
                         match: match64(opcodes:key:f:filters:eventLoop:))
        }()
    }
}
