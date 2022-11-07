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
import HaByLo
import NIO
import NIOConcurrencyHelpers

struct UnsolicitedFilters {
    let addrCache: RemoveDuplicatesFilter<[PeerAddress]> = .init()
    let blockCache: RemoveDuplicatesFilter<BlockChain.Hash<BlockHeaderHash>> = .init()
    let txCache: RemoveDuplicatesFilter<BlockChain.Hash<TransactionLegacyHash>> = .init()
    
    func unsolicited(addr: [PeerAddress], source: Client.Key) -> DuplicatesResult {
        self.addrCache.filter(addr)
    }
    
    func unsolicited(block: BlockChain.Hash<BlockHeaderHash>, source: Client.Key) -> DuplicatesResult {
        self.blockCache.filter(block)
    }
    
    func unsolicited(tx: BlockChain.Hash<TransactionLegacyHash>, source: Client.Key) -> DuplicatesResult {
        self.txCache.filter(tx)
    }
}

extension UnsolicitedFilters {
    enum DuplicatesResult {
        case block
        case pass
    }
    
    struct Cache<T: Hashable> {
        private var fifo: CircularBuffer<T> = .init(initialCapacity: 16)
        private var set: Set<T> = []
        
        init(initialCapacity: Int? = nil) {
            self.fifo = .init(initialCapacity: initialCapacity ?? 16)
        }
        
        func contains(_ value: T) -> Bool {
            return set.contains(value)
        }
        
        mutating func add(_ value: T) {
            guard !self.set.contains(value)
            else { return }
            self.fifo.append(value)
            precondition(self.set.insert(value).inserted)
        }
        
        mutating func removeFirst(count: Int = 1) {
            for _ in 0..<count {
                guard let first = self.fifo.popFirst()
                else { preconditionFailure() }
                self.set.remove(first)
            }
        }
        
        var count: Int {
            return self.fifo.count
        }
    }

    final class RemoveDuplicatesFilter<T: Hashable> {
        let lock: NIOConcurrencyHelpers.NIOLock = .init()
        var cache: Cache<T>
        
        private let cacheDepth: Int
        
        init() {
            self.cacheDepth = Settings.BITCOIN_UNSOLICITED_FILTER_CACHE
            self.cache = .init(initialCapacity: self.cacheDepth + 10)
        }
        
        func filter(_ value: T) -> DuplicatesResult {
            let contains = self.lock.withLock {
               self.cache.contains(value)
            }
            
            if contains {
                return .block
            } else {
                return self.lock.withLock {
                    if cache.count >= self.cacheDepth {
                        cache.removeFirst(count: max(1, cacheDepth >> 4))
                    }
                    cache.add(value)

                    return .pass
                }
            }
        }
    }
}
