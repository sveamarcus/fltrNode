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
import FileRepo
import func HaByLo.zip
import struct Foundation.IndexSet
import NIO


protocol CompactFilterFileRepo: FileRepo {
    var flagIndex: Int { get }
}

struct CompactFilterFileRepoNoMoreRecords: Swift.Error {
}

extension CompactFilterFileRepo {
    func first(processed: Int, fn: StaticString = #function, where predicate: @escaping (File.RecordFlag) -> Bool) -> Future<Model> {
        self.firstIndex(processed: processed, fn: fn, where: predicate)
        .flatMap {
            self.find(id: $0)
        }
    }
    
    func firstIndex(processed: Int, fn: StaticString = #function, where predicate: @escaping (File.RecordFlag) -> Bool) -> Future<Int> {
        self.records(from: max(self.offset, processed), limit: 1, for: predicate)
        .flatMapThrowing {
            if let first = $0.first {
                return first
            } else {
                throw File.Error.noMatchingPredicate(event: fn)
            }
        }
    }
    
    func records(from lowerBound: Int? = nil,
                 to upperBound: Int? = nil,
                 limit matching: Int? = nil,
                 for predicate: @escaping (File.RecordFlag) -> Bool) -> Future<IndexSet> {
        assert(matching == nil || matching! > 0)
        assert(upperBound == nil || lowerBound ?? 0 <= upperBound!)
        
        let promise = self.eventLoop.makePromise(of: IndexSet.self)
        var currentIndex = lowerBound ?? self.offset
        var result: IndexSet = .init()
        
        self.fileSize()
        .flatMap { fileSize -> Future<Void> in
            let offsetLowerFilePosition = zip(lowerBound, self.offset).map(-).map { $0 * self.recordSize } ?? 0
            let offsetUpperFilePosition = zip(upperBound, self.offset).map(-).map { $0 * self.recordSize } ?? fileSize
            let totalByteCount = offsetUpperFilePosition - offsetLowerFilePosition
            
            return self.nonBlockingFileIO.readChunked(
                fileHandle: self.nioFileHandle,
                fromOffset: Int64(offsetLowerFilePosition),
                byteCount: totalByteCount,
                chunkSize: self.recordSize * self.nonBlockingFileIOnumberOfChunks,
                allocator: self.allocator,
                eventLoop: self.eventLoop
            ) { (buffer: ByteBuffer) in
                var buffer = buffer
                for _ in (0..<self.nonBlockingFileIOnumberOfChunks) {
                    guard let record = buffer.readSlice(length: self.recordSize),
                        let flag = record.getInteger(at: self.flagIndex).map(File.RecordFlag.init(rawValue:)) else {
                            return self.eventLoop.makeFailedFuture(
                                CompactFilterFileRepoNoMoreRecords()
                            )
                    }
                    
                    defer {
                        currentIndex += 1
                    }
                    
                    if predicate(flag) {
                        result.insert(currentIndex)
                        
                        if let matching = matching {
                            guard result.count < matching else {
                                return self.eventLoop.makeFailedFuture(
                                    CompactFilterFileRepoNoMoreRecords()
                                )
                            }
                        }
                    }
                }
                return self.eventLoop.makeSucceededFuture(())
            }
        }
        .whenComplete {
            switch $0 {
            case .failure(let error) where error is CompactFilterFileRepoNoMoreRecords:
                fallthrough
            case .success:
                promise.succeed(result)
            case .failure(let error):
                promise.fail(error)
            }
        }

        return promise.futureResult
    }
    
    func processedRecords(processed: Int) -> Future<Int> {
        self.firstIndex(processed: processed, where: { $0.isDisjoint(with: .processed) })
        .flatMapError {
            guard case File.Error.noMatchingPredicate = $0 else {
                return self.eventLoop.makeFailedFuture($0)
            }
            
            return self.count().map { $0 + self.offset }
        }
        .map {
            assert($0 >= self.offset)
            return $0 - self.offset
        }
    }
    
    func cfHeadersTip(processed: Int) -> Future<Int> {
        self.firstIndex(processed: processed, where: { $0.isDisjoint(with: .hasFilterHeader) })
        .flatMapError {
            guard case File.Error.noMatchingPredicate = $0 else {
                return self.eventLoop.makeFailedFuture($0)
            }
            
            return self.count().map {
                assert($0 > 0)
                return $0 + self.offset
            }
        }
        .recover {
            preconditionFailure("\($0)")
        }
        .map {
            assert($0 > self.offset)
            return $0 - 1
        }
    }
}
