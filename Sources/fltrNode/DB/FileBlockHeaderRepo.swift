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
import FileRepo
import NIO
import class NIOTransportServices.NIOTSEventLoopGroup
import struct Foundation.Date

extension BlockHeader: Identifiable {
    public var id: Int {
        if let id = try? self.height() {
            return id
        } else {
            return -1
        }
    }
}

final class FileBlockHeaderRepo: HeaderRepoProtocol {
    typealias Model = BlockHeader
    let fieldSelector: ClosedRange<Int>? = (0...79)
    
    let recordSize = 145
    let allocator: ByteBufferAllocator = .init()
    let eventLoop: EventLoop
    let nonBlockingFileIO: NonBlockingFileIOClient
    let nioFileHandle: NIOFileHandle
    let offset: Int

    init(eventLoop: EventLoop,
         nonBlockingFileIO: NonBlockingFileIOClient,
         nioFileHandle: NIOFileHandle,
         offset: Int) {
        self.eventLoop = eventLoop
        self.nonBlockingFileIO = nonBlockingFileIO
        self.nioFileHandle = nioFileHandle
        self.offset = offset
    }

    func fileDecode(id: Int, buffer: inout ByteBuffer) throws -> BlockHeader {
        var copy = buffer
        guard let version = copy.readInteger(endianness: .host, as: Int32.self) else {
            throw File.Error.readError(message: "Could not read 32 bit version from ByteBuffer", event: #function)
        }
        guard let merkleRoot = copy.readBytes(length: 32) else {
            throw File.Error.readError(message: "Could not read 32 bytes merkleRoot from ByteBuffer", event: #function)
        }
        guard let timestamp = copy.readInteger(endianness: .host, as: UInt32.self) else {
            throw File.Error.readError(message: "Could not read 32 bit timestamp from ByteBuffer", event: #function)
        }
        guard let difficulty = copy.readInteger(endianness: .host, as: UInt32.self) else {
            throw File.Error.readError(message: "Could not read 32 bit difficulty from ByteBuffer", event: #function)
        }
        guard let nonce = copy.readInteger(endianness: .host, as: UInt32.self) else {
                   throw File.Error.readError(message: "Could not read 32 bit nonce from ByteBuffer", event: #function)
               }
        guard let hash = copy.readBytes(length: 32) else {
            throw File.Error.readError(message: "Could not read 32 bytes hash from ByteBuffer", event: #function)
        }
        return BlockHeader(
            .serial(version: version,
                    merkleRoot: .little(merkleRoot),
                    timestamp: timestamp,
                    difficulty: difficulty,
                    nonce: nonce,
                    id: id,
                    hash: .little(hash))
        )
    }

    func fileEncode(_ row: BlockHeader, buffer: inout ByteBuffer) throws {
        let serial = try row.serialView()
        buffer.writeInteger(serial.version, endianness: .host, as: Int32.self)
        buffer.writeBytes(serial.merkleRoot.littleEndian)
        buffer.writeInteger(serial.timestamp, endianness: .host, as: UInt32.self)
        buffer.writeInteger(serial.difficulty, endianness: .host, as: UInt32.self)
        buffer.writeInteger(serial.nonce, endianness: .host, as: UInt32.self)
        buffer.writeBytes(serial.hash.littleEndian)
    }

    // MARK: Non-recursive for reference
//    func find(date: Date) -> Future<BlockHeader> {
//        let promise = self.eventLoop.makePromise(of: Model.self)
//        let rows = self.count()
//        rows.cascadeFailure(to: promise)
//        rows.whenSuccess { rows in
//            self.binarySearch(comparable: UInt32(date.timeIntervalSince1970),
//                              left: 0,
//                              right: rows - 1,
//                              promise: promise) {
//                try! $0.serialView().timestamp
//            }
//        }
//        return promise.futureResult
//    }
    
    func find(date: Date) -> Future<BlockHeader> {
        self.find(since1970: .init(date.timeIntervalSince1970))
    }
    
    func find(since1970 date: UInt32) -> Future<BlockHeader> {
        self.count().flatMap {
            self.binarySearch(comparable: date,
                              left: 0,
                              right: $0 - 1) {
                                $0.timestamp
            }
        }
    }
}

extension FileBlockHeaderRepo {
    func latestBlockLocator() -> Future<BlockLocator> {
        self.heights().flatMap {
            let locatorHeights = BlockChain.strideGenerator(lowerHeight: $0.lowerHeight, upperHeight: $0.upperHeight)
            
            let futures: [Future<BlockHeader>] = locatorHeights.map {
                self.find(id: $0)
            }
            
            return Future.whenAllSucceed(futures, on: self.eventLoop)
            .flatMapThrowing {
                try $0.map {
                    try $0.blockHash()
                }
            }
            .map {
                BlockLocator(hashes: $0, stop: .zero)
            }
        }
    }
}

