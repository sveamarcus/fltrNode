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
import NIO

final class FileCFHeaderRepo: CompactFilterFileRepo {
    typealias Model = CF.SerialHeader
    let allocator: ByteBufferAllocator = .init()
    let recordSize = 145
    let flagIndex = 80
    let fieldSelector: ClosedRange<Int>? = (80...144)

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
    
    func fileDecode(id: Int, buffer: inout ByteBuffer) throws -> CF.SerialHeader {
        var copy = buffer
        guard let recordFlag = copy.readInteger().map(File.RecordFlag.init(rawValue:)),
            recordFlag.contains(.hasFilterHeader) else {
            throw File.Error.compactHeaderMissing(id: id)
        }
        
        guard let filterHashLittleEndian = copy.readBytes(length: 32) else {
            throw File.Error.readError(message: "Could not read filter hash", event: #function)
        }
        guard let headerHashLittleEndian = copy.readBytes(length: 32) else {
            throw File.Error.readError(message: "Could not read header hash", event: #function)
        }
        
        return CF.SerialHeader(recordFlag: recordFlag,
                               id: id,
                               filterHash: .little(filterHashLittleEndian),
                               headerHash: .little(headerHashLittleEndian))
        
    }
    
    func fileEncode(_ row: CF.SerialHeader, buffer: inout ByteBuffer) throws {
        buffer.writeInteger(row.recordFlag.rawValue)
        buffer.writeBytes(row.filterHash.littleEndian)
        buffer.writeBytes(row.headerHash.littleEndian)
    }
    
    func findWire(id: Int, event: StaticString = #function) -> Future<CF.Header> {
        self.find(id: id - 1, event: event).and(
            self.find(id: id, event: event)
        )
        .flatMap { previous, here in
            guard let wire = CF.Header(here, previousHeader: previous) else {
                return self.eventLoop.makeFailedFuture(File.Error.readError(
                    message: "Cannot make (wire) CF.Header, probable file corruption",
                    event: #function)
                )
            }
            return self.eventLoop.makeSucceededFuture(wire)
        }
    }
    
    func addFlags(id: Int, flags: File.RecordFlag) -> Future<Void> {
        self.find(id: id)
        .map {
            CF.SerialHeader(recordFlag: $0.recordFlag.union(flags),
                            id: $0.id,
                            filterHash: $0.filterHash,
                            headerHash: $0.headerHash)
        }
        .flatMap {
            self.write($0)
        }
    }
}
