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
@testable import fltrNode
import FileRepo
import NodeTestLibrary
import NIO
import NIOTransportServices
import XCTest

final class FileCFHeaderRepoTests: XCTestCase {
    var wireFilterHeaders: [CF.Header]! = nil
    var serialFilterHeaders: [CF.SerialHeader]! = nil
    var threadPool: NIOThreadPool! = nil
    var nonBlockingFileIO: NonBlockingFileIOClient! = nil
    var elg: NIOTSEventLoopGroup!
    var eventLoop: EventLoop! = nil
    var savedSettings: NodeSettings!
    
    override func setUp() {
        self.savedSettings = Settings
        TestingHelpers.diSetup()
        
        self.threadPool = NIOThreadPool.init(numberOfThreads: NonBlockingFileIO.defaultThreadPoolSize)
        self.threadPool.start()

        self.wireFilterHeaders = TestingHelpers.DB.makeWireFilterHeaders(count: 200)
        self.serialFilterHeaders = self.wireFilterHeaders.enumerated().map(TestingHelpers.DB.wireToSerial(_:))
        do {
            try FileManager.default
            .removeItem(atPath: Settings.BITCOIN_BLOCKHEADER_FILEPATH)
        } catch {}
        
        self.elg = .init(loopCount: 1)
        self.eventLoop = self.elg.next()
        
        let nonBlockingFileIODependency: (NIOThreadPool) -> NonBlockingFileIOClient = Settings.NONBLOCKING_FILE_IO
        self.nonBlockingFileIO = nonBlockingFileIODependency(self.threadPool)
    }
    
    override func tearDown() {
        do {
            try FileManager.default
            .removeItem(atPath: Settings.BITCOIN_BLOCKHEADER_FILEPATH)
        } catch {}
        
        XCTAssertNoThrow(try self.threadPool?.syncShutdownGracefully())
        self.threadPool = nil
        self.nonBlockingFileIO = nil
        XCTAssertNoThrow(try self.elg.syncShutdownGracefully())
        self.elg = nil
        self.eventLoop = nil

        Settings = self.savedSettings
        self.savedSettings = nil
    }
    
    func testWriteSingleHeader() {
        XCTAssertNoThrow(
            try withTemporaryFile { fileHandle, path in
                try TestingHelpers.DB.withFileCFHeaderRepo(fileHandle: fileHandle,
                                                           nonBlockingFileIO: self.nonBlockingFileIO,
                                                           eventLoop: self.eventLoop) { repo in
                    try repo.write(self.serialFilterHeaders[0]).wait()
                }
            }
        )
    }
    
    func testWriteReadSingleHeader() {
        XCTAssertNoThrow(
            try withTemporaryFile { fileHandle, path in
                try TestingHelpers.DB.withFileCFHeaderRepo(fileHandle: fileHandle,
                                                           nonBlockingFileIO: self.nonBlockingFileIO,
                                                           eventLoop: self.eventLoop) { repo in
                    try repo.write(self.serialFilterHeaders[0]).wait()
                    try repo.write(self.serialFilterHeaders[1]).wait()

                    XCTAssertEqual(try repo.find(id: 1).wait(), self.serialFilterHeaders[1])
                    XCTAssertEqual(try repo.findWire(id: 1).wait(), self.wireFilterHeaders[1])

                }
            }
        )
    }

    func testWriteReadMultiHeader() {
        XCTAssertNoThrow(
            try withTemporaryFile { fileHandle, path in
                try TestingHelpers.DB.withFileCFHeaderRepo(fileHandle: fileHandle,
                                                           nonBlockingFileIO: self.nonBlockingFileIO,
                                                           eventLoop: self.eventLoop) { repo in
                    try self.serialFilterHeaders.forEach {
                        try repo.write($0).wait()
                    }

                    try self.serialFilterHeaders.enumerated().forEach {
                        let resultSerial = try repo.find(id: $0.offset, event: #function).wait()
                        XCTAssertEqual($0.element, resultSerial)
                    }
                    
                    try self.wireFilterHeaders.enumerated().dropFirst().forEach {
                        let resultWire = try repo.findWire(id: $0.offset, event: #function).wait()
                        XCTAssertEqual($0.element, resultWire)
                    }
                }
            }
        )
    }

    func testDeleteMulti() {
        XCTAssertNoThrow(
            try withTemporaryFile { fileHandle, path in
                try TestingHelpers.DB.withFileCFHeaderRepo(fileHandle: fileHandle,
                                                           nonBlockingFileIO: self.nonBlockingFileIO,
                                                           eventLoop: self.eventLoop) { repo in
                    try self.serialFilterHeaders.forEach {
                        try repo.write($0).wait()
                    }
                    
                    try repo.delete(from: 1).wait()
                    XCTAssertEqual(try repo.count().wait(), 1)
                    XCTAssertEqual(try repo.find(id: 0).wait(), self.serialFilterHeaders[0])
                    XCTAssertThrowsError(try repo.find(id: 1).wait())
                }
            }
        )
    }

    func testSeekBeyondEndThrows() {
        XCTAssertNoThrow(
            try withTemporaryFile { fileHandle, path in
                try TestingHelpers.DB.withFileCFHeaderRepo(fileHandle: fileHandle,
                                                           nonBlockingFileIO: self.nonBlockingFileIO,
                                                           eventLoop: self.eventLoop) { repo in
                    try self.serialFilterHeaders.forEach {
                        try repo.write($0).wait()
                    }
                    
                    XCTAssertThrowsError(try repo.find(id: self.serialFilterHeaders.count).wait()) { error in
                        guard let e = error as? File.Error, case .seekError = e else {
                            XCTFail()
                            return
                        }
                    }
                }
            }
        )
    }

    func testFindUninitializedThrows() {
        XCTAssertNoThrow(
            try withTemporaryFile { fileHandle, path in
                try TestingHelpers.DB.withFileCFHeaderRepo(fileHandle: fileHandle,
                                                           nonBlockingFileIO: self.nonBlockingFileIO,
                                                           eventLoop: self.eventLoop) { repo in
                    try repo.nonBlockingFileIO.changeFileSize(fileHandle: fileHandle,
                                                              size: Int64(145 * 10),
                                                              eventLoop: repo.eventLoop).wait()
                    XCTAssertThrowsError(
                        try repo.find(id: 1).wait()
                    ) { error in
                        guard case .compactHeaderMissing = error as? File.Error else {
                            XCTFail()
                            return
                        }
                    }
                }
            }
        )
    }
    
    func testFindFromThrough() {
        XCTAssertNoThrow(
            try withTemporaryFile { fileHandle, path in
                try TestingHelpers.DB.withFileCFHeaderRepo(fileHandle: fileHandle,
                                                           nonBlockingFileIO: self.nonBlockingFileIO,
                                                           eventLoop: self.eventLoop,
                                                           offset: 0) { repo in
                    try self.serialFilterHeaders.forEach {
                        try repo.write($0).wait()
                    }
                }

                try TestingHelpers.DB.withFileCFHeaderRepo(fileHandle: fileHandle,
                                                           nonBlockingFileIO: self.nonBlockingFileIO,
                                                           eventLoop: self.eventLoop,
                                                           offset: 1000) { repo in
                    let many1 = try repo.find(from: 1000, through: 1198).wait()
                    let many2 = try repo.find(from: 1000).wait()

                    XCTAssertEqual(many1, many2.dropLast())
                    
                    XCTAssertThrowsError(
                        try repo.find(from: 999).wait()
                    )

                    XCTAssertThrowsError(
                        try repo.find(from: 1200).wait()
                    )

                    XCTAssertThrowsError(
                        try repo.find(from: 1001, through: 1000).wait()
                    )

                    XCTAssertNoThrow(
                        try repo.find(from: 1000, through: 1000).wait()
                    )

                    XCTAssertNoThrow(
                        try repo.find(from: 1199, through: 1199).wait()
                    )

                    self.serialFilterHeaders.enumerated().forEach {
                        XCTAssertEqual($0.element.headerHash, many2[$0.offset].headerHash)
                    }
                }
            }
        )
    }
    
    func testRecordFlagPredicateMatch() {
        XCTAssertNoThrow(
            try withTemporaryFile { fileHandle, path in
                try TestingHelpers.DB.withFileCFHeaderRepo(fileHandle: fileHandle,
                                                           nonBlockingFileIO: self.nonBlockingFileIO,
                                                           eventLoop: self.eventLoop) { repo in
                    try self.serialFilterHeaders.forEach {
                        try repo.write($0).wait()
                    }
                }
                
                try TestingHelpers.DB.writeFlags(
                    [.hasFilterHeader, .nonMatchingFilter],
                    fileHandle: fileHandle,
                    nonBlockingFileIO: nonBlockingFileIO,
                    start: 0,
                    repeating: 10,
                    recordSize: 145,
                    eventLoop: self.eventLoop
                ).wait()
                
                try TestingHelpers.DB.writeFlags(
                    [.hasFilterHeader, .nonMatchingFilter],
                    fileHandle: fileHandle,
                    nonBlockingFileIO: nonBlockingFileIO,
                    start: 20,
                    repeating: 10,
                    recordSize: 145,
                    eventLoop: self.eventLoop
                ).wait()
                
                try TestingHelpers.DB.writeFlags(
                    [.hasFilterHeader, .processed],
                    fileHandle: fileHandle,
                    nonBlockingFileIO: nonBlockingFileIO,
                    start: 40,
                    repeating: 10,
                    recordSize: 145,
                    eventLoop: self.eventLoop
                ).wait()
                
                try TestingHelpers.DB.writeFlags(
                    [.hasFilterHeader, .nonMatchingFilter, .processed],
                    fileHandle: fileHandle,
                    nonBlockingFileIO: nonBlockingFileIO,
                    start: 100,
                    repeating: 1,
                    recordSize: 145,
                    eventLoop: self.eventLoop
                ).wait()
                
                
                try TestingHelpers.DB.withFileCFHeaderRepo(
                    fileHandle: fileHandle,
                    nonBlockingFileIO: self.nonBlockingFileIO,
                    eventLoop: self.eventLoop) { repo in
                    let find0 = repo.first(processed: 0) {
                        $0.contains(
                            [.hasFilterHeader, .nonMatchingFilter]
                        )
                    }
                    XCTAssertEqual(try find0.wait().id, 0)
                    
                    let find10 = repo.first(processed: 0) {
                        !$0.contains(.nonMatchingFilter)
                    }
                    XCTAssertEqual(try find10.wait().id, 10)
                    
                    let find40 = repo.first(processed: 0) {
                        $0.contains([.hasFilterHeader, .processed])
                    }
                    XCTAssertEqual(try find40.wait().id, 40)
                    
                    let find100 = repo.firstIndex(processed: 0) {
                        $0 == [ .hasFilterHeader, .nonMatchingFilter, .processed ]
                    }
                    XCTAssertEqual(try find100.wait(), 100)
                    
                    XCTAssertEqual(try repo.records(for: { $0.contains(.hasFilterHeader) }).wait().count, 200)
                    XCTAssertEqual(try repo.records(limit: 10, for: { $0.contains(.hasFilterHeader) }).wait().count, 10)
                    
                    let processedRecords = try repo.records(for: { $0.contains(.processed) }).wait()
                    let unprocessedRecords = try repo.records(limit: 150, for: { !$0.contains(.processed) }).wait()
                    XCTAssertEqual(processedRecords.count, 11)
                    XCTAssertEqual(unprocessedRecords.count, 150)
                    XCTAssert(processedRecords.contains(100))
                    XCTAssert(unprocessedRecords.contains(101))
                    XCTAssert(processedRecords.isDisjoint(with: unprocessedRecords))
                    
                    XCTAssertThrowsError(try repo.firstIndex(processed: 0,
                                                             where: { _ in false }).wait())
                }

                // with offset and start/end limited
                try TestingHelpers.DB.withFileCFHeaderRepo(
                    fileHandle: fileHandle,
                    nonBlockingFileIO: self.nonBlockingFileIO,
                    eventLoop: self.eventLoop,
                    offset: 100) { repo in
                    XCTAssertEqual(
                        try repo.records(from: 100, to: 109, for: {
                            $0.contains(.nonMatchingFilter)
                        })
                        .wait(),
                        [100, 101, 102, 103, 104, 105, 106, 107, 108]
                    )
                    XCTAssertEqual(
                        try repo.records(from: 109, to: 121, for: {
                            $0.contains(.nonMatchingFilter)
                        })
                        .wait(),
                        [109, 120]
                    )
                    XCTAssertEqual(
                        try repo.records(from: 140, to: 150, limit: 2, for: {
                            $0.contains(.processed)
                        })
                        .wait(),
                        [140, 141]
                    )
                    
                    XCTAssertThrowsError(try repo.firstIndex(processed:0,
                                                             where: { _ in false }).wait())
                }
                
            }
        )
    }
    
    func testProcessedRecords() {
        XCTAssertNoThrow(
            try withTemporaryFile { fileHandle, path in
                try TestingHelpers.DB.withFileCFHeaderRepo(fileHandle: fileHandle,
                                                           nonBlockingFileIO: self.nonBlockingFileIO,
                                                           eventLoop: self.eventLoop) { repo in
                    try self.serialFilterHeaders.forEach {
                        try repo.write($0).wait()
                    }
                }
                
                try TestingHelpers.DB.writeFlags(
                    [.hasFilterHeader ],
                    fileHandle: fileHandle,
                    nonBlockingFileIO: nonBlockingFileIO,
                    start: 0,
                    repeating: 1,
                    recordSize: 145,
                    eventLoop: self.eventLoop
                ).wait()
                
                try TestingHelpers.DB.writeFlags(
                    [.hasFilterHeader, .processed],
                    fileHandle: fileHandle,
                    nonBlockingFileIO: nonBlockingFileIO,
                    start: 1,
                    repeating: 199,
                    recordSize: 145,
                    eventLoop: self.eventLoop
                ).wait()
                
                try TestingHelpers.DB.withFileCFHeaderRepo(
                    fileHandle: fileHandle,
                    nonBlockingFileIO: self.nonBlockingFileIO,
                    eventLoop: self.eventLoop
                ) { repo in
                    XCTAssertEqual(try repo.processedRecords(processed: 0).wait(), 0)
                }
                
                try TestingHelpers.DB.writeFlags(
                    [.hasFilterHeader, .processed],
                    fileHandle: fileHandle,
                    nonBlockingFileIO: nonBlockingFileIO,
                    start: 0,
                    repeating: 1,
                    recordSize: 145,
                    eventLoop: self.eventLoop
                ).wait()

                try TestingHelpers.DB.withFileCFHeaderRepo(
                    fileHandle: fileHandle,
                    nonBlockingFileIO: self.nonBlockingFileIO,
                    eventLoop: self.eventLoop
                ) { repo in
                    XCTAssertEqual(try repo.processedRecords(processed: 0).wait(), 200)
                }
                
                try TestingHelpers.DB.writeFlags(
                    .hasFilterHeader,
                    fileHandle: fileHandle,
                    nonBlockingFileIO: nonBlockingFileIO,
                    start: 100,
                    repeating: 1,
                    recordSize: 145,
                    eventLoop: self.eventLoop
                ).wait()

                try TestingHelpers.DB.withFileCFHeaderRepo(
                    fileHandle: fileHandle,
                    nonBlockingFileIO: self.nonBlockingFileIO,
                    eventLoop: self.eventLoop
                ) { repo in
                    XCTAssertEqual(try repo.processedRecords(processed: 0).wait(), 100)
                }

                try TestingHelpers.DB.writeFlags(
                    .hasFilterHeader,
                    fileHandle: fileHandle,
                    nonBlockingFileIO: nonBlockingFileIO,
                    start: 1,
                    repeating: 1,
                    recordSize: 145,
                    eventLoop: self.eventLoop
                ).wait()

                try TestingHelpers.DB.withFileCFHeaderRepo(
                    fileHandle: fileHandle,
                    nonBlockingFileIO: self.nonBlockingFileIO,
                    eventLoop: self.eventLoop
                ) { repo in
                    XCTAssertEqual(try repo.processedRecords(processed: 0).wait(), 1)
                }
            }
        )
    }
}

