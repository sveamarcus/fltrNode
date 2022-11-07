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
import HaByLo
import FileRepo
import NodeTestLibrary
import NIO
import NIOTransportServices
import XCTest

let flagPosition = 80

final class FileBlockHeaderRepoTests: XCTestCase {
    var repo: FileBlockHeaderRepo! = nil
    var hashedHeaders: [BlockHeader]! = nil
    var serialHeaders: [BlockHeader]! = nil
    var fakeHeaders: [BlockHeader]! = nil
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
        
        hashedHeaders = BlockHeaderMock.hashedRealHeaders
        XCTAssertNoThrow(
            serialHeaders = try hashedHeaders.enumerated().map {
                var header = $0.element
                try header.addHeight($0.offset)
                try header.serial()
                return header
            }
        )
        XCTAssertNoThrow(
            self.fakeHeaders = try BlockHeaderMock.TestChain.createInvalid(length: 200)
                .map {
                    var copy = $0
                    try copy.serial()
                    return copy
            }
        )
        
        do {
            try FileManager.default
            .removeItem(atPath: Settings.BITCOIN_BLOCKHEADER_FILEPATH)
        } catch {}
        
        self.elg = .init(loopCount: 1)
        self.eventLoop = self.elg.next()
        self.repo = TestingHelpers.DB.getFileBlockHeaderRepo(offset: 0, threadPool: self.threadPool, eventLoop: self.eventLoop)
        
        let nonBlockingFileIODependency: (NIOThreadPool) -> NonBlockingFileIOClient = Settings.NONBLOCKING_FILE_IO
        self.nonBlockingFileIO = nonBlockingFileIODependency(self.threadPool)
    }
    
    override func tearDown() {
        do {
            try repo.close().wait()
        } catch {}
        XCTAssertNoThrow(try self.threadPool.syncShutdownGracefully())
        self.threadPool = nil
        self.repo = nil
        self.hashedHeaders = nil
        self.serialHeaders = nil
        self.fakeHeaders = nil
        self.nonBlockingFileIO = nil
        XCTAssertNoThrow(try self.elg.syncShutdownGracefully())
        self.elg = nil
        self.eventLoop = nil
        do {
            try FileManager.default
            .removeItem(atPath: Settings.BITCOIN_BLOCKHEADER_FILEPATH)
        } catch {}
        
        Settings = self.savedSettings
        self.savedSettings = nil
    }
    
    func testClosedError() {
        XCTAssertNoThrow(try repo.close().wait())
        XCTAssertThrowsError(
            try repo.write(serialHeaders[0]).wait()
        )
    }
    
    func testWriteSingleHeader() {
        XCTAssertNoThrow(try repo.write(serialHeaders[0]).wait())
    }
    
    func testWriteReadSingleHeader() {
        XCTAssertNoThrow(try repo.write(serialHeaders[0]).wait())
        var read: BlockHeader! = nil
        XCTAssertNoThrow(
            read = try repo.find(id: 0).wait()
        )
        XCTAssertEqual(read, serialHeaders[0])
    }

    func testWriteReadMultiHeader() {
        XCTAssertNoThrow(
            try serialHeaders.forEach {
                try repo.write($0).wait()
            }
        )
        
        XCTAssertNoThrow(
            try serialHeaders.enumerated().forEach {
                let result = try repo.find(id: $0.offset, event: #function).wait()
                logger.trace(#function, "Reading data back from file", result)
                XCTAssertEqual($0.element, result)
            }
        )
    }
    
    func testDeleteMulti() {
        XCTAssertNoThrow(
            try serialHeaders.forEach {
                try repo.write($0).wait()
            }
        )

        XCTAssertNoThrow(try repo.delete(from: 1).wait())
        
        XCTAssertNoThrow(
            XCTAssertEqual(try repo.count().wait(), 1)
        )
    }
    
    func testBinaryDateSearch() {
        var fakeHeaders: [BlockHeader]!
        XCTAssertNoThrow(
            fakeHeaders = try BlockHeaderMock.TestChain.createInvalid(length: 200)
            .map {
                var copy = $0
                try copy.serial()
                return copy
            }
        )
        guard let fake = fakeHeaders else {
            XCTFail()
            return
        }
        
        XCTAssertNoThrow(
            try fake.forEach {
                try repo.write($0).wait()
            }
        )
        
        let day: TimeInterval = 60 * 60 * 24
        XCTAssertNoThrow(
            try (0...(fake.count - 1)).forEach {
                XCTAssertEqual(
                    try repo.find(date: Date(timeIntervalSince1970: .init($0 + 1) * day)).wait(), fake[$0]
                )
            }
        )
        
        // Try to find a date that doesn't exist, one second less than the 25:th item
        XCTAssertThrowsError(try repo.find(date: Date(timeIntervalSince1970: 25 * day - 1)).wait()) { error in
            guard let e = error as? File.NoExactMatchFound<BlockHeader> else {
                XCTFail()
                return
            }
            XCTAssertEqual(Date(timeIntervalSince1970: TimeInterval(e.left.timestamp)),
                           Date(timeIntervalSince1970: 24 * day))
        }

        XCTAssertThrowsError(
            try repo.find(date: Date(timeIntervalSince1970: TimeInterval(fake.count + 1) * day)).wait()
        ) { error in
            guard let e = error as? File.NoExactMatchFound<BlockHeader> else {
                XCTFail()
                return
            }
            XCTAssertEqual(Date(timeIntervalSince1970: TimeInterval(e.left.timestamp)),
                           Date(timeIntervalSince1970: day * TimeInterval(fake.count)))
        }

        XCTAssertThrowsError(
            try repo.find(date: Date(timeIntervalSince1970: 0)).wait()
        ) { error in
            guard let e = error as? File.NoExactMatchFound<BlockHeader> else {
                XCTFail()
                return
            }
            XCTAssertEqual(Date(timeIntervalSince1970: TimeInterval(e.left.timestamp)),
                           Date(timeIntervalSince1970: day))
        }
    }
    
    func testLatestBlockLocatorWithSerialHeaders() {
        XCTAssertNoThrow(
            try self.serialHeaders.forEach {
                try self.repo.write($0).wait()
            }
        )
    
        XCTAssertNoThrow(
            XCTAssertEqual(
                try self.repo.latestBlockLocator().wait().hashes,
                [
                    try self.serialHeaders[3].blockHash(),
                    try self.serialHeaders[2].blockHash(),
                    try self.serialHeaders[0].blockHash()
                ]
           )
        )
    }
    
    func testRefreshBlockLocatorGuard() {
        let emptyHeaders: [BlockHeader] = []
        
        let locatorHashes = [3, 2, 0].compactMap {
            try? self.serialHeaders[$0].blockHash()
        }
        let testLocator = BlockLocator(hashes: locatorHashes, stop: .zero)
        
        XCTAssertEqual(
            BlockChain.refreshBlockLocator(new: emptyHeaders,
                                           previous: testLocator).hashes,
            testLocator.hashes
        )
        
        let singleHeader = Array(self.fakeHeaders[0...0])
        XCTAssertEqual(
            BlockChain.refreshBlockLocator(new: singleHeader, previous: testLocator).hashes,
            singleHeader.compactMap { try? $0.blockHash() } + testLocator.hashes
        )
        
        let fullLocatorHashes = [199, 198, 196, 192, 184, 168, 136, 72, 0].compactMap {
            try? self.fakeHeaders[$0].blockHash()
        }
        let fullLocator = BlockLocator(hashes: fullLocatorHashes, stop: .zero)
        let newHeaders = Array(self.fakeHeaders[0...1])
        
        XCTAssertEqual(
            BlockChain.refreshBlockLocator(new: newHeaders, previous: testLocator).hashes,
            newHeaders.compactMap { try? $0.blockHash() }.reversed() + testLocator.hashes.suffix(5)
        )
        
        // Final element should be filtered because it is the same as provided in newHeaders
        XCTAssertEqual(
            BlockChain.refreshBlockLocator(new: newHeaders, previous: fullLocator).hashes,
            newHeaders.compactMap { try? $0.blockHash() }.reversed() + fullLocator.hashes.suffix(5).dropLast()
        )
    }
    
    func testLatestBlockLocatorWithFakeHeaders() {
        var fakeHeaders: [BlockHeader]!
        XCTAssertNoThrow(
            fakeHeaders = try BlockHeaderMock.TestChain.createInvalid(length: 200)
            .map {
                var copy = $0
                try copy.serial()
                return copy
            }
        )
        guard let fake = fakeHeaders else {
            XCTFail()
            return
        }
        
        XCTAssertNoThrow(
            try fake.forEach {
                try repo.write($0).wait()
            }
        )
        
        let assertIndices: [Int] = [ 199, 198, 196, 192, 184, 168, 136, 72, 0 ]
        var assertHashes: [BlockChain.Hash<BlockHeaderHash>]!
        XCTAssertNoThrow(
            assertHashes = try assertIndices.map {
                try fake[$0].blockHash()
            }
        )
        
        guard let blockLocator = try? self.repo.latestBlockLocator().wait() else {
            XCTFail()
            return
        }
        
        XCTAssertEqual(blockLocator.hashes, assertHashes)

        // Refresh Block Locator test
        guard let newHeaders = try? BlockHeaderMock.TestChain.createInvalid(length: 200, offset: 200) else {
            XCTFail()
            return
        }

        let refreshed = BlockChain.refreshBlockLocator(new: newHeaders, previous: blockLocator)
        let refreshedHashes = assertIndices.compactMap {
            try? newHeaders[$0].blockHash()
        }
        
        XCTAssertEqual(refreshedHashes + blockLocator.hashes.suffix(5), refreshed.hashes)
    }

//    func testRestructureWithRemainingLimit() throws {
//        try withTemporaryFile { fileHandle, path in
//            try TestingHelpers.DB.withFileBlockHeaderRepo(fileHandle: fileHandle,
//                                                          nonBlockingFileIO: self.nonBlockingFileIO,
//                                                          eventLoop: self.eventLoop) { repo in
//                try self.fakeHeaders.forEach {
//                    try repo.write($0).wait()
//                }
//            }
//
//            try TestingHelpers.DB.writeFlags(
//                [.hasFilterHeader, .nonMatchingFilter, .processed],
//                fileHandle: fileHandle,
//                nonBlockingFileIO: self.nonBlockingFileIO,
//                repeating: 200,
//                recordSize: 145,
//                eventLoop: self.eventLoop
//            ).wait()
//
//            try TestingHelpers.DB.withFileBlockHeaderRepo(fileHandle: fileHandle,
//                                                          nonBlockingFileIO: self.nonBlockingFileIO,
//                                                          eventLoop: self.eventLoop) { bhRepo in
//                try TestingHelpers.DB.withFileCFHeaderRepo(fileHandle: fileHandle,
//                                                           nonBlockingFileIO: self.nonBlockingFileIO,
//                                                           eventLoop: self.eventLoop) { cfRepo in
//                    guard let new = try Node
//                        .restructure(initialOffset: 0,
//                                     openFile: fileHandle,
//                                     bhRepo: bhRepo,
//                                     cfHeaderRepo: cfRepo,
//                                     eventLoop: self.eventLoop,
//                                     threadPool: self.threadPool,
//                                     nonBlockingFileIO: self.nonBlockingFileIO,
//                                     minNumberHeaders: 50,
//                                     restructureFactor: 2,
//                                     blockHeaderFilePath: path,
//                                     newFilePath: path + ".new")
//                        .wait()
//                    else {
//                        XCTFail()
//                        return
//                    }
//
//                    defer {
//                        XCTAssertNoThrow(
//                            try File.close(on: eventLoop,
//                                           { new.bhRepo.close() },
//                                           { new.cfHeaderRepo.close() }).wait()
//                        )
//                        XCTAssertNoThrow(try new.openFile.close())
//                    }
//
//                    XCTAssertNoThrow(
//                        try (150..<200).forEach {
//                            XCTAssertEqual(try new.bhRepo.find(id: $0).wait().blockHash(), try self.fakeHeaders?[$0].blockHash())
//                        }
//                    )
//
//                    XCTAssertNoThrow(
//                        XCTAssertEqual(try new.bhRepo.count().wait(), 50)
//                    )
//
//                    XCTAssertThrowsError(
//                        try new.bhRepo.find(id: 200).wait()
//                    )
//                }
//            }
//        }
//    }
//
//    func testRestructureWithFilterFlagsLimit() throws {
//        try withTemporaryFile { fileHandle, path in
//            try TestingHelpers.DB.withFileBlockHeaderRepo(fileHandle: fileHandle,
//                                                          nonBlockingFileIO: self.nonBlockingFileIO,
//                                                          eventLoop: self.eventLoop) { repo in
//                try self.fakeHeaders.forEach {
//                    try repo.write($0).wait()
//                }
//            }
//
//            try TestingHelpers.DB.writeFlags(
//                [.hasFilterHeader, .nonMatchingFilter, .processed],
//                fileHandle: fileHandle,
//                nonBlockingFileIO: self.nonBlockingFileIO,
//                repeating: 101,
//                recordSize: 145,
//                eventLoop: self.eventLoop
//            ).wait()
//
//            try TestingHelpers.DB.withFileBlockHeaderRepo(fileHandle: fileHandle,
//                                                          nonBlockingFileIO: self.nonBlockingFileIO,
//                                                          eventLoop: self.eventLoop) { bhRepo in
//                try TestingHelpers.DB.withFileCFHeaderRepo(fileHandle: fileHandle,
//                                                           nonBlockingFileIO: self.nonBlockingFileIO,
//                                                           eventLoop: self.eventLoop) { cfRepo in
//                    guard let new = try Node
//                        .restructure(initialOffset: 0,
//                                     openFile: fileHandle,
//                                     bhRepo: bhRepo,
//                                     cfHeaderRepo: cfRepo,
//                                     eventLoop: self.eventLoop,
//                                     threadPool: self.threadPool,
//                                     nonBlockingFileIO: self.nonBlockingFileIO,
//                                     minNumberHeaders: 50,
//                                     restructureFactor: 2,
//                                     blockHeaderFilePath: path,
//                                     newFilePath: path + ".new")
//                        .wait()
//                    else {
//                        XCTFail()
//                        return
//                    }
//
//                    defer {
//                        XCTAssertNoThrow(
//                            try File.close(on: eventLoop,
//                                           { new.bhRepo.close() },
//                                           { new.cfHeaderRepo.close() }).wait()
//                        )
//                        XCTAssertNoThrow(try new.openFile.close())
//                    }
//
//                    XCTAssertNoThrow(
//                        try (51..<200).forEach {
//                            XCTAssertEqual(try new.bhRepo.find(id: $0).wait().blockHash(), try self.fakeHeaders?[$0].blockHash())
//                        }
//                    )
//
//                    XCTAssertNoThrow(
//                        XCTAssertEqual(try new.bhRepo.count().wait(), 149)
//                    )
//
//                    XCTAssertThrowsError(
//                        try new.bhRepo.find(id: 200).wait()
//                    )
//                }
//            }
//        }
//    }
    
//    func testRestructureSkip() throws {
//        try withTemporaryFile { fileHandle, path in
//            try TestingHelpers.DB.withFileBlockHeaderRepo(fileHandle: fileHandle,
//                                                          nonBlockingFileIO: self.nonBlockingFileIO,
//                                                          eventLoop: self.eventLoop) { repo in
//                try self.fakeHeaders.forEach {
//                    try repo.write($0).wait()
//                }
//            }
//            
//            try TestingHelpers.DB.writeFlags(
//                [.hasFilterHeader, .nonMatchingFilter, .processed],
//                fileHandle: fileHandle,
//                nonBlockingFileIO: self.nonBlockingFileIO,
//                repeating: 100,
//                recordSize: 145,
//                eventLoop: self.eventLoop
//            ).wait()
//            
//            try TestingHelpers.DB.withFileBlockHeaderRepo(fileHandle: fileHandle,
//                                                          nonBlockingFileIO: self.nonBlockingFileIO,
//                                                          eventLoop: self.eventLoop) { bhRepo in
//                try TestingHelpers.DB.withFileCFHeaderRepo(fileHandle: fileHandle,
//                                                           nonBlockingFileIO: self.nonBlockingFileIO,
//                                                           eventLoop: self.eventLoop) { cfRepo in
//                    XCTAssertNil(try Node
//                                    .restructure(initialOffset: 0,
//                                                 openFile: fileHandle,
//                                                 bhRepo: bhRepo,
//                                                 cfHeaderRepo: cfRepo,
//                                                 eventLoop: self.eventLoop,
//                                                 threadPool: self.threadPool,
//                                                 nonBlockingFileIO: self.nonBlockingFileIO,
//                                                 minNumberHeaders: 100,
//                                                 restructureFactor: 1,
//                                                 blockHeaderFilePath: path,
//                                                 newFilePath: path + ".new")
//                                    .wait())
//                    
//                    try TestingHelpers.DB.writeFlags(
//                        [.hasFilterHeader, .nonMatchingFilter, .processed],
//                        fileHandle: fileHandle,
//                        nonBlockingFileIO: self.nonBlockingFileIO,
//                        start: 100,
//                        repeating: 100,
//                        recordSize: 145,
//                        eventLoop: self.eventLoop
//                    ).wait()
//
//                    XCTAssertNil(try Node
//                                    .restructure(initialOffset: 0,
//                                                 openFile: fileHandle,
//                                                 bhRepo: bhRepo,
//                                                 cfHeaderRepo: cfRepo,
//                                                 eventLoop: self.eventLoop,
//                                                 threadPool: self.threadPool,
//                                                 nonBlockingFileIO: self.nonBlockingFileIO,
//                                                 minNumberHeaders: 200,
//                                                 restructureFactor: 1,
//                                                 blockHeaderFilePath: path,
//                                                 newFilePath: path + ".new")
//                                    .wait())
//                }
//            }
//        }
//    }
    
    func testOffsetWriteRead() {
        XCTAssertNoThrow(
            try withTemporaryFile { fileHandle, path in
                try TestingHelpers.DB.withFileBlockHeaderRepo(fileHandle: fileHandle,
                                                              nonBlockingFileIO: self.nonBlockingFileIO,
                                                              eventLoop: self.eventLoop,
                                                              offset: 0) { repo in
                    try self.fakeHeaders.forEach {
                        try repo.write($0).wait()
                    }
                }

                try TestingHelpers.DB.withFileBlockHeaderRepo(fileHandle: fileHandle,
                                                              nonBlockingFileIO: self.nonBlockingFileIO,
                                                              eventLoop: self.eventLoop,
                                                              offset: 1000) { repo in
                    XCTAssertThrowsError(
                        try repo.find(id: 999).wait()
                    )

                    XCTAssertThrowsError(
                        try repo.find(id: 1200).wait()
                    )

                    try self.fakeHeaders.enumerated().forEach {
                        XCTAssertEqual(try $0.element.blockHash(), try repo.find(id: 1000 + $0.offset).wait().blockHash())
                    }
                }
            }
        )
    }
    
    func testOffsetDelete() {
        func assertFileError(_ error: Error) {
            guard let fileError = error as? File.Error, case .noDataFoundFileEmpty(let str) = fileError else {
                XCTFail()
                return
            }
            
            XCTAssertEqual(str, "BlockHeader")
        }

        XCTAssertNoThrow(
            try withTemporaryFile { fileHandle, path in
                try TestingHelpers.DB.withFileBlockHeaderRepo(fileHandle: fileHandle,
                                                              nonBlockingFileIO: self.nonBlockingFileIO,
                                                              eventLoop: self.eventLoop,
                                                              offset: 0) { repo in
                    
                    XCTAssertThrowsError(try repo.heights().wait(), "", assertFileError(_:))

                    try self.fakeHeaders.forEach {
                        try repo.write($0).wait()
                    }
                }
            
                try TestingHelpers.DB.withFileBlockHeaderRepo(fileHandle: fileHandle,
                                                              nonBlockingFileIO: self.nonBlockingFileIO,
                                                              eventLoop: self.eventLoop,
                                                              offset: 1000) { repo in
                    XCTAssertThrowsError(try repo.delete(from: 999).wait())
                    XCTAssertThrowsError(try repo.delete(from: 1200).wait())
                    
                    try self.fakeHeaders.reversed().enumerated().forEach {
                        let id = 1199 - $0.offset
                        XCTAssertEqual(try repo.find(id: id).wait().blockHash(), try $0.element.blockHash())
                        XCTAssertNoThrow(try repo.delete(from: id).wait())
                        XCTAssertThrowsError(try repo.find(id: id).wait())
                    }
                    
                    XCTAssertThrowsError(try repo.heights().wait(), "", assertFileError(_:))
                }
            }
        )
    }
    
    func testOffsetAppend() {
        XCTAssertNoThrow(
            try withTemporaryFile { fileHandle, path in
                try TestingHelpers.DB.withFileBlockHeaderRepo(fileHandle: fileHandle,
                                                              nonBlockingFileIO: self.nonBlockingFileIO,
                                                              eventLoop: self.eventLoop,
                                                              offset: 0) { repo in
                    try self.fakeHeaders.prefix(100).forEach {
                        try repo.write($0).wait()
                    }
                }
                
                try TestingHelpers.DB.withFileBlockHeaderRepo(fileHandle: fileHandle,
                                                              nonBlockingFileIO: self.nonBlockingFileIO,
                                                              eventLoop: self.eventLoop,
                                                              offset: 20) { repo in
                    XCTAssertThrowsError(
                        try repo.find(id: 120).wait()
                    )
                    
                    XCTAssertThrowsError(
                        try repo.append(self.fakeHeaders.dropFirst(121)).wait()
                    )
                    
                    XCTAssertThrowsError(
                        try repo.append(self.fakeHeaders.dropFirst(119)).wait()
                    )
                    
                    let appendHeaders = self.fakeHeaders.dropFirst(120)
                    XCTAssertEqual(appendHeaders.first?.id, 120)
                    try repo.append(appendHeaders).wait()
                    
                    XCTAssertEqual(try repo.find(id: 199).wait().id, 199)
                    
                    XCTAssertThrowsError(
                        try repo.find(id: 200).wait()
                    )
                }
            }
        )
    }
    
    func testFindFromThrough() {
        XCTAssertNoThrow(
            try withTemporaryFile { fileHandle, path in
                try TestingHelpers.DB.withFileBlockHeaderRepo(fileHandle: fileHandle,
                                                              nonBlockingFileIO: self.nonBlockingFileIO,
                                                              eventLoop: self.eventLoop,
                                                              offset: 0) { repo in
                    try self.fakeHeaders.forEach {
                        try repo.write($0).wait()
                    }
                }

                try TestingHelpers.DB.withFileBlockHeaderRepo(fileHandle: fileHandle,
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

                    try self.fakeHeaders.enumerated().forEach {
                        XCTAssertEqual(try $0.element.blockHash(), try many2[$0.offset].blockHash())
                    }
                }
            }
        )
    }
}
