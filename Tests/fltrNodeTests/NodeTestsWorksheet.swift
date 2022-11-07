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
import XCTest

extension NodeTests {
    // MARK: DownloadSheet / RepoData Load
    func testLoadWorksheetFullyProcessed() {
        let (bhTest, _, _) = Self.dbWriteTest20(eventLoop: self.elg.next())
        
        let bhRepo = self.makeBhRepo(offset: 1000)
        let cfHeaderRepo = self.makeCfRepo(offset: 1000)
        defer {
            self.repoClose(bhRepo)
            self.repoClose(cfHeaderRepo)
        }

        XCTAssertNoThrow(try bhRepo.delete(from: 1015).wait())
        
        for id in (1000..<1015) {
            XCTAssertNoThrow(try cfHeaderRepo.addFlags(id: id, flags: [.hasFilterHeader, .nonMatchingFilter, .processed]).wait())
        }
        
        let networkState = TestingHelpers.Node.createNetworkState(offset: 1000,
                                                                  muxer: self.muxer,
                                                                  cfMatcher: self.cfMatcher,
                                                                  eventLoop: self.embedded,
                                                                  threadPool: self.threadPool,
                                                                  nioFileHandle: self.fileHandle)
        
        var repoData: Node.Worksheet.Properties? = nil
        XCTAssertNoThrow(
            repoData = try networkState.loadWorksheetData(skipLocator: false,
                                                          processed: 0,
                                                          eventLoop: self.embedded)
            .wait()
        )
        XCTAssertEqual(repoData?.bhHeight, 1014)
        XCTAssertEqual(repoData?.locator?.hashes.first, try? bhTest[..<15].last?.blockHash() )
        XCTAssertEqual(repoData?.locator?.hashes.last, try? bhTest[..<15].first?.blockHash() )
        XCTAssertEqual(
            repoData?.backlog.map { try? $0.blockHash() },
            bhTest[..<15].suffix(Settings.BITCOIN_MAX_HEADERS_OVERWRITE).map { try? $0.blockHash() }
        )
        XCTAssertNotNil(repoData)
        XCTAssertNil(repoData?.cfHeader)
        XCTAssertNil(repoData?.filter)
        XCTAssertNil(repoData?.fullBlock)
        
        TestingHelpers.Node.closeNetworkState(networkState, eventLoop: self.embedded)
    }
    
    func testLoadWorksheetOnlyCFHeader() throws {
        let (bhTest, cfHeaderSerial, _) = Self.dbWriteTest20(eventLoop: self.elg.next())
        
        let bhRepo = self.makeBhRepo(offset: 1000)
        let cfHeaderRepo = self.makeCfRepo(offset: 1000)
        defer {
            self.deferClose(bhRepo: bhRepo, cfHeaderRepo: cfHeaderRepo)
        }

        for id in (1000..<1015) {
            XCTAssertNoThrow(try cfHeaderRepo.addFlags(id: id, flags: [.hasFilterHeader, .nonMatchingFilter, .processed]).wait())
        }
        
        let networkState = TestingHelpers.Node.createNetworkState(offset: 1000,
                                                                  muxer: self.muxer,
                                                                  cfMatcher: self.cfMatcher,
                                                                  eventLoop: self.embedded,
                                                                  threadPool: self.threadPool,
                                                                  nioFileHandle: self.fileHandle)

        var repoData: Node.Worksheet.Properties? = nil
        XCTAssertNoThrow(
            repoData = try networkState.loadWorksheetData(skipLocator: false,
                                                          processed: 0,
                                                          eventLoop: self.embedded)
                .wait()
        )
        XCTAssertEqual(repoData?.cfHeader?.start.headerHash, cfHeaderSerial[14].headerHash)
        XCTAssertEqual(repoData?.cfHeader?.stops.count, 1)
        XCTAssertNoThrow(
            XCTAssertEqual(try repoData?.cfHeader?.stops.first?.blockHash(),
                           try bhTest.last?.blockHash())
        )
        XCTAssertNotNil(repoData)
        XCTAssertNil(repoData?.filter)
        XCTAssertNil(repoData?.fullBlock)
        
        TestingHelpers.Node.closeNetworkState(networkState, eventLoop: self.embedded)
    }

    func testLoadWorksheet() {
        Settings.BITCOIN_CF_BUNDLE_SIZE = 5
        let (bhTest, cfHeaderSerial, cfHeader) = Self.dbWriteTest20(eventLoop: self.elg.next())
        
        let bhRepo = self.makeBhRepo(offset: 1000)
        let cfHeaderRepo = self.makeCfRepo(offset: 1000)
        defer {
            self.repoClose(bhRepo)
            self.repoClose(cfHeaderRepo)
        }
        XCTAssertNoThrow(try cfHeaderRepo.addFlags(id: 1010, flags: .matchingFilter).wait())
        
        let networkState = TestingHelpers.Node.createNetworkState(offset: 1000,
                                                                  muxer: self.muxer,
                                                                  cfMatcher: self.cfMatcher,
                                                                  eventLoop: self.embedded,
                                                                  threadPool: self.threadPool,
                                                                  nioFileHandle: self.fileHandle)
        
        var repoData: Node.Worksheet.Properties! = nil
        XCTAssertNoThrow(
            repoData = try networkState.loadWorksheetData(skipLocator: false,
                                                          processed: 0,
                                                          eventLoop: self.embedded).wait()
        )

        XCTAssertEqual(repoData?.bhHeight, 1019)
        XCTAssertEqual(repoData?.locator?.hashes.first, try? bhTest.last?.blockHash() )
        XCTAssertEqual(repoData?.locator?.hashes.last, try? bhTest.first?.blockHash() )
        XCTAssertEqual(
            repoData?.backlog.map { try? $0.blockHash() },
            bhTest.suffix(Settings.BITCOIN_MAX_HEADERS_OVERWRITE).map { try? $0.blockHash() }
        )
        XCTAssertEqual(repoData?.cfHeader?.start.headerHash, cfHeaderSerial[14].headerHash)
        XCTAssertEqual(repoData?.cfHeader?.stops.count, 1)
        XCTAssertNoThrow(
            XCTAssertEqual(try repoData?.cfHeader?.stops.first?.blockHash(),
                           try bhTest.last?.blockHash())
        )
        XCTAssertGreaterThan(Settings.BITCOIN_CF_CONCURRENT_INVOCATIONS, 4)
        XCTAssertNoThrow(
            XCTAssertEqual(try repoData?.filter?.first?.map(\.0).map { try $0.blockHash() },
                           try bhTest[11..<15].map { try $0.blockHash() })
        )
        XCTAssertEqual(repoData?.filter?.first?.map(\.1).map(\.headerHash), cfHeader[11..<15].map(\.headerHash))
        XCTAssertNoThrow(
            XCTAssertEqual(try repoData?.fullBlock?.first?.blockHash(), try bhTest[10].blockHash())
        )
        
        TestingHelpers.Node.closeNetworkState(networkState, eventLoop: self.embedded)
    }
    
    func setup1000FullBlockDownload() -> [BlockHeader] {
        let eventLoop = self.elg.next()
        let bhTest = TestingHelpers.DB.writeFakeBlockHeaders(count: 1000, eventLoop: eventLoop)
        TestingHelpers.DB.writeFakeCompactFilters(count: 1000, processed: 1000, eventLoop: eventLoop)
        
        let repo = TestingHelpers.DB.getFileCFHeaderRepo(offset: 10_000,
                                                         threadPool: threadPool,
                                                         eventLoop: eventLoop)
        XCTAssertNoThrow(try repo.close().wait())
        
        return bhTest
    }
    
    func fbDownloadTag(_ index: Int, tag: File.RecordFlag = [ .hasFilterHeader, .matchingFilter ]) throws {
        let repo = TestingHelpers.DB.getFileCFHeaderRepo(offset: 10_000,
                                                         threadPool: self.threadPool,
                                                         eventLoop: self.elg.next())

        let flags = try repo.find(id: index).wait()
        let modified = CF.SerialHeader(recordFlag: tag,
                                       id: index,
                                       filterHash: flags.filterHash,
                                       headerHash: flags.headerHash)
        XCTAssertNoThrow(try repo.write(modified).wait())
        XCTAssertNoThrow(try repo.close().wait())
    }

    func testLoadFirstFullBlockDownload() {
        Settings.BITCOIN_FULL_BLOCK_DOWNLOADER_BATCH_SIZE = 2

        let bhTest = self.setup1000FullBlockDownload()
        XCTAssertNoThrow(try self.fbDownloadTag(10_000))
        
        let networkState = TestingHelpers.Node.createNetworkState(offset: 10_000,
                                                                  muxer: self.muxer,
                                                                  cfMatcher: self.cfMatcher,
                                                                  eventLoop: self.embedded,
                                                                  threadPool: self.threadPool,
                                                                  nioFileHandle: self.fileHandle)
        var repoData: Node.Worksheet.Properties! = nil
        XCTAssertNoThrow(
            repoData = try networkState.loadWorksheetData(skipLocator: true,
                                                          processed: 0,
                                                          eventLoop: self.embedded)
                .wait()
        )

        XCTAssertEqual(Settings.NODE_PROPERTIES.processedIndex, 0)
        XCTAssertNoThrow(XCTAssertEqual(repoData.fullBlock?.count, 1))
        XCTAssertNoThrow(XCTAssertEqual(try repoData.fullBlock?.first?.blockHash(), try bhTest[0].blockHash()))
        
        TestingHelpers.Node.closeNetworkState(networkState, eventLoop: self.embedded)
    }

    func testLoadDoubleFirstFullBlockDownload() {
        Settings.BITCOIN_FULL_BLOCK_DOWNLOADER_BATCH_SIZE = 2

        let bhTest = self.setup1000FullBlockDownload()
        XCTAssertNoThrow(try self.fbDownloadTag(10_000))
        XCTAssertNoThrow(try self.fbDownloadTag(10_001))
        
        let networkState = TestingHelpers.Node.createNetworkState(offset: 10_000,
                                                                  muxer: self.muxer,
                                                                  cfMatcher: self.cfMatcher,
                                                                  eventLoop: self.embedded,
                                                                  threadPool: self.threadPool,
                                                                  nioFileHandle: self.fileHandle)
        var repoData: Node.Worksheet.Properties! = nil
        XCTAssertNoThrow(
            repoData = try networkState.loadWorksheetData(skipLocator: true,
                                                          processed: 0,
                                                          eventLoop: self.embedded)
                .wait()
        )

        XCTAssertEqual(Settings.NODE_PROPERTIES.processedIndex, 0)
        XCTAssertNoThrow(XCTAssertEqual(repoData.fullBlock?.count, 2))
        XCTAssertNoThrow(XCTAssertEqual(try repoData.fullBlock?.first?.blockHash(), try bhTest[0].blockHash()))
        XCTAssertNoThrow(XCTAssertEqual(try repoData.fullBlock?.last?.blockHash(), try bhTest[1].blockHash()))

        TestingHelpers.Node.closeNetworkState(networkState, eventLoop: self.embedded)
    }

    func testLoadMiddleFullBlockDownload() {
        let bhTest = self.setup1000FullBlockDownload()
        XCTAssertNoThrow(try self.fbDownloadTag(10_499))
        
        let networkState = TestingHelpers.Node.createNetworkState(offset: 10_000,
                                                                  muxer: self.muxer,
                                                                  cfMatcher: self.cfMatcher,
                                                                  eventLoop: self.embedded,
                                                                  threadPool: self.threadPool,
                                                                  nioFileHandle: self.fileHandle)
        var repoData: Node.Worksheet.Properties! = nil
        XCTAssertNoThrow(
            repoData = try networkState.loadWorksheetData(skipLocator: true,
                                                          processed: 0,
                                                          eventLoop: self.embedded)
                .wait()
        )

        XCTAssertEqual(Settings.NODE_PROPERTIES.processedIndex, 10498)
        XCTAssertNoThrow(XCTAssertEqual(repoData.fullBlock?.count, 1))
        XCTAssertNoThrow(XCTAssertEqual(try repoData.fullBlock?.first?.blockHash(), try bhTest[499].blockHash()))

        TestingHelpers.Node.closeNetworkState(networkState, eventLoop: self.embedded)
    }

    func testLoadLastFullBlockDownload() {
        let bhTest = self.setup1000FullBlockDownload()
        XCTAssertNoThrow(try self.fbDownloadTag(10_999))
        
        let networkState = TestingHelpers.Node.createNetworkState(offset: 10_000,
                                                                  muxer: self.muxer,
                                                                  cfMatcher: self.cfMatcher,
                                                                  eventLoop: self.embedded,
                                                                  threadPool: self.threadPool,
                                                                  nioFileHandle: self.fileHandle)
        var repoData: Node.Worksheet.Properties! = nil
        XCTAssertNoThrow(
            repoData = try networkState.loadWorksheetData(skipLocator: true,
                                                          processed: 0,
                                                          eventLoop: self.embedded)
                .wait()
        )

        XCTAssertEqual(Settings.NODE_PROPERTIES.processedIndex, 10_998)
        XCTAssertNoThrow(XCTAssertEqual(repoData.fullBlock?.count, 1))
        XCTAssertNoThrow(XCTAssertEqual(try repoData.fullBlock?.first?.blockHash(), try bhTest[999].blockHash()))

        TestingHelpers.Node.closeNetworkState(networkState, eventLoop: self.embedded)
    }

    func testLoadFirstLastFullBlockDownload() {
        Settings.BITCOIN_FULL_BLOCK_DOWNLOADER_BATCH_SIZE = 2

        let bhTest = self.setup1000FullBlockDownload()
        XCTAssertNoThrow(try self.fbDownloadTag(10_000))
        XCTAssertNoThrow(try self.fbDownloadTag(10_999))

        let networkState = TestingHelpers.Node.createNetworkState(offset: 10_000,
                                                                  muxer: self.muxer,
                                                                  cfMatcher: self.cfMatcher,
                                                                  eventLoop: self.embedded,
                                                                  threadPool: self.threadPool,
                                                                  nioFileHandle: self.fileHandle)
        var repoData: Node.Worksheet.Properties! = nil
        XCTAssertNoThrow(
            repoData = try networkState.loadWorksheetData(skipLocator: true,
                                                          processed: 0,
                                                          eventLoop: self.embedded)
                .wait()
        )

        XCTAssertEqual(Settings.NODE_PROPERTIES.processedIndex, 0)
        XCTAssertNoThrow(XCTAssertEqual(repoData.fullBlock?.count, 2))
        XCTAssertNoThrow(XCTAssertEqual(try repoData.fullBlock?.first?.blockHash(), try bhTest[0].blockHash()))
        XCTAssertNoThrow(XCTAssertEqual(try repoData.fullBlock?.last?.blockHash(), try bhTest[999].blockHash()))

        TestingHelpers.Node.closeNetworkState(networkState, eventLoop: self.embedded)
    }

    func testLoadMiddleLastFullBlockDownload() {
        Settings.BITCOIN_FULL_BLOCK_DOWNLOADER_BATCH_SIZE = 2

        let bhTest = self.setup1000FullBlockDownload()
        XCTAssertNoThrow(try self.fbDownloadTag(10_499))
        XCTAssertNoThrow(try self.fbDownloadTag(10_999))

        let networkState = TestingHelpers.Node.createNetworkState(offset: 10_000,
                                                                  muxer: self.muxer,
                                                                  cfMatcher: self.cfMatcher,
                                                                  eventLoop: self.embedded,
                                                                  threadPool: self.threadPool,
                                                                  nioFileHandle: self.fileHandle)
        var repoData: Node.Worksheet.Properties! = nil
        XCTAssertNoThrow(
            repoData = try networkState.loadWorksheetData(skipLocator: true,
                                                          processed: 0,
                                                          eventLoop: self.embedded)
                .wait()
        )

        XCTAssertEqual(Settings.NODE_PROPERTIES.processedIndex, 10498)
        XCTAssertNoThrow(XCTAssertEqual(repoData.fullBlock?.count, 2))
        XCTAssertNoThrow(XCTAssertEqual(try repoData.fullBlock?.first?.blockHash(), try bhTest[499].blockHash()))
        XCTAssertNoThrow(XCTAssertEqual(try repoData.fullBlock?.last?.blockHash(), try bhTest[999].blockHash()))

        TestingHelpers.Node.closeNetworkState(networkState, eventLoop: self.embedded)
    }
    
    func testLoadNilFullBlockDownload() {
        Settings.BITCOIN_FULL_BLOCK_DOWNLOADER_BATCH_SIZE = 2

        _ = self.setup1000FullBlockDownload()
        XCTAssertNoThrow(try self.fbDownloadTag(10_499))
        XCTAssertNoThrow(try self.fbDownloadTag(10_999))
        XCTAssertNoThrow(try self.fbDownloadTag(10_001, tag: [ .hasFilterHeader ]))

        let networkState = TestingHelpers.Node.createNetworkState(offset: 10_000,
                                                                  muxer: self.muxer,
                                                                  cfMatcher: self.cfMatcher,
                                                                  eventLoop: self.embedded,
                                                                  threadPool: self.threadPool,
                                                                  nioFileHandle: self.fileHandle)
        var repoData: Node.Worksheet.Properties! = nil
        XCTAssertNoThrow(
            repoData = try networkState.loadWorksheetData(skipLocator: true,
                                                          processed: 0,
                                                          eventLoop: self.embedded)
                .wait()
        )
        XCTAssertEqual(Settings.NODE_PROPERTIES.processedIndex, 10_000)
        XCTAssertNoThrow(XCTAssertNil(repoData.fullBlock))

        TestingHelpers.Node.closeNetworkState(networkState, eventLoop: self.embedded)
    }

    func testLoadFirstLastBreakInTheMiddleFullBlockDownload() {
        Settings.BITCOIN_FULL_BLOCK_DOWNLOADER_BATCH_SIZE = 2

        let bhTest = self.setup1000FullBlockDownload()
        XCTAssertNoThrow(try self.fbDownloadTag(10_000))
        XCTAssertNoThrow(try self.fbDownloadTag(10_499, tag: [ .hasFilterHeader, .nonMatchingFilter ]))
        XCTAssertNoThrow(try self.fbDownloadTag(10_999))

        let networkState = TestingHelpers.Node.createNetworkState(offset: 10_000,
                                                                  muxer: self.muxer,
                                                                  cfMatcher: self.cfMatcher,
                                                                  eventLoop: self.embedded,
                                                                  threadPool: self.threadPool,
                                                                  nioFileHandle: self.fileHandle)
        var repoData: Node.Worksheet.Properties! = nil
        XCTAssertNoThrow(
            repoData = try networkState.loadWorksheetData(skipLocator: true,
                                                          processed: 0,
                                                          eventLoop: self.embedded)
                .wait()
        )

        XCTAssertEqual(Settings.NODE_PROPERTIES.processedIndex, 0)
        XCTAssertNoThrow(XCTAssertEqual(repoData.fullBlock?.count, 1))
        XCTAssertNoThrow(XCTAssertEqual(try repoData.fullBlock?.first?.blockHash(), try bhTest[0].blockHash()))

        TestingHelpers.Node.closeNetworkState(networkState, eventLoop: self.embedded)
    }

    func testLoadRhapsodyFullBlockDownload() {
        Settings.BITCOIN_FULL_BLOCK_DOWNLOADER_BATCH_SIZE = 4

        let bhTest = self.setup1000FullBlockDownload()
        XCTAssertNoThrow(try self.fbDownloadTag(10_000))
        XCTAssertNoThrow(try self.fbDownloadTag(10_001))
        XCTAssertNoThrow(try self.fbDownloadTag(10_499))
        XCTAssertNoThrow(try self.fbDownloadTag(10_999))

        let networkState = TestingHelpers.Node.createNetworkState(offset: 10_000,
                                                                  muxer: self.muxer,
                                                                  cfMatcher: self.cfMatcher,
                                                                  eventLoop: self.embedded,
                                                                  threadPool: self.threadPool,
                                                                  nioFileHandle: self.fileHandle)
        var repoData: Node.Worksheet.Properties! = nil
        XCTAssertNoThrow(
            repoData = try networkState.loadWorksheetData(skipLocator: true,
                                                          processed: 0,
                                                          eventLoop: self.embedded)
                .wait()
        )

        XCTAssertEqual(Settings.NODE_PROPERTIES.processedIndex, 0)
        XCTAssertEqual(repoData!.bhHeight, 10_999)
        XCTAssertNil(repoData!.locator)
        
        guard var fb = repoData.fullBlock
        else { XCTFail(); return }
        
        XCTAssertEqual(fb.count, 4)
        XCTAssertNoThrow(
            XCTAssertEqual(try fb.popFirst()?.blockHash(), try bhTest[0].blockHash())
        )
        XCTAssertNoThrow(
            XCTAssertEqual(try fb.popFirst()?.blockHash(), try bhTest[1].blockHash())
        )
        XCTAssertNoThrow(
            XCTAssertEqual(try fb.popFirst()?.blockHash(), try bhTest[499].blockHash())
        )
        XCTAssertNoThrow(
            XCTAssertEqual(try fb.popFirst()?.blockHash(), try bhTest[999].blockHash())
        )

        XCTAssertNoThrow(try self.fbDownloadTag(10_002, tag: .hasFilterHeader))
        XCTAssertNoThrow(
            repoData = try networkState.loadWorksheetData(skipLocator: true,
                                                          processed: 0,
                                                          eventLoop: self.embedded).wait()
        )
        XCTAssertEqual(repoData.fullBlock?.count, 2)
        XCTAssertEqual(try repoData.fullBlock?.first?.blockHash(), try bhTest[0].blockHash())
        XCTAssertEqual(try repoData.fullBlock?.last?.blockHash(), try bhTest[1].blockHash())

        XCTAssertNoThrow(try self.fbDownloadTag(10_000, tag: [ .hasFilterHeader, .matchingFilter, .processed ]))
        XCTAssertNoThrow(
            repoData = try networkState.loadWorksheetData(skipLocator: true,
                                                          processed: 0,
                                                          eventLoop: self.embedded).wait()
        )
        XCTAssertEqual(repoData.fullBlock?.count, 1)
        XCTAssertEqual(try repoData.fullBlock?.first?.blockHash(), try bhTest[1].blockHash())

        TestingHelpers.Node.closeNetworkState(networkState, eventLoop: self.embedded)
        
    }
}
