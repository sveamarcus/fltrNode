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
import fltrECC
import fltrTx
import fltrWAPI
import HaByLo
import NodeTestLibrary
import NIO
import XCTest

extension NodeTests {
    // MARK: Static Downloaders
    func testDownloadHeadersEmpty() {
        let (bhData, _, _) = Self.dbWriteTest20(eventLoop: self.elg.next())

        let latestBlockLocator = self.latestBlockLocator()
        let downloadFuture = Node.BlockHeaderDownload.blockHeaderDownload(locator: latestBlockLocator,
                                                                          blockHeaderBacklog: bhData.suffix(10),
                                                                          muxer: self.muxer,
                                                                          nioThreadPool: self.threadPool,
                                                                          eventLoop: self.embedded)
        XCTAssert(self.nodeReadPending().bitcoinCommandValue is GetheadersCommand)
        self.nodeWritePendingCommand(
            HeadersCommand(headers: [])
        )

        XCTAssertThrowsError(try downloadFuture.wait()) { error in
            switch error {
            case Node.HeadersError.emptyPayload:
                break
            default:
                XCTFail()
            }
        }
    }
    
    func testDownloadHeadersSingleNoRollback() {
        let (bhData, _, _) = Self.dbWriteTest20(eventLoop: self.elg.next())
        guard let last = bhData.last else {
            XCTFail()
            return
        }
        let subsequent = Self.nextHeader(last)

        let downloadFuture = Node.BlockHeaderDownload.blockHeaderDownload(locator: self.latestBlockLocator(),
                                                                          blockHeaderBacklog: bhData.suffix(10),
                                                                          muxer: self.muxer,
                                                                          nioThreadPool: self.threadPool,
                                                                          eventLoop: self.embedded)
        guard let getheaders = self.nodeReadPending().bitcoinCommandValue as? GetheadersCommand else {
            XCTFail()
            return
        }
        XCTAssertEqual(getheaders.blockLocator.hashes.first, try? bhData.last?.blockHash())
        self.nodeWritePendingCommand(
            HeadersCommand(headers: [subsequent.wire])
        )

        do {
            let (headers, rollback) = try downloadFuture.wait()
            XCTAssertEqual(try last.blockHash(), try headers.first?.previousBlockHash())
            XCTAssertEqual(last.id + 1, headers.first?.id)
            XCTAssertNil(rollback)
        } catch {
            XCTFail(error.localizedDescription)
            return
        }
    }
    
    func testDownloadHeadersMultiNoRollback() {
        let (bhData, _, _) = Self.dbWriteTest20(eventLoop: self.elg.next())
        guard let last = bhData.last else {
            XCTFail()
            return
        }
        let subsequent1 = Self.nextHeader(last)
        let subsequent2 = Self.nextHeader(subsequent1.full)
        let subsequent3 = Self.nextHeader(subsequent2.full)
        let sendHeaders = [ subsequent1.wire, subsequent2.wire, subsequent3.wire ]

        let downloadFuture = Node.BlockHeaderDownload.blockHeaderDownload(locator: self.latestBlockLocator(),
                                                                          blockHeaderBacklog: bhData.suffix(10),
                                                                          muxer: self.muxer,
                                                                          nioThreadPool: self.threadPool,
                                                                          eventLoop: self.embedded)
        guard let getheaders = self.nodeReadPending().bitcoinCommandValue as? GetheadersCommand else {
            XCTFail()
            return
        }
        XCTAssertEqual(getheaders.blockLocator.hashes.first, try? last.blockHash())
        self.nodeWritePendingCommand(
            HeadersCommand(headers: sendHeaders)
        )

        do {
            let (headers, rollback) = try downloadFuture.wait()
            XCTAssertEqual(headers, [ subsequent1.full, subsequent2.full, subsequent3.full ])
            XCTAssertEqual(try last.blockHash(), try headers.first?.previousBlockHash())
            XCTAssertEqual(last.id + 1, headers.first?.id)
            XCTAssertNil(rollback)
        } catch {
            XCTFail(error.localizedDescription)
            return
        }
    }

    func testDownloadHeadersRollback() {
        let (bhData, _, _) = Self.dbWriteTest20(eventLoop: self.elg.next())
        let last = bhData[bhData.endIndex - 5]

        let subsequent1 = Self.nextHeader(last)
        let subsequent2 = Self.nextHeader(subsequent1.full)
        let subsequent3 = Self.nextHeader(subsequent2.full)
        let sendHeaders = [ subsequent1.wire, subsequent2.wire, subsequent3.wire ]

        let downloadFuture = Node.BlockHeaderDownload.blockHeaderDownload(locator: self.latestBlockLocator(),
                                                                          blockHeaderBacklog: bhData.suffix(10),
                                                                          muxer: self.muxer,
                                                                          nioThreadPool: self.threadPool,
                                                                          eventLoop: self.embedded)
        guard let _ = self.nodeReadPending().bitcoinCommandValue as? GetheadersCommand else {
            XCTFail()
            return
        }
        self.nodeWritePendingCommand(
            HeadersCommand(headers: sendHeaders)
        )

        do {
            let (headers, rollback) = try downloadFuture.wait()
            XCTAssertEqual(headers, [ subsequent1.full, subsequent2.full, subsequent3.full ])
            XCTAssertEqual(try last.blockHash(), try headers.first?.previousBlockHash())
            XCTAssertEqual(last.id + 1, headers.first?.id)
            XCTAssertNotNil(rollback)
            XCTAssertEqual(rollback, last.id)
        } catch {
            XCTFail(error.localizedDescription)
            return
        }
    }
    
    func testDownloadHeadersFailBacklog() {
        let (bhData, _, _) = Self.dbWriteTest20(eventLoop: self.elg.next())
        let last = bhData[bhData.endIndex - 12]

        let subsequent1 = Self.nextHeader(last)
        let subsequent2 = Self.nextHeader(subsequent1.full)
        let subsequent3 = Self.nextHeader(subsequent2.full)
        let sendHeaders = [ subsequent1.wire, subsequent2.wire, subsequent3.wire ]

        let downloadFuture = Node.BlockHeaderDownload.blockHeaderDownload(
            locator: self.latestBlockLocator(),
            blockHeaderBacklog: bhData.suffix(10),
            muxer: self.muxer,
            nioThreadPool: self.threadPool,
            eventLoop: self.embedded
        )
        guard let _ = self.nodeReadPending().bitcoinCommandValue as? GetheadersCommand else {
            XCTFail()
            return
        }
        self.nodeWritePendingCommand(
            HeadersCommand(headers: sendHeaders)
        )

        XCTAssertThrowsError(try downloadFuture.wait()) { error in
            switch error as? Node.HeadersError {
            case let .couldNotConnectWithBlockHeaderBacklog(hash):
                XCTAssertEqual(hash, try? subsequent1.wire.previousBlockHash())
            default:
                XCTFail()
                return
            }
        }
    }
    
    func testWalletNotifyRollbackNewTip() {
        let (bhData, _, _) = Self.dbWriteTest20(eventLoop: self.elg.next())

        XCTAssertThrowsError(
            _ = try Node.BlockHeaderDownload.walletNotifyNewTip(full: bhData,
                                                                rollback: 1,
                                                                nodeDelegate: self.nodeDelegate,
                                                                eventLoop: self.embedded)
                .wait()
        ) { error in
            switch error as? Node.Error {
            case .rollback(let newHeight):
                XCTAssertEqual(newHeight, 1)
            default:
                XCTFail(error.localizedDescription)
            }
        }
    }

    func testWalletNotifyNewTip() {
        let (bhData, _, _) = Self.dbWriteTest20(eventLoop: self.elg.next())

        XCTAssertNoThrow(
            _ = try Node.BlockHeaderDownload.walletNotifyNewTip(full: Array(bhData[..<10]),
                                                                rollback: nil,
                                                                nodeDelegate: self.nodeDelegate,
                                                                eventLoop: self.embedded)
                .wait()
        )
        XCTAssertEqual(nodeDelegate.newTipHeight, 9)
    }
    
    func testWalletNotifyNewTip2() {
        let (bhData, _, _) = Self.dbWriteTest20(eventLoop: self.elg.next())

        XCTAssertNoThrow(
            _ = try Node.BlockHeaderDownload.walletNotifyNewTip(full: Array(bhData[..<15]),
                                            rollback: nil,
                                            nodeDelegate: self.nodeDelegate,
                                            eventLoop: self.embedded)
                .wait()
        )
        XCTAssertEqual(nodeDelegate.newTipHeight, 14)
    }
    
    func testWalletNotifyTimeoutRollbackNewTip() {
        let (bhData, _, _) = Self.dbWriteTest20(eventLoop: self.elg.next())

        class TimeoutRollbackDelegate: EmptyTestDelegate {
            let eventLoop: EmbeddedEventLoopLocked
            @Setting(\.BITCOIN_WALLET_DELEGATE_COMMIT_TIMEOUT) var timeout: TimeAmount

            init(_ eventLoop: EmbeddedEventLoopLocked) {
                self.eventLoop = eventLoop
                super.init()
            }

            override func rollback(height: Int, commit: () -> Void) {
                self.eventLoop.advanceTime(to: .now() + self.timeout)
                super.rollback(height: height, commit: commit)
            }
        }

        class TimeoutNewTipDelegate: EmptyTestDelegate {
            let eventLoop: EmbeddedEventLoopLocked
            @Setting(\.BITCOIN_WALLET_DELEGATE_COMMIT_TIMEOUT) var timeout: TimeAmount

            init(_ eventLoop: EmbeddedEventLoopLocked) {
                self.eventLoop = eventLoop
                super.init()
            }

            override func newTip(height: Int, commit: () -> Void) {
                self.eventLoop.advanceTime(to: .now() + self.timeout)

                super.newTip(height: height, commit: commit)
            }
        }

        let timeoutError: (Error) -> Void = { error in
            switch error as? Node.Error {
            case .timeoutErrorWaitingForDelegate, .rollback:
                break
            default:
                XCTFail(error.localizedDescription)
            }
        }


        XCTAssertThrowsError(
            _ = try Node.BlockHeaderDownload.walletNotifyNewTip(full: bhData,
                                                                rollback: 1,
                                                                nodeDelegate: TimeoutRollbackDelegate(self.embedded),
                                                                eventLoop: self.embedded)
                .wait()
            , "", timeoutError)
        
        XCTAssertNoThrow(
            _ = try Node.BlockHeaderDownload.walletNotifyNewTip(full: bhData,
                                                                rollback: nil,
                                                                nodeDelegate: TimeoutRollbackDelegate(self.embedded),
                                                                eventLoop: self.embedded)
                .wait()
        )
        
        XCTAssertThrowsError(
            _ = try Node.BlockHeaderDownload.walletNotifyNewTip(full: bhData,
                                                                rollback: 1,
                                                                nodeDelegate: TimeoutNewTipDelegate(self.embedded),
                                                                eventLoop: self.embedded)
                .wait()
            , "", timeoutError)
        
        XCTAssertThrowsError(
            _ = try Node.BlockHeaderDownload.walletNotifyNewTip(full: bhData,
                                                                rollback: nil,
                                                                nodeDelegate: TimeoutNewTipDelegate(self.embedded),
                                                                eventLoop: self.embedded)
                .wait()
            , "", timeoutError)
    }

    func testVerifyBlockHeaderHashChain() throws {
        let (bhData, _, _) = Self.dbWriteTest20(eventLoop: self.elg.next())
        var bhDataSlice = bhData[...]
        var first = bhDataSlice.popFirst()!
        XCTAssertNoThrow(try first.addPreviousBlockHash(.zero))

        let full = [ first ] + zip([ first ] + bhDataSlice, bhDataSlice)
        .map { first, second in
            var copy = second
            try! copy.addPreviousBlockHash(first.blockHash())
            return copy
        }

        XCTAssertNoThrow(
            try Node.BlockHeaderDownload.verifyBlockHeaderHashChain(full: full,
                                                                    eventLoop: self.embedded)
        )
    }
    
    func testVerifyBlockHeaderHashChainFail() throws {
        let (bhData, _, _) = Self.dbWriteTest20(eventLoop: self.elg.next())
        var bhDataSlice = bhData[...]
        var first = bhDataSlice.popFirst()!
        XCTAssertNoThrow(try first.addPreviousBlockHash(.zero))

        let full = zip([ first ] + bhDataSlice, bhDataSlice)
        .map { first, second in
            var copy = second
            try! copy.addPreviousBlockHash(first.blockHash())
            return copy
        } + [ first ]

        XCTAssertThrowsError(
            try Node.BlockHeaderDownload.verifyBlockHeaderHashChain(full: full,
                                                                    eventLoop: self.embedded)
        )
    }

    func testGetCFHeadersAndStore() {
        let (_, cfWire, _) = Self.dbWriteTest20(eventLoop: self.elg.next())
        let bhRepo = self.makeBhRepo()
        let cfHeaderRepo = self.makeCfRepo()
        defer {
            sync()
            self.deferClose(bhRepo: bhRepo, cfHeaderRepo: cfHeaderRepo)
        }

        var nextWire: [CF.Header] = [ Self.nextHeader(cfWire.last!) ]
        for _ in (1...4) {
            nextWire.append(Self.nextHeader(nextWire.last!))
        }

        var bhIndex: Int!
        XCTAssertNoThrow(bhIndex = try bhRepo.heights().wait().upperHeight)
        var cfIndex: Int!
        XCTAssertNoThrow(cfIndex = try cfHeaderRepo.cfHeadersTip(processed: 0).wait())

        // Download headers
        var getCfHeader: Future<ArraySlice<CF.SerialHeader>>!
        XCTAssertNoThrow(
            getCfHeader = Node.CFHeaderDownload.cfHeaderDownload(start: try cfHeaderRepo.find(id: cfIndex).wait(),
                                                                 stop: try bhRepo.find(id: bhIndex).wait(),
                                                                 muxer: self.muxer,
                                                                 eventLoop: self.embedded)
        )

        // fake reply with next headers
        guard let request = self.nodeReadPending().bitcoinCommandValue as? CF.GetcfheadersCommand else {
            XCTFail()
            return
        }
        self.nodeWritePendingCommand(CF.CfheadersCommand(filterType: .basic, stop: request.stopHash, headers: nextWire))

        // Write headers
        XCTAssertNoThrow(try getCfHeader.wait())
        XCTAssertNoThrow(
            Node.CFHeaderDownload.store(cfHeaders: getCfHeader,
                                        cfHeaderRepo: cfHeaderRepo)
        )

        Thread.sleep(forTimeInterval: 0.1)

        // Load and verify
        XCTAssertNoThrow(
            try nextWire.enumerated().forEach {
                XCTAssertEqual(try cfHeaderRepo.findWire(id: 15 + $0.offset).wait(), $0.element)
            }
        )
        XCTAssertThrowsError(try cfHeaderRepo.findWire(id: 20).wait())
    }
    
    func testGetCFHeadersFails() throws {
        let (bhData, _, _) = Self.dbWriteTest20(eventLoop: self.elg.next())
        let bhRepo = self.makeBhRepo()
        let cfRepo = self.makeCfRepo()
        defer {
            self.repoClose(bhRepo)
            self.repoClose(cfRepo)
        }

        var bhIndex: Int!
        XCTAssertNoThrow(bhIndex = try bhRepo.heights().wait().upperHeight)
        var cfIndex: Int!
        XCTAssertNoThrow(cfIndex = try cfRepo.cfHeadersTip(processed: 0).wait())

        let illegalStopHash = Node.CFHeaderDownload.cfHeaderDownload(start: try cfRepo.find(id: cfIndex).wait(),
                                                    stop: try bhRepo.find(id: bhIndex).wait(),
                                                    muxer: self.muxer,
                                                    eventLoop: self.embedded)

        guard let _ = self.nodeReadPending().bitcoinCommandValue as? CF.GetcfheadersCommand else {
            XCTFail()
            return
        }
        self.nodeWritePendingCommand(CF.CfheadersCommand(filterType: .basic, stop: .zero, headers: []))
        XCTAssertThrowsError(try illegalStopHash.wait()) { error in
            switch error {
            case Node.HeadersError.unexpectedStopHash: break
            default: XCTFail()
            }
        }

        let emptyPayload = Node.CFHeaderDownload.cfHeaderDownload(start: try cfRepo.find(id: cfIndex).wait(),
                                                                  stop: try bhRepo.find(id: bhIndex).wait(),
                                                                  muxer: self.muxer,
                                                                  eventLoop: self.embedded)
        guard let requestCFHeadersEmptyPayload = self.nodeReadPending().bitcoinCommandValue as? CF.GetcfheadersCommand else {
            XCTFail()
            return
        }
        XCTAssertNoThrow(
            XCTAssertEqual(requestCFHeadersEmptyPayload.stopHash, try bhData.last!.blockHash())
        )

        self.nodeWritePendingCommand(
            CF.CfheadersCommand(filterType: .basic, stop: requestCFHeadersEmptyPayload.stopHash, headers: [])
        )
        XCTAssertThrowsError(try emptyPayload.wait()) { error in
            switch error {
            case Node.HeadersError.emptyPayload: break
            default: XCTFail()
            }
        }
    }
    
    // MARK: BlockHeaderDownload
    func testBlockHeaderDownloadAndStore() {
        let (fakeHeaders, _, _) = Self.dbWriteTest20(eventLoop: self.elg.next())
        var downloadHeaders = Self.nextHeaders(count: 10, last: fakeHeaders.last!).wire[...]
        let locator = self.latestBlockLocator()
        let backlog = Array(fakeHeaders.suffix(3))
        let maxHeadersOverwrite = 2
        let bhRepo = self.makeBhRepo()
        defer {
            self.repoClose(bhRepo)
        }
        
        let downloader = Node.BlockHeaderDownload(count: 2,
                                                  locator: locator,
                                                  backlog: backlog,
                                                  maxHeadersOverwrite: maxHeadersOverwrite,
                                                  bhRepo: bhRepo,
                                                  muxer: self.muxer,
                                                  nodeDelegate: self.nodeDelegate,
                                                  threadPool: self.threadPool,
                                                  eventLoop: self.embedded)
        
        let future = downloader.start()
        let firstGet = self.nodeReadPending()
        XCTAssert(firstGet.bitcoinCommandValue is GetheadersCommand)
        self.nodeWritePendingCommand(HeadersCommand(headers: [ downloadHeaders.popFirst()! ]))
        Thread.sleep(forTimeInterval: 0.1)
        XCTAssertEqual(self.nodeDelegate.newTipHeight, 20)

        let secondGet = self.nodeReadPending()
        XCTAssert(secondGet.bitcoinCommandValue is GetheadersCommand)
        self.nodeWritePendingCommand(HeadersCommand(headers: [ downloadHeaders.popFirst()! ]))
        Thread.sleep(forTimeInterval: 0.1)
        XCTAssertEqual(self.nodeDelegate.newTipHeight, 21)
        XCTAssertNoThrow(XCTAssertEqual(try future.wait(), .processed))
        XCTAssertEqual(self.waitNow(bhRepo.find(from: 20)).map(\.id), [20, 21])
    }
    
    func testBlockHeaderDownloadCancel() {
        let (fakeHeaders, _, _) = Self.dbWriteTest20(eventLoop: self.elg.next())
        var downloadHeaders = Self.nextHeaders(count: 10, last: fakeHeaders.last!).wire[...]
        let locator = self.latestBlockLocator()
        let backlog = Array(fakeHeaders.suffix(3))
        let maxHeadersOverwrite = 2
        let bhRepo = self.makeBhRepo()
        defer {
            self.repoClose(bhRepo)
        }
        
        let downloader = Node.BlockHeaderDownload(count: 100,
                                                  locator: locator,
                                                  backlog: backlog,
                                                  maxHeadersOverwrite: maxHeadersOverwrite,
                                                  bhRepo: bhRepo,
                                                  muxer: self.muxer,
                                                  nodeDelegate: self.nodeDelegate,
                                                  threadPool: self.threadPool,
                                                  eventLoop: self.embedded)
        let future = downloader.start()
        downloader.cancel()
        let firstGet = self.nodeReadPending()
        XCTAssert(firstGet.bitcoinCommandValue is GetheadersCommand)
        self.nodeWritePendingCommand(HeadersCommand(headers: [ downloadHeaders.popFirst()! ]))
        Thread.sleep(forTimeInterval: 0.1)
        XCTAssertEqual(self.nodeDelegate.newTipHeight, 20)
        XCTAssertThrowsError(try future.wait()) { error in
            XCTAssert(error is EventLoopQueueingCancel)
        }
    }
    
    func testBlockHeaderDownloadSynched() {
        let (fakeHeaders, _, _) = Self.dbWriteTest20(eventLoop: self.elg.next())
        var downloadHeaders = Self.nextHeaders(count: 10, last: fakeHeaders.last!).wire[...]
        let locator = self.latestBlockLocator()
        let backlog = Array(fakeHeaders.suffix(3))
        let maxHeadersOverwrite = 2
        let bhRepo = self.makeBhRepo()
        defer {
            self.repoClose(bhRepo)
        }
        
        let downloader = Node.BlockHeaderDownload(count: 3,
                                                  locator: locator,
                                                  backlog: backlog,
                                                  maxHeadersOverwrite: maxHeadersOverwrite,
                                                  bhRepo: bhRepo,
                                                  muxer: self.muxer,
                                                  nodeDelegate: self.nodeDelegate,
                                                  threadPool: self.threadPool,
                                                  eventLoop: self.embedded)
        
        let future = downloader.start()
        let firstGet = self.nodeReadPending()
        XCTAssert(firstGet.bitcoinCommandValue is GetheadersCommand)
        self.nodeWritePendingCommand(HeadersCommand(headers: [ downloadHeaders.popFirst()! ]))
        Thread.sleep(forTimeInterval: 0.1)
        XCTAssertEqual(self.nodeDelegate.newTipHeight, 20)
        self.nodeDelegate.newTipHeight = nil
        
        let secondGet = self.nodeReadPending()
        XCTAssert(secondGet.bitcoinCommandValue is GetheadersCommand)
        self.nodeWritePendingCommand(HeadersCommand(headers: []))
        switch self.waitNow(future) {
        case .synch(let blockHeader, let key):
            XCTAssertEqual(self.nodeDelegate.newTipHeight, nil)
            XCTAssertEqual(key, .init(self.client))
            XCTAssertEqual(blockHeader.id, 20)
        default:
            XCTFail()
        }
    }
    
    func testBlockHeaderDownloadRollback() {
        let (fakeHeaders, _, _) = Self.dbWriteTest20(eventLoop: self.elg.next())
        let downloadHeaders = Self.nextHeaders(count: 10, last: fakeHeaders[fakeHeaders.endIndex - 3]).wire
        let locator = self.latestBlockLocator()
        let backlog = Array(fakeHeaders.suffix(3))
        let maxHeadersOverwrite = 2
        let bhRepo = self.makeBhRepo()
        defer {
            self.repoClose(bhRepo)
        }
        
        let downloader = Node.BlockHeaderDownload(count: 3,
                                                  locator: locator,
                                                  backlog: backlog,
                                                  maxHeadersOverwrite: maxHeadersOverwrite,
                                                  bhRepo: bhRepo,
                                                  muxer: self.muxer,
                                                  nodeDelegate: self.nodeDelegate,
                                                  threadPool: self.threadPool,
                                                  eventLoop: self.embedded)
        let future = downloader.start()
        let firstGet = self.nodeReadPending()
        XCTAssert(firstGet.bitcoinCommandValue is GetheadersCommand)
        self.nodeWritePendingCommand(HeadersCommand(headers: downloadHeaders))
        Thread.sleep(forTimeInterval: 0.2)
        XCTAssertNoThrow(XCTAssertEqual(try bhRepo.count().wait(), 18))
        XCTAssertNoThrow(XCTAssertEqual(try future.wait(), .rollback(17)))
    }

    func testBlockHeaderDownloadRollbackIsSubsetError() {
        let (fakeHeaders, _, _) = Self.dbWriteTest20(eventLoop: self.elg.next())
        let downloadHeaders = zip(fakeHeaders.suffix(3), fakeHeaders.suffix(4).dropFirst())
            .map { lhs, rhs -> BlockHeader in
                var copy = lhs
                try! copy.addPreviousBlockHash(rhs.blockHash())
                try! copy.wire()
                return copy
            }
        let locator = self.latestBlockLocator()
        let backlog = Array(fakeHeaders.suffix(5))
        let maxHeadersOverwrite = 3
        let bhRepo = self.makeBhRepo()
        defer {
            self.repoClose(bhRepo)
        }

        for i in (0..<3) {
            let dropped: [BlockHeader] = downloadHeaders.dropLast(i)
            let downloader = Node.BlockHeaderDownload(count: 3,
                                                      locator: locator,
                                                      backlog: backlog,
                                                      maxHeadersOverwrite: maxHeadersOverwrite,
                                                      bhRepo: bhRepo,
                                                      muxer: self.muxer,
                                                      nodeDelegate: self.nodeDelegate,
                                                      threadPool: self.threadPool,
                                                      eventLoop: self.embedded)
            let future = downloader.start()
            let firstGet = self.nodeReadPending()
            XCTAssert(firstGet.bitcoinCommandValue is GetheadersCommand)
            self.nodeWritePendingCommand(HeadersCommand(headers: dropped))
            
            XCTAssertThrowsError(try future.wait()) { error in
                switch error {
                case Node.HeadersError.illegalRollbackIsSubset:
                    break
                default:
                    XCTFail()
                }
            }
            
            XCTAssertNoThrow(XCTAssertEqual(try bhRepo.count().wait(), 20))
        }
    }

     func testBlockHeaderDownloadFailChain() {
        let (fakeHeaders, _, _) = Self.dbWriteTest20(eventLoop: self.elg.next())
        let failHeaders = Self.nextHeaders(count: 10, last: fakeHeaders[fakeHeaders.startIndex]).wire
        let locator = self.latestBlockLocator()
        let backlog = Array(fakeHeaders.suffix(3))
        let maxHeadersOverwrite = 2
        let bhRepo = self.makeBhRepo()
        defer {
            self.repoClose(bhRepo)
        }

        let downloader = Node.BlockHeaderDownload(count: 3,
                                                  locator: locator,
                                                  backlog: backlog,
                                                  maxHeadersOverwrite: maxHeadersOverwrite,
                                                  bhRepo: bhRepo,
                                                  muxer: self.muxer,
                                                  nodeDelegate: self.nodeDelegate,
                                                  threadPool: self.threadPool,
                                                  eventLoop: self.embedded)
        let future = downloader.start()
        let firstGet = self.nodeReadPending()
        XCTAssert(firstGet.bitcoinCommandValue is GetheadersCommand)
        self.nodeWritePendingCommand(HeadersCommand(headers: failHeaders))
        
        XCTAssertThrowsError(try future.wait()) { error in
            switch error {
            case Node.HeadersError.couldNotConnectWithBlockHeaderBacklog:
                break
            default:
                XCTFail()
            }
        }
        
        XCTAssert(self.nodeDelegate.rollbackEvents.isEmpty)
        XCTAssertEqual(self.nodeDelegate.newTipHeight, nil)
    }
    
    // MARK: CFHeaderDownload
    func testCFHeaderDownload() {
        let (_, _, serial) = Self.dbWriteTest20(eventLoop: self.elg.next())
        let bhRepo = self.makeBhRepo()
        let cfRepo = self.makeCfRepo()
        defer {
            self.deferClose(bhRepo: bhRepo, cfHeaderRepo: cfRepo)
        }
        
        let sequenceGen = sequence(first: try! cfRepo.findWire(id: 14).wait()) {
            Self.nextHeader($0)
        }
        let sequence = Array(sequenceGen.dropFirst().prefix(5))
        
        let stops = (15..<20).map { try? bhRepo.find(id: $0).wait() }.compactMap { $0 }
        XCTAssertEqual(stops.count, 5)
        let downloader = Node.CFHeaderDownload(start: serial.last!,
                                               stops: stops,
                                               walletCreation: .recovered,
                                               cfHeaderRepo: cfRepo,
                                               muxer: self.muxer,
                                               nodeDelegate: self.nodeDelegate,
                                               threadPool: self.threadPool,
                                               eventLoop: self.embedded)
        let future = downloader.start()
        defer { downloader.cancel() }
        
        XCTAssertNoThrow(
            try (0..<5).forEach {
                XCTAssert($0 < 4 ? downloader.almostComplete == false : downloader.almostComplete)
                let firstGet = self.nodeReadPending()
                XCTAssert(firstGet.bitcoinCommandValue is CF.GetcfheadersCommand)
                self.nodeWritePendingCommand(
                    try CF.CfheadersCommand(filterType: .basic,
                                            stop: stops[$0].blockHash(),
                                            headers: [ sequence[$0] ])
                )
            }
        )
        Thread.sleep(forTimeInterval: 0.2)
        XCTAssertNoThrow(try future.wait())
    }
    
    func testCFHeaderDownloadChainFail() {
        let (_, _, serial) = Self.dbWriteTest20(eventLoop: self.elg.next())
        let bhRepo = self.makeBhRepo()
        let cfRepo = self.makeCfRepo()
        defer {
            self.deferClose(bhRepo: bhRepo, cfHeaderRepo: cfRepo)
        }
        
        let sequenceGen = sequence(first: try! cfRepo.findWire(id: 14).wait()) {
            Self.nextHeader($0)
        }
        let sequence = Array(sequenceGen.dropFirst().prefix(5).suffix(4))
        
        let stops = (15..<20).map { try? bhRepo.find(id: $0).wait() }.compactMap { $0 }
        XCTAssertEqual(stops.count, 5)
        let downloader = Node.CFHeaderDownload(start: serial.last!,
                                               stops: stops,
                                               walletCreation: .recovered,
                                               cfHeaderRepo: cfRepo,
                                               muxer: self.muxer,
                                               nodeDelegate: self.nodeDelegate,
                                               threadPool: self.threadPool,
                                               eventLoop: self.embedded)
        let future = downloader.start()
        defer { downloader.cancel() }
        
        let firstGet = self.nodeReadPending()
        XCTAssert(firstGet.bitcoinCommandValue is CF.GetcfheadersCommand)
        XCTAssertNoThrow(
            self.nodeWritePendingCommand(
                try CF.CfheadersCommand(filterType: .basic,
                                        stop: stops[0].blockHash(),
                                        headers: [ sequence[0] ])
            )
        )
        Thread.sleep(forTimeInterval: 0.2)
        var nodeDownloaderOutcome: NodeDownloaderOutcome!
        XCTAssertNoThrow(nodeDownloaderOutcome = try future.wait())
        
        switch nodeDownloaderOutcome {
        case .rollback(14):
            break
        default:
            XCTFail()
        }
    }
    
    // MARK: CompactFilterDownload
    func testCFDownload() {
        let (bh, wire, _) = Self.dbWriteTest20(eventLoop: self.elg.next())
        let bhRepo = self.makeBhRepo()
        let cfRepo = self.makeCfRepo()
        defer {
            self.deferClose(bhRepo: bhRepo, cfHeaderRepo: cfRepo)
        }
        
        var walletDelegateEvents = [ CompactFilterEvent.download(15), .download(16),
                       .download(18), .download(19),
                       .nonMatching(15), .nonMatching(16),
                       .nonMatching(18), .nonMatching(19) ][...]
        self.nodeDelegate.filterEventCallback = {
            XCTAssertEqual($0, walletDelegateEvents.popFirst())
        }
        
        let filterTuples = TestingHelpers.DB.makeEmptyFilters(previousHeaderHash: wire[14].headerHash, blockHeaders: bh[15..<20])
        XCTAssertNoThrow(
            try filterTuples.map(\.0).forEach {
                try cfRepo.write($0).wait()
            }
        )
        
        guard let fifteen = try? cfRepo.findWire(id: 15).wait(),
              let sixteen = try? cfRepo.findWire(id: 16).wait(),
              let eighteen = try? cfRepo.findWire(id: 18).wait(),
              let nineteen = try? cfRepo.findWire(id: 19).wait()
        else {
            XCTFail()
            return
        }
        let batch1: Node.CFBatch = [ (bh[15], fifteen), (bh[16], sixteen), ]
        let batch2: Node.CFBatch = [ (bh[18], eighteen),  (bh[19], nineteen), ]
        let batches: ArraySlice<Node.CFBatch> = [ batch1, batch2, ][...]
        
        let downloader = Node.CompactFilterDownload(batches: batches,
                                                    cfHeaderRepo: cfRepo,
                                                    muxer: self.muxer,
                                                    nodeDelegate: self.nodeDelegate,
                                                    threadPool: self.threadPool,
                                                    cfMatcher: self.cfMatcher,
                                                    pubKeys: { self.embedded.makeSucceededFuture([]) },
                                                    eventLoop: self.embedded,
                                                    maxConcurrentInvocations: 2,
                                                    cfTimeout: .seconds(60))
        let future = downloader.start()
        defer { downloader.cancel() }

        XCTAssertEqual(self.client.queueDepth, 2)
        let firstGet = self.nodeReadPending()
        XCTAssert(firstGet.bitcoinCommandValue is CFBundleCommand)
        self.nodeWritePendingCommand(filterTuples.map(\.1)[0])
        self.nodeWritePendingCommand(filterTuples.map(\.1)[1])
        Thread.sleep(forTimeInterval: 0.2)

        XCTAssertEqual(self.client.queueDepth, 1)
        let secondGet = self.nodeReadPending()
        XCTAssert(secondGet.bitcoinCommandValue is CFBundleCommand)
        self.nodeWritePendingCommand(filterTuples.map(\.1)[3])
        self.nodeWritePendingCommand(filterTuples.map(\.1)[4])
        Thread.sleep(forTimeInterval: 0.2)

        XCTAssertNoThrow(try future.wait())
        XCTAssertNoThrow(
            try [15, 16, 18, 19, ].forEach {
                XCTAssert(try cfRepo.find(id: $0).wait().recordFlag.contains([.nonMatchingFilter, .processed]))
            }
        )
        XCTAssert(walletDelegateEvents.isEmpty)
    }
    
    func testCFDownloadFail() {
        let (bh, wire, _) = Self.dbWriteTest20(eventLoop: self.elg.next())
        let bhRepo = self.makeBhRepo()
        let cfRepo = self.makeCfRepo()
        defer {
            self.deferClose(bhRepo: bhRepo, cfHeaderRepo: cfRepo)
        }
        
        var walletDelegateEvents = [ CompactFilterEvent.download(15), .download(16),
                                     .download(18), .download(19),
                                     .failure(15), .failure(16),
                                     .failure(18), .failure(19) ][...]
        self.nodeDelegate.filterEventCallback = {
            XCTAssertEqual($0, walletDelegateEvents.popFirst())
        }
        
        let filterTuples = TestingHelpers.DB.makeEmptyFilters(previousHeaderHash: wire[14].headerHash, blockHeaders: bh[15..<20])
        XCTAssertNoThrow(
            try filterTuples.map(\.0).forEach {
                try cfRepo.write($0).wait()
            }
        )
        
        guard let fifteen = try? cfRepo.findWire(id: 15).wait(),
              let sixteen = try? cfRepo.findWire(id: 16).wait(),
              let eighteen = try? cfRepo.findWire(id: 18).wait(),
              let fail = try? cfRepo.findWire(id: 9).wait()
        else {
            XCTFail()
            return
        }
        let rewriteFifteen: CF.Header = .init(filterHash: fifteen.filterHash, previousHeaderHash: .zero, headerHash: .zero)
        
        let batch1: Node.CFBatch = [ (bh[15], rewriteFifteen), (bh[16], sixteen), ]
        let batch2: Node.CFBatch = [ (bh[18], eighteen),  (bh[19], fail), ]
        let batches: ArraySlice<Node.CFBatch> = [ batch1, batch2, ][...]
        
        let downloader = Node.CompactFilterDownload(batches: batches,
                                                    cfHeaderRepo: cfRepo,
                                                    muxer: self.muxer,
                                                    nodeDelegate: self.nodeDelegate,
                                                    threadPool: self.threadPool,
                                                    cfMatcher: self.cfMatcher,
                                                    pubKeys: { self.embedded.makeSucceededFuture([]) },
                                                    eventLoop: self.embedded,
                                                    maxConcurrentInvocations: 2,
                                                    cfTimeout: .seconds(60))
        let future = downloader.start()
        defer { downloader.cancel() }

        XCTAssertEqual(self.client.queueDepth, 2)
        let firstGet = self.nodeReadPending()
        XCTAssert(firstGet.bitcoinCommandValue is CFBundleCommand)
        self.nodeWritePendingCommand(filterTuples.map(\.1)[0])
        self.nodeWritePendingCommand(filterTuples.map(\.1)[1])
        Thread.sleep(forTimeInterval: 0.2)

        XCTAssertEqual(self.client.queueDepth, 1)
        let secondGet = self.nodeReadPending()
        XCTAssert(secondGet.bitcoinCommandValue is CFBundleCommand)
        self.nodeWritePendingCommand(filterTuples.map(\.1)[3])
        self.nodeWritePendingCommand(filterTuples.map(\.1)[4])
        Thread.sleep(forTimeInterval: 0.2)

        XCTAssertThrowsError(try future.wait()) {
            switch $0 {
            case let error as Node.CompactFilterDownload.CompoundError:
                switch (error.errors[0], error.errors[1]) {
                case (CF.CfilterCommand.CFError.validationFailureHeaderHash, CF.CfilterCommand.CFError.validationFailureFilterHash):
                    break
                default:
                    XCTFail()
                }
            default:
                XCTFail()
            }
        }
        XCTAssert(walletDelegateEvents.isEmpty)
    }
    
    func testCFDownloadDropOutcome() {
        let (bh, wire, _) = Self.dbWriteTest20(eventLoop: self.elg.next())
        let bhRepo = self.makeBhRepo()
        let cfRepo = self.makeCfRepo()
        defer {
            self.deferClose(bhRepo: bhRepo, cfHeaderRepo: cfRepo)
        }
        
        let filterTuples = TestingHelpers.DB.makeEmptyFilters(previousHeaderHash: wire[14].headerHash, blockHeaders: bh[15..<20])
        XCTAssertNoThrow(
            try filterTuples.map(\.0).forEach {
                try cfRepo.write($0).wait()
            }
        )
        
        guard let fifteen = try? cfRepo.findWire(id: 15).wait(),
              let sixteen = try? cfRepo.findWire(id: 16).wait(),
              let eighteen = try? cfRepo.findWire(id: 18).wait(),
              let nineteen = try? cfRepo.findWire(id: 19).wait()
        else {
            XCTFail()
            return
        }
        let batch1: Node.CFBatch = [ (bh[15], fifteen), (bh[16], sixteen), ]
        let batch2: Node.CFBatch = [ (bh[18], eighteen),  (bh[19], nineteen), ]
        let batches: ArraySlice<Node.CFBatch> = [ batch1, batch2, ][...]
        
        let downloader = Node.CompactFilterDownload(batches: batches,
                                                    cfHeaderRepo: cfRepo,
                                                    muxer: self.muxer,
                                                    nodeDelegate: self.nodeDelegate,
                                                    threadPool: self.threadPool,
                                                    cfMatcher: self.cfMatcher,
                                                    pubKeys: { self.embedded.makeSucceededFuture([]) },
                                                    eventLoop: self.embedded,
                                                    maxConcurrentInvocations: 2,
                                                    cfTimeout: .seconds(60))
        let closeFuture = self.client.closeFuture
        self.client.close()
        self.waitNow(closeFuture)
        
        let future = downloader.start()
        defer { downloader.cancel() }
        
        XCTAssertNoThrow(XCTAssertEqual(try future.wait(), .drop))
    }
    
    // MARK: FullBlockDownload
    struct FullBlockTestData {
        let opcodes: [UInt8]
        let outpoint: Tx.Outpoint
        let data: [(blockHeader: BlockHeader,
                    cfHeader: CF.SerialHeader,
                    fullBlock: BlockCommand,
                    tx: Tx.AnyIdentifiableTransaction)]
    }
    
    func setupFullBlockDownload() -> FullBlockTestData {
        let outpoint: Tx.Outpoint = {
            let txId: Tx.TxId = .little((0..<32).map { UInt8($0) })
            return .init(transactionId: txId, index: 0)
        }()
        let (header0, cfHeader0, fullBlock0, tx0) = BlockCommand.make(height: 0, outpoint: outpoint)

        let pubKey = DSA.PublicKey(.G)
        let opCodes = PublicKeyHash(pubKey).scriptPubKeyWPKH
        let (header1, cfHeader1, fullBlock1, tx1) = BlockCommand.make(height: 1,
                                                                      prevBlockHash: try! header0.blockHash(),
                                                                      opCodes: opCodes)

        let (header2, cfHeader2, fullBlock2, tx2) = BlockCommand.make(height: 2,
                                                                      prevBlockHash: try! header1.blockHash())

        return FullBlockTestData(opcodes: opCodes,
                                 outpoint: outpoint,
                                 data: [ (header0, cfHeader0, fullBlock0, tx0),
                                         (header1, cfHeader1, fullBlock1, tx1),
                                         (header2, cfHeader2, fullBlock2, tx2), ])
    }
    
    func testFullBlockDownload() {
        let bhRepo = self.makeBhRepo()
        let cfRepo = self.makeCfRepo()
        defer {
            self.deferClose(bhRepo: bhRepo, cfHeaderRepo: cfRepo)
        }

        let setup = setupFullBlockDownload()
        for d in setup.data {
            XCTAssertNoThrow(try bhRepo.write(d.blockHeader).wait())
            XCTAssertNoThrow(try cfRepo.write(d.cfHeader).wait())
        }
        
        let spent = SpentOutpoint(outpoint: setup.outpoint,
                                  outputs: [ .outgoing(10_000, []) ],
                                  tx: .init(setup.data[0].tx))
        let funding = FundingOutpoint(outpoint: .init(transactionId: setup.data[1].tx.txId, index: 0),
                                      amount: 10_000,
                                      scriptPubKey: ScriptPubKey(tag: 0, index: 1, opcodes: setup.opcodes))
        var expected = [ TransactionEvent.spent(spent),
                         .new(funding), ][...]
        var compareHeight = 0
        self.nodeDelegate.transactionCallback = { height, events in
            print("EVENTS ARE height \(height) #\(events.count)", events)

            defer { compareHeight += 1 }
            XCTAssertEqual(compareHeight, height)
            XCTAssertEqual(events.first, expected.popFirst())
            return .relaxed
        }
        
        var filterEventsExpected = [ CompactFilterEvent.blockDownload(0),
                                     .blockDownload(1),
                                     .blockMatching(0),
                                     .blockDownload(2),
                                     .blockMatching(1),
                                     .blockNonMatching(2), ][...]
        self.nodeDelegate.filterEventCallback = { event in
            XCTAssertEqual(event, filterEventsExpected.popFirst())
        }
        
        let downloader = Node.FullBlockDownload
            .init([ setup.data[0].blockHeader,
                    setup.data[1].blockHeader,
                    setup.data[2].blockHeader ],
                  outpoints: { self.embedded.makeSucceededFuture([ setup.outpoint ]) },
                  fullBlockCommandTimeout: .seconds(60),
                  pubKeys: { self.embedded.makeSucceededFuture(
                    Set([ ScriptPubKey(tag: 0, index: 1, opcodes: setup.opcodes), ])
                  )},
                  cfHeaderRepo: cfRepo,
                  muxer: self.muxer,
                  nodeDelegate: self.nodeDelegate,
                  threadPool: self.threadPool,
                  eventLoop: self.embedded)
//        let downloader = Node.FullBlockDownload([ setup.data[0].blockHeader,
//                                                  setup.data[1].blockHeader,
//                                                  setup.data[2].blockHeader ],
//                                                outpoints: { [ setup.outpoint ] },
//                                                pubKeys: { .init([ ScriptPubKey(tag: 0, opcodes: setup.opcodes), ]) },
//                                                cfHeaderRepo: cfRepo,
//                                                muxer: self.muxer,
//                                                nodeDelegate: self.nodeDelegate,
//                                                threadPool: self.threadPool,
//                                                eventLoop: self.embedded)
        let future = downloader.start()
        defer { downloader.cancel() }
        
        let firstGet = self.nodeReadPending()
        XCTAssert(firstGet.bitcoinCommandValue is GetdataCommand)
        self.nodeWritePendingCommand(setup.data[0].fullBlock)
        Thread.sleep(forTimeInterval: 0.1)

        let secondGet = self.nodeReadPending()
        XCTAssert(secondGet.bitcoinCommandValue is GetdataCommand)
        self.nodeWritePendingCommand(setup.data[1].fullBlock)
        Thread.sleep(forTimeInterval: 0.1)

        let thirdGet = self.nodeReadPending()
        XCTAssert(thirdGet.bitcoinCommandValue is GetdataCommand)
        self.nodeWritePendingCommand(setup.data[2].fullBlock)
        Thread.sleep(forTimeInterval: 0.1)
        
        XCTAssertNoThrow(try future.wait())
        XCTAssert(filterEventsExpected.isEmpty)
    }
    
    func testFullBlockDownloadFail() {
        let (bh, _, _) = Self.dbWriteTest20(eventLoop: self.elg.next())
        let bhRepo = self.makeBhRepo()
        let cfRepo = self.makeCfRepo()
        defer {
            self.deferClose(bhRepo: bhRepo, cfHeaderRepo: cfRepo)
        }

        let (blockHeader19, cfHeader19, fullBlock19, _) = BlockCommand.make(height: 19)

        // make BlockCommand unverified by rewriting
        var buffer = ByteBufferAllocator().buffer(capacity: 1000)
        fullBlock19.write(to: &buffer)
        let wireBlock = BlockCommand(fromBuffer: &buffer)!

        XCTAssertNoThrow(try bhRepo.write(blockHeader19).wait())
        XCTAssertNoThrow(try cfRepo.write(cfHeader19).wait())

        var filterEventsExpected = [ CompactFilterEvent.blockDownload(19),
                                     .blockFailed(19), ][...]
        self.nodeDelegate.filterEventCallback = { event in
            XCTAssertEqual(event, filterEventsExpected.popFirst())
        }

        let downloader = Node.FullBlockDownload
            .init([ bh[19], ],
                  outpoints: { self.embedded.makeSucceededFuture([]) },
                  fullBlockCommandTimeout: .seconds(60),
                  pubKeys: { self.embedded.makeSucceededFuture([]) },
                  cfHeaderRepo: cfRepo,
                  muxer: self.muxer,
                  nodeDelegate: self.nodeDelegate,
                  threadPool: self.threadPool,
                  eventLoop: self.embedded)
//        let downloader = Node.FullBlockDownload([ bh[19], ],
//                                                outpoints: { [] },
//                                                pubKeys: { [] },
//                                                cfHeaderRepo: cfRepo,
//                                                muxer: self.muxer,
//                                                nodeDelegate: self.nodeDelegate,
//                                                threadPool: self.threadPool,
//                                                eventLoop: self.embedded)
        let future = downloader.start()
        defer { downloader.cancel() }

        let firstGet = self.nodeReadPending()
        XCTAssert(firstGet.bitcoinCommandValue is GetdataCommand)
        self.nodeWritePendingCommand(wireBlock)
        Thread.sleep(forTimeInterval: 0.4)

        XCTAssertThrowsError(try future.wait()) {
            switch $0 {
            case BlockCommand.BlockError.blockHeaderMismatch:
                break
            default:
                XCTFail("\($0)")
            }
        }
        XCTAssert(filterEventsExpected.isEmpty)
    }
    
    func testFullBlockStrictOutcomeRollback() {
        let bhRepo = self.makeBhRepo()
        let cfRepo = self.makeCfRepo()
        defer {
            self.deferClose(bhRepo: bhRepo, cfHeaderRepo: cfRepo)
        }

        let setup = setupFullBlockDownload()
        for d in setup.data {
            XCTAssertNoThrow(try bhRepo.write(d.blockHeader).wait())
            XCTAssertNoThrow(try cfRepo.write(d.cfHeader).wait())
        }

        var outcomes = [ TransactionEventCommitOutcome.relaxed, .strict ][...]
        self.nodeDelegate.transactionCallback = { _, _ in
            return outcomes.popFirst() ?? { XCTFail(); return .relaxed }()
        }
        let downloader = Node.FullBlockDownload
            .init([ setup.data[0].blockHeader,
                    setup.data[1].blockHeader,
                    setup.data[2].blockHeader ],
                  outpoints: { self.embedded.makeSucceededFuture(
                    [ setup.outpoint ]
                  )},
                  fullBlockCommandTimeout: .seconds(60),
                  pubKeys: { self.embedded.makeSucceededFuture(
                    Set([ ScriptPubKey(tag: 0, index: 1, opcodes: setup.opcodes), ])
                  )},
                  cfHeaderRepo: cfRepo,
                  muxer: self.muxer,
                  nodeDelegate: self.nodeDelegate,
                  threadPool: self.threadPool,
                  eventLoop: self.embedded)
        let future = downloader.start()
    
        let firstGet = self.nodeReadPending()
        XCTAssert(firstGet.bitcoinCommandValue is GetdataCommand)
        self.nodeWritePendingCommand(setup.data[0].fullBlock)
        Thread.sleep(forTimeInterval: 0.1)

        let secondGet = self.nodeReadPending()
        XCTAssert(secondGet.bitcoinCommandValue is GetdataCommand)
        self.nodeWritePendingCommand(setup.data[1].fullBlock)

        XCTAssertNoThrow(
            XCTAssertEqual(try future.wait(), .rollback(2))
        )

        downloader.cancel()
    }
    
    func testFullBlockFail() {
        let bhRepo = self.makeBhRepo()
        let cfRepo = self.makeCfRepo()
        defer {
            self.deferClose(bhRepo: bhRepo, cfHeaderRepo: cfRepo)
        }

        let setup = setupFullBlockDownload()
        for d in setup.data {
            XCTAssertNoThrow(try bhRepo.write(d.blockHeader).wait())
            XCTAssertNoThrow(try cfRepo.write(d.cfHeader).wait())
        }

        var outcomes = [ TransactionEventCommitOutcome.relaxed, .strict ][...]
        self.nodeDelegate.transactionCallback = { heigh, events in
            return outcomes.popFirst() ?? { XCTFail(); return .relaxed }()
        }
        let downloader = Node.FullBlockDownload
            .init([ setup.data[0].blockHeader,
                    setup.data[1].blockHeader,
                    setup.data[2].blockHeader ],
                  outpoints: { self.embedded.makeSucceededFuture(
                    [ setup.outpoint ]
                  )},
                  fullBlockCommandTimeout: .seconds(60),
                  pubKeys: { self.embedded.makeSucceededFuture(
                    Set([ ScriptPubKey(tag: 0, index: 1, opcodes: setup.opcodes), ])
                  )},
                  cfHeaderRepo: cfRepo,
                  muxer: self.muxer,
                  nodeDelegate: self.nodeDelegate,
                  threadPool: self.threadPool,
                  eventLoop: self.embedded)
        
        let future = downloader.start()
        let closeFuture = self.client.closeFuture
        self.client.close()
        XCTAssertThrowsError(try future.wait()) { error in
            guard let error = error as? ChannelError
            else { XCTFail(); return }
            
            switch error {
            case .outputClosed: break
            default: XCTFail()
            }
        }
        downloader.cancel()
        
        self.waitNow(closeFuture)
    }
    
    // MARK: VerifyDownload
    func setupVerifyDownload() -> (bh: BlockHeader, cfHeader: CF.Header, filter: CF.CfilterCommand, block: BlockCommand) {
        // Testnet 2,150,656
        var headerBuffer = ByteBuffer(bytes: "04e0ff3f95ddcc1eb8a6e39b6c64d9f8d4c6c6458dca3423108e5afc476b00000000000069778fa6da8c97772704fa1c844453b4a3989da980d662ca41d966e736bfefd55eca0b62c9ca001c2fa1faa6".hex2Bytes)
        var header = BlockHeader(fromBuffer: &headerBuffer)!
        try! header.addBlockHash()
        try! header.addHeight(2_150_656)
        var cfHeader_2150656 = ByteBuffer(bytes: "8b16566ff886111b77c8ef5d69b357e212b0b3b2235097333584a29daef64053".hex2Bytes)
        let cfHeader = CF.Header(buffer: &cfHeader_2150656,
                                 previousHeaderHash: .makeHash(from: "26190367963f31d0ba12517ffb311900143e071a0ecd702a0ce2afe8fe15abeb".hex2Bytes))!
        var filter_2150656 = ByteBuffer(bytes: "0e1086e00ffaaaefa5d50444d3d08ccec3a8d383c063354de66101af82186a4c3ba7dfcb0008".hex2Bytes)
        // n = 14
        let n = filter_2150656.readVarInt()!
        let filter = CF.CfilterCommand(.wire(filterType: .basic,
                                             blockHash: try! header.blockHash(),
                                             filterHash: cfHeader.filterHash,
                                             n: Int(n),
                                             compressed: Array(filter_2150656.readableBytesView)))
        
        let blockBytes = """
04e0ff3f95ddcc1eb8a6e39b6c64d9f8d4c6c6458dca3423108e5afc476b00000000000069778fa6da8c97772704fa1c844453b4a3989da980d662ca41d966e736bfefd55eca0b62c9ca001c2fa1faa606020000000001010000000000000000000000000000000000000000000000000000000000000000ffffffff480300d120045eca0b62425443506f6f6cfabe6d6d0ff509648391e5230824d211f486794288014936a8bc39ccd4869897beb862cd040000000d6531d50203b472596e010000000000ffffffff029ad64a000000000017a9147bef0b4a4dafa77b2ec52b81659cbcf0d9a91487870000000000000000266a24aa21a9ed93de14ff4ec531b393ee21bfb8b668101f622f63dc3d8d6f70d9e8bee4480c41012000000000000000000000000000000000000000000000000000000000000000000000000001000000010e331810512ea19ec08a7f1d086b079610b3e184d43d4d3c20b3c8c804846333030000006a473044022022935b1ac1fa7831ef4b0629c1b5877d1af1980edb9e74abe65013445b551cc2022066a48faabc172ab70ffe997a008c1db75eba920a61128e93366a2c2e7c05bc190121037435c194e9b01b3d7f7a2802d6684a3af68d05bbf4ec8f17021980d777691f1dfdffffff047c15000000000000536a4c5054325b9bb76a71a53822fcb7734339bb9234860279fc668907d82280e29fba349dea0458f4b79a1604b57148525a65558ee522c6dd4ac6232be61af2782ed4831db6430020d0ff00010020a7640001282a220000000000001976a914000000000000000000000000000000000000000088ac2a220000000000001976a914000000000000000000000000000000000000000088aca6fa091a000000001976a914ba27f99e007c7f605a8305e318c1abde3cd220ac88ac000000000100000001eee9c2292ef878fa565b7969be62e22364ad70ba686707eccdd1581608ffcc2e000000006a473044022033162f550cf34f71317198242681e320e5a1f34ff1972e95ca08720e48facc0c02203d4c8ce268bc10bc6ead1fd276463fa88b89426ee50c3f5500e09e853cf3340d0121024ca31ba52114b65e80c2106866a7084c46019ee4150540da68036823ad6b4a3bffffffff0222062d04000000001976a91403862adfc247e34b5c379006423e840fafe5056b88acba7d120100000000160014e58f0b72a5e4948f08be8baacbca5e9c75635516000000000100000000010178f6676fe72ba031cce860751bbe4e5b7b45ec5fb9947e6c604e238b08738e0b0100000000ffffffff02c800000000000000160014a78845ec49dbd849f20f9555c6402a3fa4f5d1d9ba08dd050000000016001498398f7087b16dff96e8b44f0854256f0054d1ba02473044022009a524a19648223c1e810c38810211c1ea01690cb2014b858238ab57fe6d006902205c31ea1bb1653067eadc48b27cc47a8ae96bcebcacee48212d6faa706707722101210345c0569e560cb34909e3315a56750851f3da542a0f419687c2c277496b30c6c50000000002000000000101e55d86d458f59319f76201dfb424d816159db665cead5545b34ec2472f606ba90200000000ffffffff02e803000000000000160014e9b3a26b50b476594f2c9845aa1827eeccb2cbe71909000000000000160014fb1593c54291e9b66c99cca14b6be6196d1b15b20248304502210092ad37dbc6752cefe14c7ac0b4fc89bc260f2e687c30ccd4a491f6d588838ce20220457fd4587c5b0d330d71792e1d37c748b96608f333fc5a40d46f0ec8c3260fd1012103b3abfc33c210776da439efa85a4be5d2e220d08e63d1bd3519ccb12df489de060000000001000000000102bb7e94d9c0b4a0b033f78c66ee3f31a68f1c7ddb262862784cfd9d22e19d465c0000000000ffffffff09dea06076093fd2034dda7175964c278e52d5a6750be5d68744442fa00033490000000000ffffffff026c400000000000001600140cf326176a970c5629994dcfe7f4308a3f52cceb6c400000000000001600146daefdd0b7b1cbe93262f4030fac99b886671f2402473044022064ebd912fcb8bdee7dd97ec0afe8641a11db9a021439b1240ea1d2ba497776dc02206b1237331c88d014aacd08f1ba6378b05adb4c5aa5cdbe269b5134b016a3faac012103aacdbb86cdb28408e58499a52cb5094924272c4d812b30478bdf40da53d2756d0247304402200f811c2f484cf7cc45ede58baae2e6ca8b26bd348ba314457428b23b80a2a745022016ebf4799f0c52993ff833b2624f485aca8d3cd08309ef250c73a1154eb9338a012102e4634321feca98206585c67db96d733b88200e4ebe222fd945b3178aef199d4300000000
""".hex2Bytes
        var blockByteBuffer = ByteBuffer(bytes: blockBytes)
        var block = BlockCommand(fromBuffer: &blockByteBuffer)!
        try! block.verify(header: header)

        return (header, cfHeader, filter, block)
    }
    
    func verifyDownload(minimumFilters: Int) throws {
        let data = setupVerifyDownload()
        
        let bhRepo = self.makeBhRepo()
        let cfRepo = self.makeCfRepo()
        defer {
            self.deferClose(bhRepo: bhRepo, cfHeaderRepo: cfRepo)
        }
        
        let verifyDownload = Node.VerifyDownload(headers: [ (data.bh, data.cfHeader), ],
                                                 muxer: self.muxer,
                                                 threadPool: self.threadPool,
                                                 eventLoop: self.embedded,
                                                 fullBlockTimeout: .seconds(5),
                                                 cfTimeout: .seconds(5),
                                                 minimumFilters: minimumFilters,
                                                 nodeProperties: Settings.NODE_PROPERTIES)
        let startFuture = verifyDownload.start()
        
        let firstGet = self.nodeReadPending()
        XCTAssert(firstGet.bitcoinCommandValue is GetdataCommand)
        self.nodeWritePendingCommand(data.block)
        Thread.sleep(forTimeInterval: 0.1)

        let secondGet = self.nodeReadPending()
        XCTAssert(secondGet.bitcoinCommandValue is CF.GetcfiltersCommand)
        self.nodeWritePendingCommand(data.filter)
        Thread.sleep(forTimeInterval: 0.1)

        verifyDownload.cancel()
        let finished = try startFuture.wait()
        XCTAssertEqual(finished, .processed)
    }
    
    func testVerifyDownloadNLimit() {
        for n in [ 0, 14, ] {
            XCTAssertNoThrow(try self.verifyDownload(minimumFilters: n))
        }
    }
    
    func testVerifyDownloadOverLimit() {
        XCTAssertThrowsError(try self.verifyDownload(minimumFilters: 15)) { error in
            switch error {
            case Node.VerifyDownload.Error.skipNotEnoughScriptPubKeys:
                break
            default:
                XCTFail()
            }
        }
    }
}
