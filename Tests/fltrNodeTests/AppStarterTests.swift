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
import fltrECC
import fltrTx
import Foundation
import LoadNode
import NIO
import NIOTransportServices
import NodeTestLibrary
import XCTest

final class AppStarterTests: XCTestCase {
    var eventLoop: EventLoop!
    var fileHandle: NIOFileHandle!
    var niots: NIOTSEventLoopGroup!
    var fileIO: NonBlockingFileIOClient!
    var threadPool: NIOThreadPool!
    var userDefaults: UserDefaults!
    
    var savedSettings: NodeSettings!
    
    override func setUp() {
        self.savedSettings = Settings
        TestingHelpers.diSetup()
        
        self.userDefaults = UserDefaults.standard
        
        guard let fileHandle = try? NIOFileHandle(path: Settings.BITCOIN_BLOCKHEADER_FILEPATH,
                                                  mode: [ .read, ],
                                                  flags: .allowFileCreation(posixMode: 0o600))
        else {
            XCTFail()
            return
        }
        self.fileHandle = fileHandle
        
        self.threadPool = NIOThreadPool(numberOfThreads: 1)
        self.threadPool.start()
        
        self.niots = NIOTSEventLoopGroup(loopCount: 1)
        self.eventLoop = self.niots.next()
        
        self.fileIO = NonBlockingFileIOClient.live(self.threadPool)
    }
    
    override func tearDown() {
        XCTAssertNoThrow(try self.fileIO.close(fileHandle: self.fileHandle,
                                               eventLoop: self.eventLoop).wait())
        self.eventLoop = nil
        self.fileIO = nil
        self.fileHandle = nil

        XCTAssertNoThrow(try self.threadPool.syncShutdownGracefully())
        self.threadPool = nil
        
        XCTAssertNoThrow(try self.niots.syncShutdownGracefully())
        self.niots = nil
        
        self.userDefaults = nil
        
        Settings = self.savedSettings
        self.savedSettings = nil
    }
    
    func testPrepare() {
        XCTAssertNoThrow(try AppStarter.prepare(new: false, year: Load.years().last!))
        let offset = fltrNode.Settings.NODE_PROPERTIES.offset
        XCTAssertGreaterThan(offset, 0)

        XCTAssert(!fltrNode.Settings.NODE_PROPERTIES.peers.isEmpty)

        Thread.sleep(forTimeInterval: 0.1)
        
        XCTAssertNoThrow(
            try TestingHelpers.DB.withFileBlockHeaderRepo(fileHandle: self.fileHandle,
                                                          nonBlockingFileIO: self.fileIO,
                                                          eventLoop: self.eventLoop,
                                                          offset: offset) { repo in
                XCTAssertThrowsError(try repo.find(from: 0).wait())
                _ = try repo.find(id: offset).wait()
            }
        )
        
        XCTAssertNoThrow(
            try TestingHelpers.DB.withFileCFHeaderRepo(fileHandle: self.fileHandle,
                                                       nonBlockingFileIO: self.fileIO,
                                                       eventLoop: self.eventLoop,
                                                       offset: offset) { repo in
                _ = try repo.find(id: offset).wait()
                _ = try repo.find(id: offset + 1).wait()
                XCTAssertThrowsError(try repo.findWire(id: offset).wait())
                _ = try repo.findWire(id: offset + 1).wait()
            }
        )
    }
    
    func testStartStopNode() {
        XCTAssertNoThrow(try AppStarter.prepare(new: false, year: Load.years().last!))
        
        struct Delegate: NodeDelegate {
            func syncEvent(_: SyncEvent) {
            }
            func newTip(height: Int, commit: () -> Void) {
                commit()
            }
            func rollback(height: Int, commit: () -> Void) {
                commit()
            }
            func transactions(height: Int,
                              events: [TransactionEvent],
                              commit: @escaping (TransactionEventCommitOutcome) -> Void) {
                commit(.relaxed)
            }
            func unconfirmedTx(height: Int, events: [TransactionEvent]) {
            }
            func estimatedHeight(_: ConsensusChainHeight.CurrentHeight) {
            }
            func filterEvent(_: CompactFilterEvent) {
            }
        }
        
        let node = AppStarter.createNode(threadPool: self.threadPool,
                                         walletDelegate: Delegate())
        
        let g = PublicKeyHash(DSA.PublicKey(Point.G))
        let scriptPubKey = ScriptPubKey(tag: 0, index: 1, opcodes: g.scriptPubKeyWPKH)
        let scriptPubKey2 = ScriptPubKey(tag: 1, index: 1, opcodes: g.scriptPubKeyLegacyWPKH)
        
        XCTAssertNoThrow(try node.addScriptPubKeys([scriptPubKey, scriptPubKey2, ]).wait())
        
        XCTAssertNoThrow(try node.start().wait())
        XCTAssertNoThrow(try node.stop().wait())
    }
}

