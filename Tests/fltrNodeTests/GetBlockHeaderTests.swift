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
import XCTest
import NodeTestLibrary
import NIO
import NIOTransportServices

final class GetBlockHeaderTests: XCTestCase {
    var blockChain: [BlockHeader]! = nil
    var fileRepo: FileBlockHeaderRepo! = nil
    var testServer: TestServer! = nil
    var localhostPeer: PeerAddress! = nil
    var elg: NIOTSEventLoopGroup!
    var threadPool: NIOThreadPool! = nil
    var savedSettings: NodeSettings!
    
    let rowsToWrite = 1000
    
    override func setUp() {
        self.savedSettings = Settings
        TestingHelpers.diSetup {
            $0.BITCOIN_NUMBER_OF_CLIENTS = 1
        }

        self.threadPool = NIOThreadPool(numberOfThreads: 2)
        self.threadPool?.start()
        
        self.elg = .init(loopCount: 1)
        
        self.fileRepo = TestingHelpers.DB.getFileBlockHeaderRepo(offset: 0,
                                                                 threadPool: self.threadPool,
                                                                 eventLoop: self.elg.next())
        
        self.testServer = TestingHelpers.Server.setupServer()
        self.localhostPeer = PeerAddress(time: UInt32(0),
                                         services: [ .nodeNetwork, .nodeWitness ],
                                         address: .string("::ffff:7f00:1"),
                                         port: PeerAddress.IPPort(self.testServer.serverPort))

        
        XCTAssertNoThrow(
            self.blockChain = try BlockHeaderMock.TestChain.createInvalid(length: 3000)
        )
    }
    
    override func tearDown() {
        XCTAssertNoThrow(
            try self.fileRepo.close().wait()
        )
        XCTAssertNoThrow(
            try self.threadPool.syncShutdownGracefully()
        )
        XCTAssertNoThrow(
            try FileManager.default
            .removeItem(atPath: Settings.BITCOIN_BLOCKHEADER_FILEPATH)
        )
        XCTAssertNoThrow(try self.elg.syncShutdownGracefully())
        self.elg = nil
        
        Settings = self.savedSettings
        self.savedSettings = nil
    }

    private func writeSomeBlockHeaderData() {
        var writeData: [BlockHeader]!
        XCTAssertNoThrow(
            writeData = try self.blockChain.prefix(self.rowsToWrite).map {
                var copy = $0
                try copy.serial()
                return copy
            }
        )
        XCTAssertNoThrow(
            try writeData.forEach {
                try self.fileRepo.write($0).wait()
            }
        )
        
        XCTAssertNoThrow(
            XCTAssertEqual(try self.fileRepo.count().wait(), self.rowsToWrite)
        )
        
        self.blockChain.removeFirst(self.rowsToWrite)
    }

    func testGetHeadersSingle() {
        // MARK: maybe use single client here
        let client = Client.connect(eventLoop: self.elg.next(),
                                    host: self.localhostPeer.address,
                                    port: self.localhostPeer.port,
                                    height: 1,
                                    delegate: nil)
        
        if case .some(.incoming(.version)) = try? self.testServer.channelIn() {
            ()
        } else {
            XCTFail()
        }

        self.writeSomeBlockHeaderData()
        var locator: BlockLocator!
        XCTAssertNoThrow(
            locator = try self.fileRepo.latestBlockLocator().wait()
        )

        XCTAssertNoThrow(
            try testServer.channelOut(command: HeadersCommand(headers: self.blockChain))
        )
        
        if case .some(.pending(let reply)) = try? testServer.channelIn() {
            XCTAssertTrue(reply is VerackCommand)
        } else {
            XCTFail()
        }
        
        var headers: [BlockHeader]!
        XCTAssertNoThrow(
            headers = try client.outgoingGetheaders(locator: locator)
                      .wait()
        )
        headers.map {
            do {
                try $0.enumerated().forEach {
                    let readWireView = try $0.element.wireView()
                    var blockChainWire = self.blockChain[$0.offset]
                    try blockChainWire.wire()
                    let blockChainWireView = try blockChainWire.wireView()
                    XCTAssertEqual(readWireView, blockChainWireView)
                }
                
            } catch { XCTFail("Error thrown \(error.localizedDescription)") }
        }
        
        XCTAssertNoThrow(
            try testServer.channelIn()
        )
        XCTAssertNoThrow(
            try testServer.stop()
        )
    }
}
