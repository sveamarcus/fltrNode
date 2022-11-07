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
import fltrTx
import HaByLo
import NodeTestLibrary
import NIOTransportServices
import XCTest

final class ClientTests: XCTestCase {
    var elg: NIOTSEventLoopGroup!
    var testServer: TestServer!
    var localhostTestServer: PeerAddress!
    var savedSettings: NodeSettings!
    
    weak var testDelegate: TestDelegate!
    
    override func setUp() {
        self.savedSettings = Settings
        TestingHelpers.diSetup()
        
        self.testServer = TestingHelpers.Server.setupServer()
        self.localhostTestServer = PeerAddress(time: UInt32(0),
                                               services: [ .nodeNetwork, .nodeWitness ],
                                               address: .string("::ffff:7f00:1"),
                                               port: PeerAddress.IPPort(testServer.serverPort))
        
        self.elg = .init(loopCount: 1)
    }

    override func tearDown() {
        XCTAssertNoThrow(try testServer.stop())
        self.testServer = nil
        self.localhostTestServer = nil
        
        XCTAssertNil(self.testDelegate)
        
        XCTAssertNoThrow(try self.elg.syncShutdownGracefully())
        self.elg = nil
        
        Settings = self.savedSettings
        self.savedSettings = nil
    }
    
    private func testAddresses(count: Int = 1000) -> [PeerAddress] {
        var addresses: [PeerAddress] = .init()
        (1...count).forEach {
            addresses.append(PeerAddress(time: UInt32(0), services: [ .nodeNetwork ], address: .string("::ffff:7f00:2"), port: .init($0)))
        }
        return addresses
    }

    class TestDelegate: ClientUnsolicitedDelegate {
        let addr: XCTestExpectation?
        let block: XCTestExpectation?
        let tx: XCTestExpectation?
        let _mempool: Node.Mempool?
        
        init(addr: XCTestExpectation? = nil,
             block: XCTestExpectation? = nil,
             tx: XCTestExpectation? = nil,
             mempool: Node.Mempool? = nil) {
            self.addr = addr
            self.block = block
            self.tx = tx
            self._mempool = mempool
        }
        
        func unsolicited(addr: [PeerAddress], source: Client.Key) {
            self.addr?.fulfill()
        }
        
        func unsolicited(block: BlockChain.Hash<BlockHeaderHash>, source: Client.Key) {
            self.block?.fulfill()
        }
        
        func unsolicited(tx: BlockChain.Hash<TransactionLegacyHash>, source: Client.Key) {
            self.tx?.fulfill()
        }
        
        var mempool: Node.Mempool {
            self._mempool ?? [:]
        }
    }
    
    func testMempool() {
        let input = Tx.In(outpoint: Tx.Outpoint(transactionId: .makeHash(from: [ 1, 2, 3, ]),
                                                index: 1),
                          scriptSig: [ 1, 2, 3, ],
                          sequence: .disable,
                          witness: { .init(witnessField: [[]]) })
        let output = Tx.Out(value: 10,
                            scriptPubKey: [ 1, 2, 3, ])
        let wire: Tx.Transaction.Initialize = .wireWitness(version: 2,
                                                           vin: [ input ],
                                                           vout: [ output ],
                                                           locktime: .disable(0))
        var tx = Tx.Transaction(wire)
        XCTAssertNoThrow(try tx.hash())
        let txId = try! tx.getTxId()
        
        var testDelegate: TestDelegate! = .init(mempool: [ txId : tx ])

        let e1 = expectation(description: "connect")
        let client = Client.connect(eventLoop: self.elg.next(),
                                    host: self.localhostTestServer.address,
                                    port: self.localhostTestServer.port,
                                    height: 123,
                                    delegate: testDelegate)
        self.testDelegate = testDelegate
        
        client.connectFuture
        .whenComplete {
            switch $0 {
            case .success:
                e1.fulfill()
            case .failure(let error):
                XCTFail("\(error)")
            }
        }
        if case .some(.incoming(.version)) = try? self.testServer.channelIn() {
            ()
        } else {
            XCTFail()
        }
        
        if case .some(.pending(let bitcoinCommandValue)) = try? self.testServer.channelIn() {
            XCTAssertTrue(bitcoinCommandValue is VerackCommand)
        } else {
            XCTFail()
        }
        if case .some(.unsolicited(.inv(let invCommand))) = try? self.testServer.channelIn() {
            XCTAssertEqual(invCommand.inventory, [ .tx(txId) ])
        } else {
            XCTFail()
        }
        wait(for: [ e1 ], timeout: 2)

        XCTAssertNoThrow(
            try self.testServer.channelOut(command: GetdataCommand(inventory: [ .tx(txId) ]))
        )
        if case .some(.pending(let cmdValue)) = try? self.testServer.channelIn() {
            guard let _ = cmdValue as? Tx.Transaction
            else { XCTFail(); return }
        } else {
            XCTFail()
        }

        XCTAssertNoThrow(
            try self.testServer.channelOut(command: GetdataCommand(inventory: [ .tx(.zero) ]))
        )
        if case .some(.pending(let cmdValue)) = try? self.testServer.channelIn() {
            guard let _ = cmdValue as? NotfoundCommand
            else { XCTFail(); return }
        } else {
            XCTFail()
        }

        testDelegate = nil
        let e2 = expectation(description: "close")
        client.closeFuture.whenSuccess {
            e2.fulfill()
        }

        client.close()
        wait(for: [e2], timeout: 1.0)
    }
    
    func testClientUnsolicitedDelegate() {
        let addrExpectation = expectation(description: "incoming addr")
        let blockExpectation = expectation(description: "block")
        let txExpectation = expectation(description: "tx")
        
        var testDelegate: TestDelegate! = TestDelegate(addr: addrExpectation, block: blockExpectation, tx: txExpectation)
        let e1 = expectation(description: "connect")
        let client = Client.connect(eventLoop: self.elg.next(),
                                    host: self.localhostTestServer.address,
                                    port: self.localhostTestServer.port,
                                    height: 123,
                                    delegate: testDelegate)
        self.testDelegate = testDelegate
        testDelegate = nil

        client.connectFuture.whenComplete {
            switch $0 {
            case .failure: XCTFail()
            case .success:
                e1.fulfill()
            }
        }

        if case .some(.incoming(.version)) = try? self.testServer.channelIn() {
            ()
        } else {
            XCTFail()
        }
        
        if case .some(.pending(let bitcoinCommandValue)) = try? self.testServer.channelIn() {
            XCTAssertTrue(bitcoinCommandValue is VerackCommand)
        } else {
            XCTFail()
        }
        wait(for: [e1], timeout: 1.0)

        XCTAssertNoThrow(try self.testServer.channelOut(command: AddrCommand(addresses: self.testAddresses())))
        XCTAssertNoThrow(try self.testServer.channelOut(command: InvCommand(inventory: [.block(.zero)])))
        XCTAssertNoThrow(try self.testServer.channelOut(command: InvCommand(inventory: [.tx(.zero)])))
        wait(for: [addrExpectation, blockExpectation, txExpectation], timeout: 1.0)

        let e2 = expectation(description: "close")
        client.closeFuture.whenSuccess {
            e2.fulfill()
        }

        client.close()
        wait(for: [e2], timeout: 1.0)
    }
    
    func testEmptyHeadersRoundtrip() throws {
        var testDelegate: TestDelegate! = TestDelegate(addr: nil, block: nil, tx: nil)
        let e1 = expectation(description: "connect")
        let client = Client.connect(eventLoop: self.elg.next(),
                                    host: self.localhostTestServer.address,
                                    port: self.localhostTestServer.port,
                                    height: 123,
                                    delegate: testDelegate)
        self.testDelegate = testDelegate
        testDelegate = nil
        
        client.connectFuture.whenComplete {
            switch $0 {
            case .failure: XCTFail()
            case .success:
                e1.fulfill()
            }
        }

        if case .some(.incoming(.version)) = try? self.testServer.channelIn() {
            ()
        } else {
            XCTFail()
        }
        
        if case .some(.pending(let bitcoinCommandValue)) = try? self.testServer.channelIn() {
            XCTAssertTrue(bitcoinCommandValue is VerackCommand)
        } else {
            XCTFail()
        }
        wait(for: [e1], timeout: 1.0)

        let bl = BlockLocator(hashes: [ .zero, .zero ], stop: .zero)
        let out = client.outgoingGetheaders(locator: bl)
        
        let getHeadersIn = try? self.testServer.channelIn()
        if case .some(.incoming(.getheaders(let getHeaders))) = getHeadersIn {
            XCTAssertEqual(getHeaders.blockLocator.hashes, [ .zero, .zero ])
        } else {
            XCTFail()
        }

        let emptyHeaders = HeadersCommand(headers: [])
        try self.testServer.channelOut(command: emptyHeaders)
        
        _ = try out.wait()
        
        let e2 = expectation(description: "close")
        client.closeFuture.whenSuccess {
            e2.fulfill()
        }

        client.close()
        wait(for: [e2], timeout: 1.0)
    }
    
    func testRejectMessage() {
        let testDelegate: TestDelegate! = TestDelegate(addr: nil, block: nil, tx: nil)
        let client = Client.connect(eventLoop: self.elg.next(),
                                    host: self.localhostTestServer.address,
                                    port: self.localhostTestServer.port,
                                    height: 123,
                                    delegate: testDelegate)
        self.testDelegate = testDelegate

        let rejectExpectation = expectation(description: "")
        let connectExpectation = expectation(description: "")
        
        client.connectFuture.whenComplete {
            switch $0 {
            case .failure: XCTFail()
            case .success:
                connectExpectation.fulfill()
            }
        }

        if case .some(.incoming(.version)) = try? self.testServer.channelIn() {
            ()
        } else {
            XCTFail()
        }
        
        if case .some(.pending(let bitcoinCommandValue)) = try? self.testServer.channelIn() {
            XCTAssertTrue(bitcoinCommandValue is VerackCommand)
        } else {
            XCTFail()
        }
        wait(for: [connectExpectation], timeout: 1.0)
        
        let rejectCommand = RejectCommand(message: "getdata",
                                          ccode: .nonstandard,
                                          reason: "failure test",
                                          hash: .big((0..<32).map(UInt8.init)))
        
        client.closeFuture.whenComplete { _ in
            rejectExpectation.fulfill()
        }
        try? self.testServer.channelOut(command: rejectCommand)
        wait(for: [rejectExpectation], timeout: 1.0)
    }
}
