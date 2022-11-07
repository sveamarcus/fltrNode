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
import NodeTestLibrary
import NIO
import NIOConcurrencyHelpers
import NIOTransportServices
import XCTest

final class MuxerTests: XCTestCase {
    var muxer: Muxer!
    var clientFactory: (() -> Client)!
    var serverElg: MultiThreadedEventLoopGroup!
    var niotsElg: NIOTSEventLoopGroup!
    var niotsEventLoop: EventLoop!
    var testServers: [TestServer]!
    var localPeerAddresses: [PeerAddress]!
    var testClients: [Client]!
    var savedSettings: NodeSettings!
    
    override func setUp() {
        self.savedSettings = Settings
        TestingHelpers.diSetup {
            $0.BITCOIN_NUMBER_OF_CLIENTS = 8
            $0.BITCOIN_COMMAND_TIMEOUT = .seconds(5)
            $0.BITCOIN_COMMAND_THROTTLE_TIMEOUT = .seconds(2)
            $0.BITCOIN_UNSOLICITED_FILTER_CACHE = 1
        }

        self.serverElg = MultiThreadedEventLoopGroup(numberOfThreads: 1)

        self.niotsElg = NIOTSEventLoopGroup(loopCount: 1)
        self.niotsEventLoop = self.niotsElg.next()
        
        var testServers: [TestServer] = []
        var localPeerAddresses: [PeerAddress] = []
        (0..<Settings.BITCOIN_NUMBER_OF_CLIENTS).forEach {
            testServers.append(.init(group: self.serverElg))
            localPeerAddresses.append(PeerAddress(time: UInt32(0),
                                                  services: [ .nodeNetwork, .nodeWitness, ],
                                                  address: .string("::ffff:7f00:1"),
                                                  port: PeerAddress.IPPort(testServers[$0].serverPort)))
        }
        self.testServers = testServers
        self.localPeerAddresses = localPeerAddresses
        self.testClients = []
    }
    
    override func tearDown() {
        XCTAssertNoThrow(
            try self.testServers.forEach {
                try $0.stop()
            }
        )
        self.testServers = nil
        
        XCTAssertNoThrow(try self.niotsElg.syncShutdownGracefully())
        self.niotsElg = nil
        
        Settings = self.savedSettings
        self.savedSettings = nil
    }
    
    enum Failing {
        case none
        case first
        case second
        case all
    }
    
    func clientFactoryTesting(_ addresses: [PeerAddress], failing: Failing) -> (ClientUnsolicitedDelegate?) -> Client {
        let lock: NIOConcurrencyHelpers.NIOLock = .init()
        var called = 0
        return { muxer in
            guard called < addresses.count else {
                preconditionFailure("Tried to load more than preconfigured number of clients")
            }

            let btcClient: Client
            switch (failing, called) {
            case (.first, 1), (.second, 2), (.all, _):
                btcClient =  lock.withLock { Client.connect(eventLoop: self.niotsEventLoop,
                                                               host: .zero,
                                                               port: 0,
                                                               height: 1,
                                                               delegate: muxer) }

            case (.none, _), (.first, _), (.second, _):
                btcClient = lock.withLock { Client.connect(eventLoop: self.niotsEventLoop,
                                                              host: addresses[called].address,
                                                              port: addresses[called].port,
                                                              height: 1,
                                                              delegate: muxer) }
            }
            
            lock.withLockVoid {
                called += 1
                self.testClients.append(btcClient)
            }

            return btcClient
        }
    }
    
    func setupMuxer(with delegate: MuxerDelegateProtocol?, failing: Failing) {
        let clientFactory = ClientFactory.testMuxer(addresses: self.localPeerAddresses,
                                                    eventLoop: self.niotsEventLoop,
                                                    failing: failing,
                                                    append: { self.testClients.append($0) })
        self.muxer = Muxer(muxerDelegate: delegate,
                           clientFactory: clientFactory)
    }

    class TestDelegate: MuxerDelegateProtocol {
        let stopExpectation: XCTestExpectation?
        let addrExpectation: XCTestExpectation?
        var addrExpectationOnce = true
        let blockExpectation: XCTestExpectation?
        var blockExpectationOnce = true
        let txExpectation: XCTestExpectation?
        var txExpectationOnce = true
        
        init(stopExpectation: XCTestExpectation? = nil,
             addrExpectation: XCTestExpectation? = nil,
             blockExpectation: XCTestExpectation? = nil,
             txExpectation: XCTestExpectation? = nil) {
            self.stopExpectation = stopExpectation
            self.addrExpectation = addrExpectation
            self.blockExpectation = blockExpectation
            self.txExpectation = txExpectation
        }
        
        func muxerStopComplete() {
            self.stopExpectation?.fulfill()
        }
        
        func unsolicited(addr: [PeerAddress], source: Client.Key) {
            if addrExpectationOnce {
                self.addrExpectationOnce.toggle()
                self.addrExpectation?.fulfill()
            } else {
                XCTFail()
            }
        }
        
        func unsolicited(block: BlockChain.Hash<BlockHeaderHash>, source: Client.Key) {
            if blockExpectationOnce {
                self.blockExpectationOnce.toggle()
                self.blockExpectation?.fulfill()
            } else {
                XCTFail()
            }
        }
        
        func unsolicited(tx: BlockChain.Hash<TransactionLegacyHash>, source: Client.Key) {
            if txExpectationOnce {
                self.txExpectationOnce.toggle()
                self.txExpectation?.fulfill()
            } else {
                XCTFail()
            }
        }
        
        var mempool: Node.Mempool { [:] }

        func guess(height: Int) {
            XCTAssertEqual(height, 1)
        }
    }

    func testMultipleConnectionsVersionHandshake() {
        let e = self.expectation(description: "stopped")
        self.setupMuxer(with: TestDelegate(stopExpectation: e), failing: .none)
        
        self.muxer.renewConnections()
        XCTAssertNoThrow(
            try self.testServers.forEach(versionPreamble(_:))
        )
        
        XCTAssertEqual(self.testClients.count, 8)
        self.testClients.forEach {
            $0.close()
        }
        
        self.muxer.stop()
        wait(for: [e], timeout: 2.0)
    }

    func testMultipleConnectionsClose() {
        let stoppedExpectation = expectation(description: "stopped")
        self.setupMuxer(with: TestDelegate(stopExpectation: stoppedExpectation), failing: .none)
        
        self.muxer.renewConnections()
        XCTAssertNoThrow(
            try self.testServers.forEach(versionPreamble(_:))
        )

        self.muxer.stop()
        wait(for: [stoppedExpectation], timeout: 2)
    }
    
    func testMultipleConnectionsStoppedAfterPending() {
        let stoppedExpectation = expectation(description: "stopped")
        let delegate = TestDelegate(stopExpectation: stoppedExpectation)
        self.setupMuxer(with: delegate, failing: .none)
        
        self.muxer.renewConnections()
        XCTAssertNoThrow(
            try self.testServers.forEach(versionPreamble(_:))
        )
        
        Thread.sleep(forTimeInterval: 0.2)
        XCTAssertEqual(self.muxer.stateForTesting().pending, 0)

        self.muxer.setStateForTesting(pending: 1)
        self.muxer.stop()
        wait(for: [stoppedExpectation], timeout: 2)
        XCTAssertEqual(self.muxer.stateForTesting().pending, 1)
        
        XCTAssert(self.muxer.muxerDelegate == nil)
        
        struct DeinitTest {
            weak var muxer: Muxer?
        }
        
        let deinitTest = DeinitTest(muxer: self.muxer)
        do {
            self.muxer = nil
        }
        
        Thread.sleep(forTimeInterval: 0.2)
        XCTAssert(deinitTest.muxer == nil)
    }
    
    func testWithAllFailingConnections() {
        let stoppedExpectation = expectation(description: "stopped")
        let delegate = TestDelegate(stopExpectation: stoppedExpectation)
        self.setupMuxer(with: delegate, failing: .all)
        
        self.muxer.renewConnections()
        Thread.sleep(forTimeInterval: 0.2)

        var allFail = 0
        self.testClients.forEach {
            $0.connectFuture.whenSuccess { _ in
                XCTFail()
            }
            $0.connectFuture.whenFailure { _ in
                allFail += 1
            }
        }
        
        self.muxer.stop()
        wait(for: [stoppedExpectation], timeout: 20)
        XCTAssertEqual(allFail, self.localPeerAddresses.count)
    }

    func testNextClient() {
        let stoppedExpectation = expectation(description: "stopped")
        let delegate = TestDelegate(stopExpectation: stoppedExpectation)
        self.setupMuxer(with: delegate, failing: .none)

        self.muxer.renewConnections()
        XCTAssertNoThrow(
            try self.testServers.forEach(versionPreamble(_:))
        )

        let firstServer = self.testServers.first.map(\.serverPort).map(UInt16.init)
        let index = self.testClients.firstIndex(where: { $0.hostPort?.port == firstServer })

        self.testClients.indices.forEach {
            guard $0 != index else {
                return
            }
            
            let client = self.testClients[$0]
            
            client.outgoingPing().and(client.outgoingPing()).and(client.outgoingPing()).whenComplete { _ in return }
        }
        Thread.sleep(forTimeInterval: 0.2)
        
        let client = self.muxer.next()
        XCTAssertNotNil(client)
        XCTAssertEqual(client.map(Client.Key.init), Client.Key(self.testClients[index!]))
        XCTAssertEqual(client.map(\.queueDepth), 0)
        
        self.testServers.dropFirst().forEach {
            _ = try? $0.channelIn()
        }

        self.muxer.stop()
        wait(for: [stoppedExpectation], timeout: 2.0)
    }
    
    func testNamedClient() {
        let stoppedExpectation = expectation(description: "stopped")
        let delegate = TestDelegate(stopExpectation: stoppedExpectation)
        self.setupMuxer(with: delegate, failing: .none)

        self.muxer.renewConnections()
        XCTAssertNoThrow(
            try self.testServers.forEach(versionPreamble(_:))
        )
        
        Thread.sleep(forTimeInterval: 0.2)

        let client = self.testClients.randomElement()
        let key = client.map(Client.Key.init)
        XCTAssertNotNil(key.flatMap(self.muxer.next))
        XCTAssertEqual(client, key.flatMap(self.muxer.next))

        self.muxer.stop()
        wait(for: [stoppedExpectation], timeout: 2.0)
    }
    
    func testServicesClient() {
        let stoppedExpectation = expectation(description: "stopped")
        let delegate = TestDelegate(stopExpectation: stoppedExpectation)
        self.setupMuxer(with: delegate, failing: .none)

        self.muxer.renewConnections()
        XCTAssertNoThrow(
            try self.testServers.forEach(versionPreamble(_:))
        )

        Thread.sleep(forTimeInterval: 0.1)

        let client = self.testClients.randomElement()
        let serviceFlags = try? client?.services()
        
        XCTAssertNotNil(serviceFlags.map(self.muxer.random(with:)))
        XCTAssertNil(self.muxer.random(with: [.nodeBloom,
                                              .nodeGetUtxo,
                                              .nodeFilters,
                                              .nodeNetwork,
                                              .nodeNetworkLimited,
                                              .nodeWitness]))
        
        self.muxer.stop()
        wait(for: [stoppedExpectation], timeout: 2.0)
    }
    
    func testUnsolicitedDelegateCallOnceFilter() {
        let stopExpectation = expectation(description: "stopped")
        let addrExpectation = expectation(description: "addrExpectation")
        let blockExpectation = expectation(description: "blockExpectation")
        let txExpectation = expectation(description: "txExpectation")
        let delegate = TestDelegate(stopExpectation: stopExpectation,
                                    addrExpectation: addrExpectation,
                                    blockExpectation: blockExpectation,
                                    txExpectation: txExpectation)
        self.setupMuxer(with: delegate, failing: .none)

        self.muxer.renewConnections()
        XCTAssertNoThrow(
            try self.testServers.forEach(versionPreamble(_:))
        )

        XCTAssertNoThrow(
            try self.testServers.forEach {
                try $0.channelOut(command: AddrCommand(addresses: self.localPeerAddresses))
                try $0.channelOut(command: InvCommand(inventory: [.block(.zero)]))
                try $0.channelOut(command: InvCommand(inventory: [.tx(.zero)]))
            }
        )
        wait(for: [addrExpectation, blockExpectation, txExpectation], timeout: 2.0)
        
        self.muxer.stop()
        wait(for: [stopExpectation], timeout: 2.0)
    }
    
    func testUnsolicitedDelegateFilterDepth() {
        let stopExpectation = expectation(description: "stopped")
        let blockExpectation = expectation(description: "blockExpectation")
        
        class LocalTestDelegate: MuxerDelegateProtocol {
            func guess(height: Int) {
                XCTAssertEqual(height, 1)
            }
            
            let stop: XCTestExpectation
            let block: XCTestExpectation
            let limit: Int
            var called = 0
            
            init(stop: XCTestExpectation, block: XCTestExpectation, limit: Int) {
                self.stop = stop; self.block = block; self.limit = limit
            }

            var mempool: Node.Mempool { [:] }

            func muxerStopComplete() {
                self.stop.fulfill()
            }
            
            func unsolicited(block: BlockChain.Hash<BlockHeaderHash>, source: Client.Key) {
                called += 1
                if called < limit {
                    return
                } else if called == limit {
                    self.block.fulfill()
                    return
                } else {
                    XCTFail()
                    return
                }
            }
            
            func unsolicited(addr: [PeerAddress], source: Client.Key) {}
            func unsolicited(tx: BlockChain.Hash<TransactionLegacyHash>, source: Client.Key) {}
        }
        
        let blockInv: [Inventory] = [.block(0), .block(1), .block(0), .block(1)]
        let delegate = LocalTestDelegate(stop: stopExpectation,
                                         block: blockExpectation,
                                         limit: 8 * blockInv.count)
        self.setupMuxer(with: delegate, failing: .none)
        self.muxer.renewConnections()
        XCTAssertNoThrow(
            try self.testServers.forEach(versionPreamble(_:))
        )

        XCTAssertNoThrow(
            try self.testServers.forEach {
                try $0.channelOut(command: InvCommand(inventory: blockInv))
            }
        )
        wait(for: [blockExpectation], timeout: 2.0)

        self.muxer.stop()
        wait(for: [stopExpectation], timeout: 2.0)
    }
}

extension ClientFactory {
    static func testMuxer(addresses: [PeerAddress],
                          eventLoop: EventLoop,
                          failing: MuxerTests.Failing,
                          append: @escaping (Client) -> Void) -> Self {
        let lock: NIOConcurrencyHelpers.NIOLock = .init()
        var called = 0
        
        return .init { delegate, height in
            guard called < addresses.count else {
                preconditionFailure("Tried to load more than preconfigured number of clients")
            }
            
            let btcClient: Client
            switch (failing, called) {
            case (.first, 1), (.second, 2), (.all, _):
                btcClient = lock.withLock {
                    Client.connect(eventLoop: eventLoop,
                                   host: .zero,
                                   port: 0,
                                   height: height,
                                   delegate: delegate)
                }
            case (.none, _), (.first, _), (.second, _):
                btcClient = lock.withLock {
                    Client.connect(eventLoop: eventLoop,
                                   host: addresses[called].address,
                                   port: addresses[called].port,
                                   height: 1,
                                   delegate: delegate)
                }
            }
            
            lock.withLockVoid {
                called += 1
                append(btcClient)
            }
            
            return eventLoop.makeSucceededFuture(btcClient)
        }
        height: { 1 }
    }
}
