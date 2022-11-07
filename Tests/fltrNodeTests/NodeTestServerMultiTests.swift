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
import NIO
import NIOTransportServices
import XCTest

final class NodeTestServerMultiTests: XCTestCase {
    var serverElg: MultiThreadedEventLoopGroup!
    var niotsElg: NIOTSEventLoopGroup!
    var niotsEventLoop: EventLoop!
    var embedded: EmbeddedEventLoopLocked!
    var testServers: [TestServer]!
    var userDefaults: UserDefaults!
    var threadPool: NIOThreadPool!
    var fileHandle: NIOFileHandle!
    var walletDelegate: TestWalletDelegate!
    var savedSettings: NodeSettings!
    
    override func setUp() {
        self.savedSettings = Settings
        TestingHelpers.diSetup {
            $0.EVENT_LOOP_GROUPS = 10
            $0.BITCOIN_NUMBER_OF_CLIENTS = 8
            $0.BITCOIN_COMMAND_TIMEOUT = .seconds(10)
            $0.BITCOIN_COMMAND_THROTTLE_TIMEOUT = .seconds(10)
            $0.BITCOIN_HEADERS_MIN_NUMBER = 20
            $0.BITCOIN_MAX_HEADERS_OVERWRITE = 2
            $0.PROTOCOL_SERVICE_OPTIONS = [ .nodeNetwork, .nodeWitness, .nodeFilters, ]
            $0.NODE_REPEATING_TASK_DELAY = .milliseconds(10)
            $0.MUXER_RECONNECT_TIMER = .seconds(10_000)
            
        }
        LOG_LEVEL = .Trace

        TestingHelpers.DB.resetBlockHeaderFile()
        XCTAssertNoThrow(
            self.fileHandle = try TestingHelpers.DB.openFileHandle()
        )

            
        self.serverElg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        self.niotsElg = NIOTSEventLoopGroup(loopCount: 1)
        self.niotsEventLoop = self.niotsElg.next()
        
        self.embedded = EmbeddedEventLoopLocked()
        self.threadPool = NIOThreadPool.init(numberOfThreads: 2)
        self.threadPool.start()

        var testServers: [TestServer] = []
        var localPeerAddresses: [PeerAddress] = []
        (0..<Settings.BITCOIN_NUMBER_OF_CLIENTS).forEach {
            testServers.append(TestServer(group: self.serverElg))
            
            localPeerAddresses.append(PeerAddress(time: UInt32(0),
                                                  services: [ .nodeNetwork, .nodeWitness, ],
                                                  address: .string("::ffff:7f00:1"),
                                                  port: PeerAddress.IPPort(testServers[$0].serverPort)))
        }
        self.testServers = testServers
        Settings.NODE_PROPERTIES.peers = .init(localPeerAddresses)
        Settings.NODE_PROPERTIES.offset = 0
        Settings.NODE_PROPERTIES.processedIndex = 0
        Settings.NODE_PROPERTIES.verifyCheckpoint = 0
        
        self.walletDelegate = .init()
    }
        
    override func tearDown() {
        XCTAssertNoThrow(
            try self.testServers.forEach {
                try $0.stop()
            }
        )
        self.testServers = nil
        self.userDefaults = nil
        XCTAssertNoThrow(try self.fileHandle.close())
        self.fileHandle = nil
        XCTAssertNoThrow(try self.threadPool.syncShutdownGracefully())
        self.threadPool = nil
        XCTAssertNoThrow(try self.embedded.syncShutdownGracefully())
        self.embedded = nil
        XCTAssertNoThrow(try self.niotsElg.syncShutdownGracefully())
        self.niotsElg = nil
        self.niotsEventLoop = nil
        
        Settings = self.savedSettings
        self.savedSettings = nil
    }
        
    struct TestWalletDelegate: NodeDelegate {
        func syncEvent(_ sync: SyncEvent) {
            logger.info("NodeDelegate sync event: \(sync)")
        }
        func newTip(height: Int, commit: () -> Void) {
            logger.info("NodeDelegate new tip height: \(height)")
            commit()
        }
        
        func rollback(height: Int, commit: () -> Void) {
            logger.info("NodeDelegate rollback height \(height)")
            commit()
        }
        
        func transactions(height: Int, events: [TransactionEvent], commit: @escaping (TransactionEventCommitOutcome) -> Void) {
            logger.info("NodeDelegate transaction events \(events) for height \(height)")
            commit(.relaxed)
        }

        func unconfirmedTx(height: Int, events: [TransactionEvent]) {
            logger.info("NodeDelegate unconfirmed tx events \(events)")
        }
        
        func estimatedHeight(_ height: ConsensusChainHeight.CurrentHeight) {
            logger.info("NodeDelegate estimated height \(height)")
        }
        
        func filterEvent(_ event: CompactFilterEvent) {
            logger.info("NodeDelegate filter event \(event)")
        }
    }
        
    func testNetworkedStartStop() {
        NodeTests.dbWriteTest20FullySynched(eventLoop: self.niotsEventLoop)
        let node = Node(eventLoopGroup: self.niotsElg,
                        nodeEventLoop: self.niotsEventLoop,
                        threadPool: self.threadPool,
                        delegate: self.walletDelegate)
        
        XCTAssertNoThrow(try node.start().wait())
        
        XCTAssertNoThrow(try self.testServers.forEach(versionPreamble(_:)))
        
        self.nodeCheckState(getter: { try! node._testingState().wait() }) {
            switch $0 {
            case .download:
                return true
            default:
                return false
            }
        }

        Thread.sleep(forTimeInterval: 0.1)

        switch self.nextServerCommand() {
        case .some(.incoming(.getheaders)):
            break
        default: XCTFail()
        }
        
        XCTAssertNoThrow(try node.stop().wait())
        guard case .initialized = try? node._testingState().wait() else {
            XCTFail()
            return
        }
    }
    
    func testNetworkedStartStopWithExternalNIOThreadPool() {
        NodeTests.dbWriteTest20FullySynched(eventLoop: self.niotsEventLoop)
        
        let threadPool = NIOThreadPool(numberOfThreads: 1)
        threadPool.start()
        let node = Node(eventLoopGroup: self.niotsElg,
                        nodeEventLoop: self.niotsEventLoop,
                        threadPool: threadPool,
                        delegate: self.walletDelegate)
        
        XCTAssertNoThrow(try node.start().wait())
        
        XCTAssertNoThrow(try self.testServers.forEach(versionPreamble(_:)))
        
        self.nodeCheckState(getter: { try! node._testingState().wait() }) {
            switch $0 {
            case .download:
                return true
            default:
                return false
            }
        }

        Thread.sleep(forTimeInterval: 0.1)

        switch self.nextServerCommand() {
        case .some(.incoming(.getheaders)):
            break
        default: XCTFail()
        }
        
        XCTAssertNoThrow(try node.stop().wait())
        guard case .initialized = try? node._testingState().wait() else {
            XCTFail()
            return
        }

        struct StillStartedMustThrow: Swift.Error {}
        XCTAssertThrowsError(try threadPool.runIfActive(eventLoop: self.niotsEventLoop) {
            throw StillStartedMustThrow()
        }.wait()) {
            guard $0 is StillStartedMustThrow
            else {
                XCTFail()
                return
            }
        }
        XCTAssertNoThrow(try threadPool.syncShutdownGracefully())
    }
    
//    func testStopFromRestructure() {
//        NodeTests.dbWriteTest20FullySynched(eventLoop: self.niotsEventLoop)
//        let node = Node(eventLoopGroup: self.niotsElg,
//                        nodeEventLoop: self.niotsEventLoop,
//                        threadPool: self.threadPool,
//                        delegate: self.walletDelegate)
//
//        XCTAssertNoThrow(try node.start().wait())
//        XCTAssertNoThrow(try self.testServers.forEach(versionPreamble(_:)))
//
//        var downloadState: Node.DownloadState!
//        self.nodeCheckState(getter: { try! node._testingState().wait() }) {
//            switch $0 {
//            case .download(let state):
//                downloadState = state
//                return true
//            default:
//                return false
//            }
//        }
//
//        Thread.sleep(forTimeInterval: 0.1)
//
//        switch self.nextServerCommand() {
//        case .some(.incoming(.getheaders)):
//            break
//        default: XCTFail()
//        }
//
//        let networkState = Node.NetworkState(downloadState)
//        let repoState: Node.RestructureState = .init(networkState, eventLoop: self.niotsEventLoop)
//        XCTAssertNoThrow(try node._testingSetState(.restructure(repoState, followedBy: .network)).wait())
//
//        XCTAssertNoThrow(try node.stop().wait())
//        guard case .initialized = try? node._testingState().wait() else {
//            XCTFail()
//            return
//        }
//    }
    
    func testStopFromNetwork() {
        NodeTests.dbWriteTest20FullySynched(eventLoop: self.niotsEventLoop)
        let node = Node(eventLoopGroup: self.niotsElg,
                        nodeEventLoop: self.niotsEventLoop,
                        threadPool: self.threadPool,
                        delegate: self.walletDelegate)
        XCTAssertNoThrow(try node.start().wait())
        XCTAssertNoThrow(try self.testServers.forEach(versionPreamble(_:)))

        var downloadState: Node.DownloadState!
        self.nodeCheckState(getter: { try! node._testingState().wait() }) {
            switch $0 {
            case .download(let state):
                downloadState = state
                return true
            default:
                return false
            }
        }

        Thread.sleep(forTimeInterval: 0.1)
        
        let cmd = self.nextServerCommand()
        switch cmd {
        case .some(.incoming(.getheaders)):
            break
        default: XCTFail("unexpected command \(String(describing: cmd))")
        }

        let networkState = Node.NetworkState(downloadState)
        XCTAssertNoThrow(try node._testingSetState(.network(networkState)).wait())
        
        XCTAssertNoThrow(try node.stop().wait())
        guard case .initialized = try? node._testingState().wait() else {
            XCTFail()
            return
        }
    }
    
    // TODO: DISABLED in Node for now, maybe delete?
    /*
    func testRestructure() {
        Settings.BITCOIN_HEADERS_RESTRUCTURE_FACTOR = 1
        Settings.BITCOIN_HEADERS_MIN_NUMBER = 1
        Settings.PROTOCOL_SERVICE_OPTIONS = []
        NodeTests.dbWriteTest20()
        precondition(Settings.NODE_PROPERTIES.offset == 0)

        let node = Node(eventLoopGroup: EventLoopGroup, nodeEventLoop: self.niotsEventLoop, delegate: self.walletDelegate)
        XCTAssertNoThrow(try node.start().wait())
        XCTAssertNoThrow(try self.testServers.forEach(versionPreamble(_:)))

        self.nodeCheckState(getter: { try! node._testingState().wait() }) {
            switch $0 {
            case .download:
                return true
            default:
                return false
            }
        }

        Thread.sleep(forTimeInterval: 0.1)
        
        let cmd = self.nextServerCommand()
        switch cmd {
        case .some(.incoming(.getheaders)):
            break
        default: XCTFail("unexpected command \(String(describing: cmd))")
        }

        XCTAssertNoThrow(try node.stop().wait())
        guard case .initialized = try? node._testingState().wait() else {
            XCTFail()
            return
        }
        
        XCTAssertEqual(Settings.NODE_PROPERTIES.offset, 9)
        let repo = TestingHelpers.DB.getFileBlockHeaderRepo(offset: Settings.NODE_PROPERTIES.offset,
                                                            threadPool: self.threadPool,
                                                            eventLoop: self.niotsEventLoop)
        XCTAssertNoThrow(
            XCTAssertEqual(try repo.heights().wait().lowerHeight, 9)
        )
        XCTAssertNoThrow(
            XCTAssertEqual(try repo.heights().wait().upperHeight, 19)
        )
        XCTAssertNoThrow(try repo.close().wait())
    }
    */
}

extension NodeTestServerMultiTests {
    func nodeCheckState(getter: @escaping () -> Node.ReactorState, _ state: @escaping (Node.ReactorState) -> Bool) {
        let nextState = expectation(description: "nextState")
        var finished = false
        
        func checkState() {
            DispatchQueue.global().asyncAfter(deadline: .now() + .milliseconds(100)) {
                finished = state(
                    getter()
                )
                if finished {
                    nextState.fulfill()
                } else {
                    checkState()
                }
            }
        }

        checkState()
        wait(for: [nextState], timeout: 2.0)
    }
    
    func nextServerCommand() -> FrameToCommandHandler.InboundOut? {
        let server = self.testServers.sorted(by: { try! $0.count.wait() > $1.count.wait() }).first!
        
        guard try! server.count.wait() > 0
        else { return nil }
        
        return try! server.channelIn()
    }
}
