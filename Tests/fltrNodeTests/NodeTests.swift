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
import FileRepo
import fltrTx
import fltrWAPI
import Stream64
import HaByLo
import NodeTestLibrary
import NIO
import NIOConcurrencyHelpers
import NIOTransportServices
import XCTest

final class NodeTests: XCTestCase {
    var channel: EmbeddedChannelLocked!
    var client: Client!
    var embedded: EmbeddedEventLoopLocked!
    var elg: NIOTSEventLoopGroup!
    var fileHandle: NIOFileHandle!
    var muxer: Muxer!
    var node: Node!
    var threadPool: NIOThreadPool!
    var cfMatcher: CF.MatcherClient!
    var nodeDelegate: EmptyTestDelegate!
    var savedSettings: NodeSettings!

    var nonBlockingFileIODependency: ((NIOThreadPool) -> NonBlockingFileIOClient)!
    

    class EmptyTestDelegate: NodeDelegate {
        let lock: NIOConcurrencyHelpers.NIOLock = .init()
        var filterEventCallback: ((CompactFilterEvent) -> Void)?
        var transactionCallback: ((Int, [TransactionEvent]) -> TransactionEventCommitOutcome)?
        var latestEstimatedHeight: ConsensusChainHeight.CurrentHeight? = nil
        var newTipHeight: Int? = nil
        var rollbackEvents: [Int] = []
        var syncEvents: [SyncEvent] = []
        private var __transactionEvents: [TransactionEvent] = []
        private var __expectation: XCTestExpectation? = nil
        
        var transactionEvents: [TransactionEvent] {
            self.lock.withLock {
                self.__transactionEvents
            }
        }
        
        func expectation(_ e: XCTestExpectation) {
            self.lock.withLockVoid {
                self.__expectation = e
            }
        }
        
        private func fulfill() {
            self.lock.withLockVoid {
                self.__expectation?.fulfill()
                self.__expectation = nil
            }
        }

        func estimatedHeight(_ height: ConsensusChainHeight.CurrentHeight) {
            self.latestEstimatedHeight = height
        }
        
        func filterEvent(_ event: CompactFilterEvent) {
            lock.withLockVoid {
                self.filterEventCallback?(event)
            }
            self.fulfill()
        }

        func newTip(height: Int, commit: () -> Void) {
            self.lock.withLockVoid {
                self.newTipHeight = height
                commit()
            }
            self.fulfill()
        }
        
        func rollback(height: Int, commit: () -> Void) {
            self.lock.withLockVoid {
                self.rollbackEvents.append(height)
            }
            commit()
            self.fulfill()
        }
        
        func syncEvent(_ sync: SyncEvent) {
            self.lock.withLockVoid {
                self.syncEvents.append(sync)
            }
            self.fulfill()
        }
        
        func transactions(height: Int, events: [TransactionEvent], commit: @escaping (TransactionEventCommitOutcome) -> Void) {
            self.lock.withLockVoid {
                commit(self.transactionCallback?(height, events) ?? .relaxed)
            }
            self.fulfill()
        }

        func unconfirmedTx(height: Int, events: [TransactionEvent]) {
            self.lock.withLockVoid {
                self.__transactionEvents.append(contentsOf: events)
            }
            self.fulfill()
        }
    }
    
    override func setUp() {
        self.savedSettings = Settings
        
        self.elg = .init(loopCount: 1)
        self.nonBlockingFileIODependency = { .embedded(threadPool: $0, elg: self.elg) }
        
        TestingHelpers.diSetup {
            $0.EVENT_LOOP_GROUPS = 10
            $0.BITCOIN_COMMAND_TIMEOUT = .seconds(10)
            $0.BITCOIN_COMMAND_THROTTLE_TIMEOUT = .seconds(10)
            $0.BITCOIN_MAX_HEADERS_OVERWRITE = 5
            $0.BITCOIN_HEADERS_MIN_NUMBER = 5
            $0.BITCOIN_NUMBER_OF_CLIENTS = 1
            $0.BITCOIN_HEADERS_RESTRUCTURE_FACTOR = 2
            $0.PROTOCOL_SERVICE_OPTIONS = [ .nodeNetwork, .nodeWitness, .nodeFilters, ]
            $0.MUXER_RECONNECT_TIMER = .seconds(10_000)
            $0.NONBLOCKING_FILE_IO = self.nonBlockingFileIODependency
        }
        LOG_LEVEL = .Trace
        
        self.embedded = EmbeddedEventLoopLocked()

        self.threadPool = NIOThreadPool.init(numberOfThreads: 1)
        self.threadPool?.start()

        Settings.NODE_PROPERTIES.offset = 0
        Settings.NODE_PROPERTIES.peers = .init()
        Settings.NODE_PROPERTIES.processedIndex = 0
        Settings.NODE_PROPERTIES.verifyCheckpoint = 11_000
        Settings.NODE_PROPERTIES.walletCreation = .set(.height(0))
        
        TestingHelpers.DB.resetBlockHeaderFile()
        XCTAssertNoThrow(
            self.fileHandle = try TestingHelpers.DB.openFileHandle()
        )
        
        self.nodeDelegate = EmptyTestDelegate()
        self.node = Node(eventLoopGroup: self.elg,
                         nodeEventLoop: self.embedded,
                         threadPool: self.threadPool,
                         delegate: self.nodeDelegate)

        let nodeMuxerDelegate = self.node._testingMuxerDelegate()
        self.embedded?.run()
        guard let delegate = try? nodeMuxerDelegate.wait() else {
            XCTFail()
            return
        }

        self.muxer = Muxer(muxerDelegate: delegate,
                           clientFactory: .init { _, _ in
                            self.embedded.makeSucceededFuture(self.client)
                           }
                           height: { 1 })
        self.client = Client.connect(eventLoop: self.embedded,
                                     host: .loopback,
                                     port: .zero,
                                     height: 1,
                                     delegate: self.muxer)
        self.muxer?.renewConnections()
        self.channel = self.client?._embedded
        XCTAssertNotNil(self.channel)
        
        self.cfMatcher = .reference()
        XCTAssertNoThrow(try self.cfMatcher.start(eventLoop: self.embedded).wait())
    }
    
    override func tearDown() {
        self.cfMatcher.stop()
        self.cfMatcher = nil
        
        self.nonBlockingFileIODependency = nil
        XCTAssertNoThrow(try self.channel?.finish(acceptAlreadyClosed: true))
        self.channel = nil
        XCTAssertNoThrow(try self.embedded?.syncShutdownGracefully())
        self.embedded = nil

        XCTAssertNoThrow(try self.fileHandle?.close())
        self.fileHandle = nil
        XCTAssertNoThrow(try self.threadPool?.syncShutdownGracefully())
        self.threadPool = nil
        self.muxer?.stop()
        self.client?.close()

        self.node = nil
        self.muxer = nil
        self.client = nil
        XCTAssertNoThrow(try self.elg.syncShutdownGracefully())
        self.elg = nil
        
        Settings = self.savedSettings
        self.savedSettings = nil
    }
    
    func testNodeSyncHeight() {
        let (bh, _, _) = Self.dbWriteTest20FullySynched(eventLoop: self.elg.next())

        XCTAssertEqual(self.waitNow(self.node._testingCurrentHeight()), .guessed(1))

        self.nodeStartDownload()

        XCTAssertEqual(self.client.queueDepth, 1)
        XCTAssert(self.nodeReadPending().bitcoinCommandValue is GetheadersCommand)
        self.nodeWritePendingCommand(HeadersCommand(headers: []))
        
        XCTAssertNil(try? self.channel?.readOutbound(as: QueueHandler.OutboundOut.self))
        self.delay()

        self.nodeCheckState {
            switch $0 {
            case .network:
                return true
            default:
                return false
            }
        }

        XCTAssertEqual(self.waitNow(self.node._testingCurrentHeight()), .synced(19, bh.last!))

        self.nodeStop()
    }
    
    func testNodeDownloadTransitionAllDownloaders() throws {
        TestingHelpers.DB.writeFakeBlockHeaders(count: 20, eventLoop: self.elg.next())
        TestingHelpers.DB.writeFakeCompactFilters(count: 15, processed: 10, eventLoop: self.elg.next())
        let cfRepo = self.makeCfRepo()
        XCTAssertNoThrow(try cfRepo.addFlags(id: 10, flags: [.matchingFilter]).wait())
        self.repoClose(cfRepo)

        self.nodeStartDownload()

        XCTAssertEqual(self.client.queueDepth, 5)

        XCTAssert(self.nodeReadPending().bitcoinCommandValue is GetdataCommand)
        self.nodeWritePendingCommand(BlockCommand())

        XCTAssert(self.nodeReadPending().bitcoinCommandValue is GetheadersCommand)
        self.nodeWritePendingCommand(HeadersCommand(headers: []))

        XCTAssert(self.nodeReadPending().bitcoinCommandValue is CF.GetcfheadersCommand)
        self.nodeWritePendingCommand(CF.CfheadersCommand(filterType: .basic, stop: .zero, headers: []))

        XCTAssert(self.nodeReadPending().bitcoinCommandValue is CFBundleCommand)
        (0..<3).forEach { _ in
            self.nodeWritePendingCommand(CF.CfilterCommand(.wire(filterType: .basic,
                                                                 blockHash: .zero,
                                                                 filterHash: GolombCodedSetClient.emptyHash,
                                                                 n: 0,
                                                                 compressed: [])))
        }

        XCTAssert(self.nodeReadPending().bitcoinCommandValue is CFBundleCommand)
        (0..<1).forEach { _ in
            self.nodeWritePendingCommand(CF.CfilterCommand(.wire(filterType: .basic,
                                                                 blockHash: .zero,
                                                                 filterHash: GolombCodedSetClient.emptyHash,
                                                                 n: 0,
                                                                 compressed: [])))
        }

        self.delay()

        self.nodeCheckState {
            switch $0 {
            case .network: return true
            default: return false
            }
        }

        self.nodeStop()
    }
    
    func testWalletDelegateUnsolicitedTransactionEvent() throws {
        Self.dbWriteTest20FullySynched(eventLoop: self.elg.next())
        
        self.waitNow(self.node.start())
        let pubKey1: PublicKeyHash = .init(1)
        let outpoint1: Tx.Outpoint = .init(transactionId: .little((1...32).reversed().map { $0 << 1 }), index: 100)
        self.nodeSetNetworkState(pubKeys: [ ScriptPubKey(tag: 0,
                                                         index: 1,
                                                         opcodes: pubKey1.scriptPubKeyLegacyWPKH),
                                            ScriptPubKey(tag: 1,
                                                         index: 1,
                                                         opcodes: pubKey1.scriptPubKeyWPKH), ],
                                 outpoints: [ outpoint1, ])
        self.tick() // execute .network state
        self.nodeCheckState {
            switch $0 {
            case .download:
                return true
            default:
                return false
            }
        }

        // prepare first transaction (TransactionEvent.new)
        let outpointTx1: Tx.Outpoint = .init(transactionId: .little((1...32).map { $0 }), index: 0)
        let unhashed1 = Tx.makeSegwitTx(for: outpointTx1, scriptPubKey: pubKey1.scriptPubKeyLegacyWPKH)
        let tx1 = Tx.AnyIdentifiableTransaction(unhashed1)
        let hash1 = tx1.txId
        let nextOutpoint: Tx.Outpoint = .init(transactionId: hash1, index: 0)

        // prepare second transaction (TransactionEvent.spent)
        let unhashed2 = Tx.makeSegwitTx(for: outpoint1, scriptPubKey: [ 0, ])
        let tx2 = Tx.AnyIdentifiableTransaction(unhashed2)
        let hash2 = tx2.txId
        var buffer1 = ByteBufferAllocator().buffer(capacity: 1000)
        var buffer2 = ByteBufferAllocator().buffer(capacity: 1000)
        unhashed1.write(to: &buffer1)
        unhashed2.write(to: &buffer2)
        let send1 = Tx.Transaction(fromBuffer: &buffer1)!
        let send2 = Tx.Transaction(fromBuffer: &buffer2)!

        XCTAssert(self.nodeReadPending().bitcoinCommandValue is GetheadersCommand)
        self.nodeWritePendingCommand(HeadersCommand(headers: []))
        self.delay()

        // send + verify first tx through NodeDelegate
        self.nodeWriteUnsolicitedCommand(.inv(InvCommand(inventory: [ .tx(hash1), ])))
        XCTAssert(self.nodeReadPending().bitcoinCommandValue is GetdataCommand)
        let e1 = expectation(description: "")
        self.nodeDelegate.expectation(e1)
        self.nodeWritePendingCommand(send1)
        let scriptPubKey = ScriptPubKey(tag: 0, index: 1, opcodes: pubKey1.scriptPubKeyLegacyWPKH)
        var verifyEvents = [ TransactionEvent.new(.init(outpoint: nextOutpoint,
                                                        amount: 10_000,
                                                        scriptPubKey: scriptPubKey)) ]
        wait(for: [e1], timeout: 2)
        XCTAssertEqual(self.nodeDelegate.transactionEvents, verifyEvents)

        // send second tx among a few of first tx as well, which should be filtered since already reviewed
        self.nodeWriteUnsolicitedCommand(.inv(InvCommand(inventory: [ .tx(hash1), .tx(hash2), .tx(hash1), ])))
        switch self.nodeReadPending().bitcoinCommandValue {
        case let getdata as GetdataCommand:
            XCTAssertEqual(getdata.inventory, [ .witnessTx(hash2), ])
        default:
            XCTFail()
        }
        let e2 = expectation(description: "")
        self.nodeDelegate.expectation(e2)
        self.nodeWritePendingCommand(send2)
        let spent = SpentOutpoint(outpoint: outpoint1,
                                  outputs: [ .outgoing(10_000, [ 0, ]) ],
                                  tx: .init(tx2))
        verifyEvents.append(.spent(spent))
        wait(for: [e2], timeout: 2)
        XCTAssertEqual(self.nodeDelegate.transactionEvents, verifyEvents)
        
        // .download -> .network transition
        self.nodeCheckState {
            switch $0 {
            case .network:
                return true
            default:
                return false
            }
        }
        self.nodeCheckState {
            switch $0 {
            case .synched:
                return true
            default:
                return false
            }
        }
        self.nodeStop()
    }

    func testNodeNetworkedStateRollback() {
        let (bh, _, _) = Self.dbWriteTest20FullySynched(eventLoop: self.elg.next())
        
        self.nodeStartDownload()
        
        XCTAssertEqual(self.client.queueDepth, 1)
        XCTAssert(self.nodeReadPending().bitcoinCommandValue is GetheadersCommand)

        let wireHeaders: [BlockHeader] = Self.nextHeaders(count: 10, last: bh.suffix(3).first!).wire
        self.nodeWritePendingCommand(HeadersCommand(headers: wireHeaders))

        self.nodeCheckState {
            switch $0 {
            case .network:
                return true
            default:
                return false
            }
        }

        self.nodeCheckState {
            switch $0 {
            case .download:
                return true
            default:
                return false
            }
        }

        XCTAssert(self.nodeReadPending().bitcoinCommandValue is GetheadersCommand)
        self.nodeWritePendingCommand(HeadersCommand(headers: []))
        XCTAssertEqual(self.waitNow(self.node._testingCurrentHeight()), .guessed(17))
        
        self.nodeCheckState {
            switch $0 {
            case .download:
                return true
            default:
                return false
            }
        }
        
        self.nodeCheckState {
            switch $0 {
            case .synched:
                return true
            default:
                return false
            }
        }
        
        XCTAssertEqual(self.nodeDelegate.rollbackEvents.popLast(), 17)
        XCTAssert(self.nodeDelegate.rollbackEvents.isEmpty)
        
        self.nodeStop()
    }

    func testNodeNetworkedStateRollbackSubsetError() {
        let (bh, _, _) = Self.dbWriteTest20FullySynched(eventLoop: self.elg.next())
        
        self.nodeStartDownload()
        
        XCTAssertEqual(self.client.queueDepth, 1)
        XCTAssert(self.nodeReadPending().bitcoinCommandValue is GetheadersCommand)

        let wireHeaders: [BlockHeader] = zip(bh.suffix(5), bh.suffix(6).dropFirst())
            .map { lhs, rhs in
                var copy = lhs
                try! copy.addPreviousBlockHash(rhs.blockHash())
                try! copy.wire()
                return copy
            }

        self.nodeWritePendingCommand(HeadersCommand(headers: wireHeaders))

        self.nodeCheckState {
            switch $0 {
            case .network:
                return true
            default:
                return false
            }
        }

        self.nodeCheckState {
            switch $0 {
            case .download:
                return true
            default:
                return false
            }
        }

        XCTAssert(self.nodeReadPending().bitcoinCommandValue is GetheadersCommand)
        self.nodeWritePendingCommand(HeadersCommand(headers: []))
        XCTAssertEqual(self.waitNow(self.node._testingCurrentHeight()), .guessed(1))
        
        self.nodeCheckState {
            switch $0 {
            case .download:
                return true
            default:
                return false
            }
        }
        
        XCTAssert(self.nodeDelegate.rollbackEvents.isEmpty)
        
        self.nodeStop()
    }
    
    func testNodeBlockHeaderDownload() {
        let (bhData, _, _) = Self.dbWriteTest20FullySynched(eventLoop: self.elg.next())
        guard let last = bhData.last else {
            XCTFail()
            return
        }
        let subsequent1 = Self.nextHeader(last)
        let subsequent2 = Self.nextHeader(subsequent1.full)
        let subsequent3 = Self.nextHeader(subsequent2.full)
        let sendHeaders = [ subsequent1.wire, subsequent2.wire, subsequent3.wire ]
        var lastSerial = subsequent3.full
        XCTAssertNoThrow(try lastSerial.serial())

        self.nodeStartDownload()
        
        XCTAssert(self.nodeReadPending().bitcoinCommandValue is GetheadersCommand)
        self.nodeWritePendingCommand(HeadersCommand(headers: sendHeaders))
        
        Thread.sleep(forTimeInterval: 0.3)
        self.delay()
        
        XCTAssert(self.nodeReadPending().bitcoinCommandValue is GetheadersCommand)
        self.nodeWritePendingCommand(HeadersCommand(headers: []))
        self.nodeCheckState {
            switch $0 {
            case .network:
                return true
            default:
                return false
            }
        }

        self.nodeStop()
        self.nodeCheckState {
            switch $0 {
            case .initialized:
                return true
            default:
                return false
            }
        }
        
        // Need to reinitialize threadPool which is correctly stopped by Node
        self.threadPool = NIOThreadPool(numberOfThreads: 1)
        self.threadPool.start()
        let repo = TestingHelpers.DB.getFileBlockHeaderRepo(offset: 0, threadPool: self.threadPool, eventLoop: self.embedded)

        var serial1 = subsequent1.full
        XCTAssertNoThrow(try serial1.serial())
        var serial2 = subsequent2.full
        XCTAssertNoThrow(try serial2.serial())
        var serial3 = subsequent3.full
        XCTAssertNoThrow(try serial3.serial())
        XCTAssertNoThrow(
            XCTAssertEqual(serial1, try repo.find(id: 20).wait())
        )
        XCTAssertNoThrow(
            XCTAssertEqual(serial2, try repo.find(id: 21).wait())
        )
        XCTAssertNoThrow(
            XCTAssertEqual(serial3, try repo.find(id: 22).wait())
        )
        XCTAssertThrowsError(
            try repo.find(id: 23).wait()
        )
        
        XCTAssertNoThrow(try repo.close().wait())
    }
    
    func testSendTransaction() {
        Self.dbWriteTest20FullySynched(eventLoop: self.elg.next())
        
        let networkState = self.nodeSetNetworkState()
        
        let input: Tx.In = .init(outpoint: Tx.Outpoint(transactionId: .makeHash(from: [ 1, 2, 3, ]), index: 1),
                                 scriptSig: [ 1, 2, 3, ],
                                 sequence: .locktimeOnly, witness: {
                                    .init(witnessField: [[1, 2, 3], [1, 2, 3]])
                                 })
        let output: Tx.Out = .init(value: 100, scriptPubKey: [ 1, 2, 3, ])
        let txInit = Tx.Transaction.Initialize.wireWitness(version: 2,
                                                           vin: [ input ],
                                                           vout: [ output ],
                                                           locktime: .enable(1000))
        var tx = Tx.Transaction(txInit)
        XCTAssertNoThrow(try tx.hash())
        let txId = try! tx.getTxId()
        let sendFuture = self.node.sendTransaction(tx)
        
        guard let inv = self.nodeReadImmediate() as? InvCommand
        else {
            XCTFail()
            return
        }
        XCTAssertEqual(inv.inventory, [ .tx(txId) ])
        XCTAssertNoThrow(XCTAssertEqual(try sendFuture.wait(), txId))
        
        TestingHelpers.Node.closeNetworkState(networkState, eventLoop: self.embedded)
    }
    
    func testReceiveReject() {
        Self.dbWriteTest20FullySynched(eventLoop: self.elg.next())
        
        let networkState = self.nodeSetNetworkState()
        
        let rejectCommand = RejectCommand(message: "command",
                                          ccode: .nonstandard,
                                          reason: "no such command command",
                                          hash: .big((0..<32).map(UInt8.init)))
        self.nodeWriteUnsolicitedCommand(.reject(rejectCommand))
        self.tick()
        
        let stateFuture = self.node._testingState()
        let result = self.waitNow(stateFuture)
        guard let muxer = result.muxer
        else {
            XCTFail()
            return
        }
        XCTAssertEqual(muxer.count(), 0)

        TestingHelpers.Node.closeNetworkState(networkState, eventLoop: self.embedded)
    }
}

// MARK: Node extensions
extension NodeTests {
    func nodeCloseSuccessCleanupClosure(_ void: ()) -> Void {
        self.fileHandle = nil
        self.threadPool = nil
        self.client = nil
        self.muxer = nil
    }
    
    func nodeGetState() -> Node.ReactorState {
        XCTAssertNotNil(self.node)
        XCTAssertNotNil(self.embedded)
        
        let getStateFuture = DispatchQueue.main.sync { self.node._testingState() }
        self.tick()
        self.tick()

        do {
            return try getStateFuture.wait()
        } catch {
            preconditionFailure("\(error)")
        }
    }

    func nodeCheckState(_ state: @escaping (Node.ReactorState) -> Bool) {
        let nextState = expectation(description: "nextState")
        var finished = false
        
        func checkState() {
            DispatchQueue.global().asyncAfter(deadline: .now() + .milliseconds(100)) {
                finished = state(
                    self.nodeGetState()
                )
                if finished {
                    nextState.fulfill()
                } else {
                    checkState()
                }
            }
        }

        checkState()
        wait(for: [nextState], timeout: 2.5)
    }
    
    @discardableResult
    func nodeSetNetworkState(pubKeys: [ScriptPubKey] = [], outpoints: [Tx.Outpoint] = []) -> Node.NetworkState {
        let networkState = TestingHelpers.Node.createNetworkState(offset: 0,
                                                                    muxer: self.muxer,
                                                                    cfMatcher: self.cfMatcher,
                                                                    eventLoop: self.embedded,
                                                                    threadPool: self.threadPool,
                                                                    nioFileHandle: self.fileHandle)
        XCTAssertNoThrow(try self.node.addScriptPubKeys(pubKeys).wait())
        XCTAssertNoThrow(try self.node.addOutpoints(outpoints).wait())
        
        let setStateFuture = self.node._testingSetState(.network(networkState))
        self.embedded.advanceTime(by: .nanoseconds(2))
        XCTAssertNoThrow(try setStateFuture.wait())
        
        return networkState
    }

    func nodeReadPending(line: Int = #line) -> PendingBitcoinCommand {
        XCTAssertNotNil(self.channel)
        guard let outbound = try? self.channel?.readOutbound(as: QueueHandler.OutboundOut.self),
            case .pending(let command) = outbound else {
            preconditionFailure("pending command expected (line: \(line))")
        }

        return command
    }
    
    func nodeReadImmediate() -> BitcoinCommandValue {
        XCTAssertNotNil(self.channel)
        guard let outbound = try? self.channel?.readOutbound(as: QueueHandler.OutboundOut.self),
            case .immediate(let command) = outbound else {
            preconditionFailure("pending command expected")
        }

        return command
    }
    
    @discardableResult
    func nodeStartDownload(pubKeys: [ScriptPubKey] = [], outpoints: [Tx.Outpoint] = []) -> Node.NetworkState {
        XCTAssertNotNil(self.node)
        let startFuture = self.node.start()
        self.embedded.run()
        XCTAssertNoThrow(try startFuture.wait())
        let networkState = self.nodeSetNetworkState(pubKeys: pubKeys, outpoints: outpoints)
        self.tick()
        self.nodeCheckState {
            switch $0 {
            case .download:
                return true
            default:
                return false
            }
        }
        self.tick()
        
        return networkState
    }
    
    func nodeStop() {
        XCTAssertNotNil(self.node)
        let stopFuture = self.node.stop()
        
        self.nodeCheckState {
            switch $0 {
            case .initialized:
                return true
            default:
                return false
            }
        }

        XCTAssertNoThrow(try stopFuture.wait())
        stopFuture.whenSuccess(self.nodeCloseSuccessCleanupClosure(_:))
    }
    
    func nodeWriteIncomingCommand(_ cmd: FrameToCommandHandler.IncomingCommand) {
        XCTAssertNotNil(self.channel)
        do {
            try self.channel?.writeInbound(FrameToCommandHandler.InboundOut.incoming(cmd))
        } catch {
            preconditionFailure(error.localizedDescription)
        }
    }
    
    func nodeWritePendingCommand<T: BitcoinCommandValue>(_ cmd: T) {
        XCTAssertNotNil(self.channel)
        do {
            try self.channel?.writeInbound(FrameToCommandHandler.InboundOut.pending(cmd))
        } catch {
            preconditionFailure(error.localizedDescription)
        }
    }
    
    func nodeWriteUnsolicitedCommand(_ cmd: FrameToCommandHandler.UnsolicitedCommand) {
        XCTAssertNotNil(self.channel)
        do {
            try self.channel?.writeInbound(FrameToCommandHandler.InboundOut.unsolicited(cmd))
        } catch {
            preconditionFailure(error.localizedDescription)
        }
    }
    
    func waitNow<T>(_ future: Future<T>) -> T {
        self.embedded.run()
        
        var result: T! = nil
        XCTAssertNoThrow(result = try future.wait())
        return result
    }
    
    func waitRepoWrite<T, R: FileRepo>(_ future: Future<T>, repo: R) -> T {
        var preCount: Int = .max
        XCTAssertNoThrow(preCount = try repo.count().wait())
        Thread.sleep(forTimeInterval: 0.02)
        XCTAssertNoThrow(XCTAssertGreaterThan(try repo.count().wait(), preCount))
        return self.waitNow(future)
    }
    
    func waitTick<T>(_ future: Future<T>) -> T {
        self.embedded.advanceTime(by: Settings.NODE_REPEATING_TASK_DELAY)

        var result: T! = nil
        XCTAssertNoThrow(result = try future.wait())
        return result
    }
    
    func tick() {
        self.embedded.advanceTime(by: Settings.NODE_REPEATING_TASK_DELAY)
    }
    
    func delay() {
        self.embedded.advanceTime(by: Settings.NODE_SLEEP_DELAY + Settings.NODE_REPEATING_TASK_DELAY)
    }
}

// MARK: DB extensions
extension NodeTests {
    @discardableResult
    static func dbWriteTest20(eventLoop: EventLoop) -> (
        fakeBlockHeaders: [BlockHeader],
        fakeWireCompactFilters: [CF.Header],
        fakeSerialCompactFilters: [CF.SerialHeader]
    ) {
        let bh = TestingHelpers.DB.writeFakeBlockHeaders(count: 20, eventLoop: eventLoop)
        let (wire, serial) = TestingHelpers.DB.writeFakeCompactFilters(count: 15, processed: 10, eventLoop: eventLoop)
        return (bh, wire, serial)
    }
    
    @discardableResult
    static func dbWriteTest20FullySynched(eventLoop: EventLoop) -> (
        fakeBlockHeaders: [BlockHeader],
        fakeWireCompactFilters: [CF.Header],
        fakeSerialCompactFilters: [CF.SerialHeader]
    ) {
        let bh = TestingHelpers.DB.writeFakeBlockHeaders(count: 20, eventLoop: eventLoop)
        let (wire, serial) = TestingHelpers.DB.writeFakeCompactFilters(count: 20, processed: 20, eventLoop: eventLoop)
        sync()
        return (bh, wire, serial)
    }

    func makeBhRepo(offset: Int = 0) -> FileBlockHeaderRepo {
        FileBlockHeaderRepo(eventLoop: self.embedded,
                            nonBlockingFileIO: self.nonBlockingFileIODependency(self.threadPool),
                            nioFileHandle: try! self.fileHandle.duplicate(),
                            offset: offset)
    }
    
    func makeCfRepo(offset: Int = 0) -> FileCFHeaderRepo {
        FileCFHeaderRepo(eventLoop: self.embedded,
                         nonBlockingFileIO: self.nonBlockingFileIODependency(self.threadPool),
                         nioFileHandle: try! self.fileHandle.duplicate(),
                         offset: offset)
    }

    static func nextHeader(_ header: BlockHeader) -> (wire: BlockHeader, full: BlockHeader) {
        let prevHash = try! header.blockHash()
        
        let wire = BlockHeader(
            .wire(version: 1,
                  previousBlockHash: prevHash,
                  merkleRoot: .zero,
                  timestamp: UInt32(Date().timeIntervalSince1970),
                  difficulty: 1,
                  nonce: UInt32.random(in: .min ... .max),
                  transactions: 0)
        )
        
        var full = wire
        try! full.addBlockHash()
        try! full.addHeight(header.id + 1)
        return (wire, full)
    }
    
    static func nextHeaders(count: Int, last: BlockHeader) -> (wire: [BlockHeader], full: [BlockHeader]) {
        var last = last
        let next: [(wire: BlockHeader, full: BlockHeader)] = (0..<count).map { _ in
            let next = self.nextHeader(last)
            last = next.full
            return next
        }
        
        let wire = next.map(\.wire)
        let full = next.map(\.full)

        return (wire, full)
    }

    
    static func nextHeader(_ wire: CF.Header) -> CF.Header {
        let prevHash = wire.headerHash
        let filterHash = wire.filterHash
        let headerHash: BlockChain.Hash<CompactFilterHeaderHash> = .makeHash(from: [filterHash.littleEndian, prevHash.littleEndian].joined())
        
        return CF.Header(filterHash: filterHash, previousHeaderHash: prevHash, headerHash: headerHash)
    }
    
    func latestBlockLocator() -> BlockLocator {
        let bhRepo = TestingHelpers.DB.getFileBlockHeaderRepo(offset: 0, threadPool: self.threadPool, eventLoop: self.embedded)
        guard let latestBlockLocator = try? bhRepo.latestBlockLocator().wait() else {
            preconditionFailure()
        }
        let closeFuture = bhRepo.close()
        self.embedded.run()
        XCTAssertNoThrow(try closeFuture.wait())
        return latestBlockLocator
    }
    
    func repoClose<R: FileRepo>(_ repo: R) {
        let close = repo.close()
        self.embedded.run()
        XCTAssertNoThrow(try close.wait())
    }
    
    func deferClose(bhRepo: FileBlockHeaderRepo, cfHeaderRepo: FileCFHeaderRepo) {
        let bhClose = bhRepo.close()
        let cfHeaderClose = cfHeaderRepo.close()
        self.embedded.run()
        XCTAssertNoThrow(try bhClose.and(cfHeaderClose).wait())
    }
}
