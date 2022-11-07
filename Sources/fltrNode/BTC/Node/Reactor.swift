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
import enum FileRepo.File
import fltrTx
import fltrWAPI
import HaByLo
import NIO
import NIOTransportServices
import NIOConcurrencyHelpers

extension Node {
    final class Reactor {
        private let eventLoop: EventLoop
        private let eventLoopGroup: NIOTSEventLoopGroup

        fileprivate var _outpoints: Outpoints = .init()
        fileprivate var _pubKeys: Set<ScriptPubKey> = .init()
        
        let mempoolLock: NIOConcurrencyHelpers.NIOLock = .init()
        fileprivate var _mempool: Node.Mempool = [:]
        
        let threadPool: NIOThreadPool
        
        var stopRequested: Promise<Void>?
        var muxerStopped: Promise<Void>?
        
        var syncHandler: SyncHandler
        
        private var state: ReactorState
        private var chainHeight: ConsensusChainHeight
        var currentHeight: ConsensusChainHeight.CurrentHeight {
            self.chainHeight.current
        }
        
        let nodeDelegate: NodeDelegate

        @Setting(\.BITCOIN_HEADERS_MIN_NUMBER) private var numberOfRemainingHeaders: Int
        @Setting(\.MUXER_CLIENT_SELECTION) private var muxerClientSelection: PeerAddressSelection
        @Setting(\.NODE_REPEATING_TASK_DELAY) private var repeatingTaskDelay: TimeAmount
        @Setting(\.NODE_ROLLBACK_DELAY) private var nodeRollbackDelay: TimeAmount
        @Setting(\.NODE_SLEEP_DELAY) private var nodeSleepDelay: TimeAmount
        @Setting(\.NODE_PROPERTIES) private var nodeProperties: NodeProperties
        
        init(eventLoop: EventLoop,
             eventLoopGroup: NIOTSEventLoopGroup,
             threadPool: NIOThreadPool,
             nodeDelegate: NodeDelegate) {
            self.eventLoop = eventLoop
            self.eventLoopGroup = eventLoopGroup
            self.nodeDelegate = nodeDelegate
            self.threadPool = threadPool
            
            self.state = .initialized
            
            self.chainHeight = .init() {
                nodeDelegate.estimatedHeight($0)
            }
            
            self.syncHandler = SyncHandler {
                nodeDelegate.syncEvent($0)
            }
        }
    }
}

extension Node {
    enum ReactorState {
        case initialized
        case started
        
        enum FollowedBy {
            case network
            case synched([BlockHeader])
        }
        
        case network(Node.NetworkState)
        case download(Node.DownloadState)
        case synched(Node.NetworkState, ConsensusChainHeight.CurrentHeight, NIODeadline, [BlockHeader])
        
        var backlog: [BlockHeader]? {
            switch self {
            case .download(let downloadState):
                return downloadState.backlog()
            case .synched(_, _, _, let backlog):
                return backlog
            case .initialized, .started, .network:
                return nil
            }
        }
        
        func checkOpen() {
            switch self {
            case .download(let state as NetworkStateProperties),
                 .network(let state as NetworkStateProperties),
                 .synched(let state as NetworkStateProperties, _, _, _):
                assert(state.openFile.isOpen)
                assert(state.bhRepo.nioFileHandle.isOpen)
                assert(state.cfHeaderRepo.nioFileHandle.isOpen)
            case .initialized,
                 .started:
                break
            }
        }
        
        var brief: String {
            switch self {
            case .download:
                return ".download"
            case .initialized:
                return ".initialized"
            case .network:
                return ".network"
            case .started:
                return ".started"
            case .synched:
                return ".synched"
            }
        }

        var muxer: Muxer? {
            switch self {
            case .download(let state):
                return state.muxer
            case .network(let state),
                 .synched(let state, _, _, _):
                return state.muxer
            case .initialized, .started:
                return nil
            }
        }
        
        var repos: (bh: FileBlockHeaderRepo, cfHeader: FileCFHeaderRepo)? {
            switch self {
            case .download(let state):
                return (state.bhRepo, state.cfHeaderRepo)
            case .network(let state),
                .synched(let state, _, _, _):
                return (state.bhRepo, state.cfHeaderRepo)
            case .initialized, .started:
                return nil
            }
        }
    }
}

extension Node.Reactor {
    // MARK: Node.Reactor Repeated Task *** FIRE ***
    func fire(_ task: RepeatedTask) -> Future<Void> {
#if DEBUG
        self.state.checkOpen()
#endif
        logger.trace("Node.Reactor - fire Called with current state \(self.state.brief)")
        let voidFuture: Future<Void>

        switch self.state {

        case .initialized:
            preconditionFailure("illegal state for fire, should have reached .started by now")
            
        case .started:
            let clientFactory = Settings.CLIENT_FACTORY(self.eventLoopGroup,
                                                        self.threadPool) {
                self.chainHeight.current.value
            }
            
            voidFuture = Node.NetworkState.createNetworkStateFuture(offset: self.offset(),
                                                                    eventLoop: self.eventLoop,
                                                                    threadPool: threadPool,
                                                                    muxerDelegate: self,
                                                                    currentPubKeys: self._pubKeys,
                                                                    clientFactory: clientFactory)
            .map { networkState in
                    self.state = .network(networkState)
            }

        case .network(let networkState):
            if Node.nodeNetworkIsReady(muxer: networkState.muxer,
                                       minimum: (Settings.BITCOIN_NUMBER_OF_CLIENTS + 1) / 2) {
                voidFuture = networkState.startDownload(currentHeight: self.chainHeight.current,
                                                        outpoints: {
                                                            self.eventLoop.submit {
                                                                self.outpoints
                                                            }
                                                        },
                                                        pubKeys: {
                                                            self.eventLoop.submit {
                                                                self.pubKeys
                                                            }
                                                        },
                                                        nodeDelegate: self.nodeDelegate,
                                                        eventLoop: self.eventLoop)
                .map { outcome in
                    switch outcome {
                    case .download(let downloadState):
                        self.state = .download(downloadState)
                        self.syncHandler.tracking()
                    case .synched(let backlog):
                        self.state = .synched(networkState, self.chainHeight.current,
                                              .now() + .minutes(2), backlog)
                        self.syncHandler.sync()
                    }
                }
            } else {
                voidFuture = self.delay
            }
            
        case .download(let downloadState):
            func setNetworkState() {
                self.state = .network(Node.NetworkState(downloadState))
            }

            voidFuture = downloadState.download(eventLoop: self.eventLoop).flatMap {
                if let outcomes = $0 {
                    defer { setNetworkState() }
                    
                    if let (blockHeader, clientKey) = outcomes.synch {
                        self.chainHeight.sync(to: blockHeader, from: clientKey)
                    }
                    
                    if outcomes.drop {
                        downloadState.muxer.dropForDiversity()
                    }
                    
                    if let rollbackHeight = outcomes.rollback {
                        return self.doRollback(height: rollbackHeight, bhRepo: downloadState.bhRepo)
                    }
                }
                
                return self.delay
            }
            
        case .synched(let networkState, let height, let deadline, let backlog):
            switch (self.chainHeight.current, NIODeadline.now()) {
            case (.synced, let now) where self.chainHeight.current == height && now > deadline:
                voidFuture = self.synchedStateHandler(networkState: networkState,
                                                      height: height,
                                                      now: now,
                                                      backlog: backlog)
            case (.synced, _) where self.chainHeight.current != height,
                 (.guessed, _),
                 (.unknown, _):
                self.chainHeight.reset()
                self.state = .network(networkState)
                voidFuture = self.eventLoop.makeSucceededVoidFuture()
            case (.synced, _):
                assert(self.chainHeight.current == height)
                voidFuture = self.delay
            }
        }
        
        return voidFuture
        .flatMapError {
            switch $0 {
            case File.Error.seekError:
                switch self.chainHeight.current {
                case .synced:
                    preconditionFailure()
                case .guessed, .unknown:
                    break
                }
            default:
                logger.error("Node.Reactor", #function, "- Fatal error occurred [\($0)]")
                preconditionFailure("\($0)")
            }
            
            return self.eventLoop.makeSucceededFuture(())
        }
        .map {
            self.stopIfRequested(task)
        }
    }
    
    private func doRollback(height: Int,
                            bhRepo: FileBlockHeaderRepo) -> Future<Void> {
        self.chainHeight.rollback(height)
        return self.rollbackDelay
        .flatMap {
            Node.nodeDelegateWith(timeout: .now() + Settings.BITCOIN_WALLET_DELEGATE_COMMIT_TIMEOUT,
                                  eventLoop: self.eventLoop) {
                self.nodeDelegate.rollback(height: height, commit: $0)
            }
        }
        .recover { error in
            preconditionFailure("Node Delegate error \(error)")
        }
        .flatMap {
            bhRepo.delete(from: height)
        }
        .flatMapError { error in
            switch error {
            case File.Error.seekError: // rollback below minimum
                return bhRepo.range().map(\.lowerBound)
                .flatMap {
                    self.doRollback(height: $0 + 2, bhRepo: bhRepo)
                }
            default:
                preconditionFailure("BlockHeader repo rollback error \(error)")
            }
        }
        .always { _ in
            self.nodeProperties.verifyCheckpoint = height
            self.nodeProperties.processedIndex = 0
        }
        .flatMap {
            return self.rollbackDelay
        }
    }
    
    private func synchedStateHandler(networkState: Node.NetworkState,
                                     height: ConsensusChainHeight.CurrentHeight,
                                     now: NIODeadline = NIODeadline.now(),
                                     backlog: [BlockHeader]) -> Future<Void> {
        let promise = self.eventLoop.makePromise(of: NodeDownloaderOutcome.self)
        
        let scheduled = self.eventLoop.scheduleTask(in: .seconds(3)) {
            promise.succeed(.quit)
        }
        
        networkState.bhRepo.latestBlockLocator()
        .flatMap { locator -> Future<NodeDownloaderOutcome> in
            let downloader = Node.BlockHeaderDownload(count: 1,
                                                      locator: locator,
                                                      backlog: backlog,
                                                      maxHeadersOverwrite: Settings.BITCOIN_MAX_HEADERS_OVERWRITE,
                                                      bhRepo: networkState.bhRepo,
                                                      muxer: networkState.muxer,
                                                      nodeDelegate: self.nodeDelegate,
                                                      threadPool: networkState.threadPool,
                                                      eventLoop: self.eventLoop)
            return downloader.start()
            .always { _ in downloader.cancel() }
        }
        .recover { _ in .quit }
        .whenSuccess {
            scheduled.cancel()
            promise.succeed($0)
        }

        return promise.futureResult
        .map {
            switch $0 {
            case .synch:
                self.state = .synched(networkState,
                                      height,
                                      now + .minutes(2),
                                      backlog)
            case .drop, .processed, .rollback, .quit, .first:
                self.chainHeight.reset()
                self.state = .network(networkState)
            }
        }
    }
    
    private func delay(for timeAmount: TimeAmount) -> Future<Void> {
        self.eventLoop
            .scheduleTask(in: timeAmount, {})
            .futureResult
    }
    
    private var delay: Future<Void> {
        self.delay(for: self.nodeSleepDelay)
    }
    
    private var rollbackDelay: Future<Void> {
        let trySync = self.threadPool.runIfActive(eventLoop: self.eventLoop) {
            #if DEBUG // Unit tests don't like a full sync
            ()
            #else
            sync()
            #endif
        }
        
        return trySync.flatMap {
            self.delay(for: self.nodeRollbackDelay)
        }
    }

    private func offset() -> Int {
        self.nodeProperties.offset
    }
    
    private func setOffset(_ newValue: Int) {
        self.nodeProperties.offset = newValue
    }
    
    private func hosts() -> Set<PeerAddress> {
        self.nodeProperties.peers
    }
    
    private func setHosts(_ newValue: Set<PeerAddress>) {
        self.nodeProperties.peers = newValue
    }
    
    func start() throws {
        self.eventLoop.assertInEventLoop()
        switch self.state {
        case .initialized:
            self.eventLoop.scheduleRepeatedAsyncTask(initialDelay: .nanoseconds(2),
                                                     delay: self.repeatingTaskDelay) { task in
                self.fire(task)
            }
            self.state = .started
        case .started, .network, .download, .synched:
            throw invalidState(self.state)
        }
    }
    
    // MARK: STOP
    func stop(_ promise: Promise<Void>? = nil) {
        self.eventLoop.assertInEventLoop()

        let muxer: Muxer? = {
            switch self.state {
            case .download(let state):
                return state.muxer
            case .network(let state):
                return state.muxer
            case .synched(let state, _, _, _):
                return state.muxer
            case .initialized, .started:
                return nil
            }
        }()
        
        switch self.state {
        case .initialized:
            promise?.succeed(())
        case .started, .network, .download, .synched:
            if let alreadyStopping = self.stopRequested {
                promise?.fail(Node.Error.closeAlreadyCalled(currentFuture: alreadyStopping.futureResult))
                return
            }

            self.stopRequested = promise ?? self.eventLoop.makePromise()

            let timer = self.eventLoop.scheduleTask(in: .milliseconds(500)) {
                if self.muxerStopped == nil {
                    self.muxerStopped = self.eventLoop.makePromise()
                    muxer?.stop()
                }
            }

            self.stopRequested!.futureResult
            .whenComplete { _ in
                timer.cancel()
            }
        }
    }
    
    private func stateAwareStop() -> Future<Void> {
        func close<NetworkState: NetworkStateProperties>(networkState: NetworkState) -> Future<Void> {
            networkState.cfMatcher.stop()
            
            let bhClose = self.delay(for: .milliseconds(200))
                .flatMap { networkState.bhRepo.close() }
            let cfClose = self.delay(for: .milliseconds(300))
                .flatMap { networkState.cfHeaderRepo.close() }
            let openFileClose = self.delay(for: .milliseconds(400))
            .flatMap {
                Node.closeHelper(openFileHandle: networkState.openFile,
                                 eventLoop: self.eventLoop)
            }
            
            let muxerStopped: Promise<Void> = {
                if let muxerStopped = self.muxerStopped {
                    return muxerStopped
                } else {
                    let muxerStopped = self.eventLoop.makePromise(of: Void.self)
                    self.muxerStopped = muxerStopped
                    networkState.muxer.stop()
                    return muxerStopped
                }
            }()
            
            return Future.whenAllComplete([bhClose,
                                           cfClose,
                                           openFileClose,
                                           muxerStopped.futureResult, ],
                                          on: self.eventLoop)
            .always {
                var errors: [String] = []
                for (index, f) in try! $0.get().enumerated() {
                    switch f {
                    case .failure(let error):
                        switch index {
                        case 0:
                            errors.append("\nBlockHeader Repo Close failed[")
                        case 1:
                            errors.append("\nCompactFilter Header Repo Close failed [")
                        case 2:
                            errors.append("\nNIOFileHandle(NetworkState.openFile) close failed [")
                        case 3:
                            errors.append("\nMuxer close failed [")
                        default:
                            preconditionFailure()
                        }

                        errors.append("\(error)]")
                    case .success: break
                    }
                }
                
                if errors.isEmpty {
                    logger.info("Node.Reactor - 🚦Stop Successful ✅")
                } else {
                    logger.error("Node.Reactor - 🚦Stop FAIL ❌", errors.joined())
                }
            }
            .map { _ in () }
            .always { _ in
                self.mempoolLock.withLockVoid {
                    self._mempool.removeAll()
                }
            }
        }
        
        self.chainHeight.reset()
        switch self.state {
        case .initialized:
            preconditionFailure("illegal state .initialized for calling stateAwareStop")
        case .started:
            self.state = .initialized
            return eventLoop.makeSucceededFuture(())
        case .download(let downloadState):
            downloadState.canceller()
            return close(networkState: downloadState)
        case .network(let networkState),
             .synched(let networkState, _, _, _):
            return close(networkState: networkState)
        }
    }

    private func stopIfRequested(_ task: RepeatedTask) {
        guard let stopPromise = self.stopRequested else {
            return
        }
        
        let cancelPromise = self.eventLoop.makePromise(of: Void.self)
        task.cancel(promise: cancelPromise)
        
        let stopped = self.stateAwareStop()

        Future.whenAllComplete([stopped, cancelPromise.futureResult], on: self.eventLoop)
        .whenSuccess {
            switch ($0[0], $0[1]) {
            case (.success, .success):
                stopPromise.succeed(())
            case (.success, .failure(let error)),
                 (.failure(let error), .success),
                 (.failure, .failure(let error)):
                stopPromise.fail(error)
            }
            
            self.muxerStopped = nil
            self.stopRequested = nil
            self.state = .initialized
        }
    }
    
    func invalidState(_ state: Node.ReactorState, event: StaticString = #function) -> Node.Error {
        return .invalidState(String(describing: state), event)
    }
    
    func newBlockHeader(_ header: BlockHeader, from client: Client.Key) {
        self.eventLoop.assertInEventLoop()
        
        let backlog = self.state.backlog
        
        if let alreadyFound = backlog?.map({
            try! $0.blockHash()
        }).contains(try! header.blockHash()) {
            guard !alreadyFound else {
                return
            }
        }
        
        if let last = backlog?.last, try! last.blockHash() == header.previousBlockHash() {
            var header = header
            try! header.addHeight(last.height() + 1)
            try! header.serial()
            self.chainHeight.sync(to: header, from: client)
        } else {
            self.chainHeight.newHeader()
        }
    }
    
    func addOutpoints(_ newOutpoints: [Tx.Outpoint]) {
        self.eventLoop.assertInEventLoop()

        self._outpoints.formUnion(newOutpoints)
    }
    
    func removeOutpoint(_ outpoint: Tx.Outpoint) {
        self.eventLoop.assertInEventLoop()

        guard let _ = self._outpoints.remove(outpoint)
        else {
            logger.error("Remove Outpoint called for outpoint \(outpoint) which was not previously added")
            return
        }
    }
    
    var outpoints: Outpoints {
        self.eventLoop.assertInEventLoop()
        
        return self._outpoints
    }
    
    func addScriptPubKeys(_ add: [ScriptPubKey]) {
        self.eventLoop.assertInEventLoop()

        self._pubKeys.formUnion(add)
        
        let cfMatcher: CF.MatcherClient? = {
            switch self.state {
            case .network(let networkState as NetworkStateProperties),
                 .download(let networkState as NetworkStateProperties),
                 .synched(let networkState as NetworkStateProperties, _, _, _):
                return networkState.cfMatcher
            case .initialized, .started:
                return nil
            }
        }()
        
        cfMatcher?.updateOpcodes(
            self._pubKeys.map(\.opcodes)
        )
    }
    
    var pubKeys: Set<ScriptPubKey> {
        self.eventLoop.assertInEventLoop()
        
        return self._pubKeys
    }
    
    func addTransaction<T: TransactionProtocol>(_ tx: T) throws -> Tx.TxId {
        var tx = Tx.Transaction(
            tx.hasWitnesses
            ? .wireWitness(version: tx.version,
                           vin: tx.vin,
                           vout: tx.vout,
                           locktime: tx.locktime)
            : .wireLegacy(version: tx.version,
                          vin: tx.vin,
                          vout: tx.vout,
                          locktime: tx.locktime)
        )

        try tx.hash()
        try tx.validate()
        
        let txId = try tx.getTxId()

        self.mempoolLock.withLockVoid {
            self._mempool[txId] = tx
        }

        logger.trace("Reactor - \(#function) adding transaction with txId [\(try! tx.getTxId())]\n"
                    + String(reflecting: tx))
        return txId
    }
    
    func sendTransaction<T: TransactionProtocol>(_ tx: T) -> Future<Tx.TxId> {
        self.eventLoop.assertInEventLoop()

        var txId: Tx.TxId!
        do {
            txId = try self.addTransaction(tx)
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
        
        guard let muxer = self.state.muxer
        else {
            return self.eventLoop.makeFailedFuture(Muxer.Error.noActiveConnectionsFound)
        }
        
        return muxer.allFuture(eventLoop: self.eventLoop) {
            $0.immediate(InvCommand(inventory: [.tx(txId)]))
        }
        .flatMapThrowing {
            let (foundAnyWorking, errorOptional) = $0.reduce((false, Optional<Swift.Error>.none)) {
                do {
                    try $1.result.get()
                    return (true, $0.1)
                } catch {
                    return ($0.0, $0.1 ?? error)
                }
            }
            
            if foundAnyWorking {
                return txId
            } else {
                throw errorOptional
                ?? Muxer.Error.noActiveConnectionsFound
            }
        }
    }
    
    func removeTransaction(_ txId: Tx.TxId) -> Tx.Transaction? {
        self.mempoolLock.withLock {
            self._mempool.removeValue(forKey: txId)
        }
    }
    
    var mempool: Node.Mempool {
        self.mempoolLock.withLock {
            self._mempool
        }
    }
    
    func blockHeaderLookup(for height: Int) -> Future<BlockHeader?> {
        self.eventLoop.assertInEventLoop()
        
        guard let bhRepo = self.state.repos?.bh
        else {
            return self.eventLoop.makeSucceededFuture(nil)
        }
        
        return bhRepo.find(id: height)
        .map(BlockHeader?.init)
        .recover { _ in nil }
    }
    
    func _testingSetState(_ state: Node.ReactorState) {
        self.state = state
    }
    
    func _testingState() -> Node.ReactorState {
        self.state
    }
}

// MARK: MuxerDelegateProtocol conformance
extension Node.Reactor: MuxerDelegateProtocol {
    func muxerStopComplete() {
        self.muxerStopped?.succeed(())
    }
    
    func unsolicited(addr: [PeerAddress], source: Client.Key) {
        let filtered = addr.filter {
            $0.services.isFilterNode
        }
        
        guard !filtered.isEmpty
        else {
            return
        }
        
        self.eventLoop.execute {
            var hosts = self.hosts()
            let newPeers: [PeerAddress] = filtered.filter {
                !hosts.contains($0)
            }
            guard !newPeers.isEmpty
            else { return }
            
            hosts.formUnion(newPeers)
            self.setHosts(hosts)
        }
    }
    
    func unsolicited(block: BlockChain.Hash<BlockHeaderHash>, source: Client.Key) {
        self.eventLoop.submit { self.chainHeight.current }
        .whenComplete {
            switch $0 {
            case .success(let currentHeight):
                guard case .synced(_, let header) = currentHeight,
                      let headerHash = try? header.blockHash() else {
                    self.chainHeight.newHeader()
                    return
                }
                
                func logError(_ error: Swift.Error) {
                    logger.error("Node.Reactor", #function,
                                 "- Unsolicited BlockHeader failure "
                                    + "[\(error)] received "
                                    + "from [\(source.client)]")
                }
                
                source.client.outgoingGetheaders(locator: .init(hashes: [headerHash], stop: .zero))
                .whenComplete { result in
                    switch result {
                    case .success(let headersCommand):
                        guard var newBlockHeader = headersCommand.first
                        else {
                            logError(Node.HeadersError.illegalUnsolicitedHeadersReceivedEmpty)
                            return
                        }
                        
                        try! newBlockHeader.addBlockHash()
                        guard try! newBlockHeader.blockHash() == block else {
                            self.eventLoop.execute { self.chainHeight.reset() }
                            logError(Node.HeadersError.illegalUnsolicitedHeaders(hash: block))
                            return
                        }
                        
                        self.eventLoop.execute {
                            self.newBlockHeader(newBlockHeader, from: source)
                        }
                    case .failure(let error):
                        logger.error("Node.Reactor", #function,
                                     "- Unsolicited BlockHeader failure "
                                        + "[\(error.localizedDescription)] received "
                                        + "from [\(source.client)]")
                    }
                }
            case .failure(let error):
                preconditionFailure("\(error)")
            }
        }
    }
    
    func unsolicited(tx: BlockChain.Hash<TransactionLegacyHash>, source: Client.Key) {
        func matchTx(_ tx: Tx.Transaction,
                     outpoints: Outpoints,
                     pubKeys: Set<ScriptPubKey>) throws {
            var tx = tx
            try tx.hash()
            try tx.validate()
            
            let spent: [TransactionEvent] = tx.findSpent(for: outpoints)
            .compactMap { outpoint in
                guard let tx = Tx.AnyTransaction(tx)
                else { return nil }
                
                let outputs: [SpentOutpoint.TransactionOutputs] = tx.findOutputs(in: self._pubKeys)
                return SpentOutpoint(outpoint: outpoint,
                                     outputs: outputs,
                                     tx: tx)
            }
            .map(TransactionEvent.spent)
            let new: [TransactionEvent] = tx.findFunding(for: pubKeys).map(TransactionEvent.new)

            
            switch (spent.isEmpty, new.isEmpty) {
            case (false, false):
                logger.info("Node.Reactor", #function, "- Matching SPENT unconfirmed tx [\(String(reflecting: tx))] with refunding tx received from [\(source.client)]")
                self.nodeDelegate.unconfirmedTx(height: self.currentHeight.value,
                                                events: spent + new) // refunding transaction
            case (false, true):
                logger.info("Node.Reactor", #function, "- Matching SPENT unconfirmed tx [\(String(reflecting: tx))] received from [\(source.client)]")
                self.nodeDelegate.unconfirmedTx(height: self.currentHeight.value,
                                                events: spent)
            case (true, false):
                logger.info("Node.Reactor", #function, "- Matching RECEIVE unconfirmed tx [\(String(reflecting: tx))] received from [\(source.client)]")
                self.nodeDelegate.unconfirmedTx(height: self.currentHeight.value,
                                                events: new)
            case (true, true):
                logger.trace("Node.Reactor", #function, "- Mempool tx (non-matching) [\(String(reflecting: tx))] received from [\(source.client)]")
                break
            }
        }

        source.client.outgoingGetdataTx(tx)
        .hopMap(to: self.eventLoop, by: self.threadPool) {
            try matchTx($0, outpoints: self._outpoints, pubKeys: self._pubKeys)
        }
        .whenFailure { error in
            logger.error("Node.Reactor", #function,
                         "- Unconfirmed unsolicited Tx failure [\(error.localizedDescription)] received from [\(source.client)]")
        }
    }
    
    func guess(height: Int) {
        if self.eventLoop.inEventLoop {
            self.chainHeight.guessing(new: height)
        } else {
            self.eventLoop.execute {
                self.chainHeight.guessing(new: height)
            }
        }
    }
}
