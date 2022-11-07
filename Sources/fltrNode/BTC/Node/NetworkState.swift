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
import FileRepo
import fltrTx
import HaByLo
import NIO

protocol NetworkStateProperties {
    var threadPool: NIOThreadPool { get }
    var nonBlockingFileIO: NonBlockingFileIOClient { get }
    var bhRepo: FileBlockHeaderRepo { get }
    var cfHeaderRepo: FileCFHeaderRepo { get }
    var openFile: NIOFileHandle { get }
    var muxer: Muxer { get }
    var cfMatcher: CF.MatcherClient { get }
}

extension Node {
    struct NetworkState: NetworkStateProperties {
        let threadPool: NIOThreadPool
        let nonBlockingFileIO: NonBlockingFileIOClient
        let bhRepo: FileBlockHeaderRepo
        let cfHeaderRepo: FileCFHeaderRepo
        let openFile: NIOFileHandle
        let muxer: Muxer
        let cfMatcher: CF.MatcherClient
        
        @Setting(\.NODE_PROPERTIES) private var nodeProperties
        @Setting(\.BITCOIN_MAX_HEADERS_OVERWRITE) private var blockHeaderBacklogCount: Int
        @Setting(\.BITCOIN_BLOCKHEADER_MAX_DOWNLOAD_COUNT) private var blockHeaderMaxDownloadCount: Int
        @Setting(\.BITCOIN_CF_BUNDLE_SIZE) private var cfBundleSize: Int
        @Setting(\.BITCOIN_CF_CONCURRENT_INVOCATIONS) private var cfConcurrentInvocations: Int
        @Setting(\.BITCOIN_CF_DOWNLOAD_FILTER_COUNT) private var cfDownloadFilterCount: Int
        @Setting(\.BITCOIN_CF_HEADERS_MAX_STOPS) private var maxCfHeaderStops: Int
        @Setting(\.BITCOIN_CF_TIMEOUT) private var cfCommandTimeout: TimeAmount
        @Setting(\.BITCOIN_VERIFY_MIN_TRANSACTIONS) private var verifyMinimumFilters: Int
        @Setting(\.BITCOIN_COMMAND_FULLBLOCK_TIMEOUT) private var fullBlockCommandTimeout: TimeAmount
        @Setting(\.BITCOIN_FULL_BLOCK_DOWNLOADER_BATCH_SIZE) private var fullBlockDownloaderBatchSize: Int
        @Setting(\.PROTOCOL_MAX_GETHEADERS) private var maxHeaders: Int
        @Setting(\.NODE_VERIFY_RANDOM_STEP_RANGE) private var verifyRandomStepRange: Range<Int>

        init(threadPool: NIOThreadPool,
             nonBlockingFileIO: NonBlockingFileIOClient,
             bhRepo: FileBlockHeaderRepo,
             cfHeaderRepo: FileCFHeaderRepo,
             openFile: NIOFileHandle,
             muxer: Muxer,
             cfMatcher: CF.MatcherClient,
             blockHeaderBacklogCount: Int? = nil,
             cfBundleSize: Int? = nil,
             cfConcurrentInvocations: Int? = nil,
             cfDownloadFilterCount: Int? = nil,
             cfCommandTimeout: TimeAmount? = nil,
             fullBlockCommandTimeout: TimeAmount? = nil,
             maxCfHeaderStops: Int? = nil,
             maxHeaders: Int? = nil,
             verifyRandomStepRange: Range<Int>? = nil) {
            self.threadPool = threadPool
            self.nonBlockingFileIO = nonBlockingFileIO
            self.bhRepo = bhRepo
            self.cfHeaderRepo = cfHeaderRepo
            self.openFile = openFile
            self.muxer = muxer
            self.cfMatcher = cfMatcher
            
            blockHeaderBacklogCount.map {
                self.blockHeaderBacklogCount = $0
            }
            cfBundleSize.map {
                self.cfBundleSize = $0
            }
            cfConcurrentInvocations.map {
                self.cfConcurrentInvocations = $0
            }
            cfDownloadFilterCount.map {
                self.cfDownloadFilterCount = $0
            }
            cfCommandTimeout.map {
                self.cfCommandTimeout = $0
            }
            fullBlockCommandTimeout.map {
                self.fullBlockCommandTimeout = $0
            }
            maxCfHeaderStops.map {
                self.maxCfHeaderStops = $0
            }
            maxHeaders.map {
                self.maxHeaders = $0
            }
            verifyRandomStepRange.map {
                self.verifyRandomStepRange = $0
            }
        }

        init(_ downloadState: DownloadState) {
            self.threadPool = downloadState.threadPool
            self.nonBlockingFileIO = downloadState.nonBlockingFileIO
            self.bhRepo = downloadState.bhRepo
            self.cfHeaderRepo = downloadState.cfHeaderRepo
            self.openFile = downloadState.openFile
            self.muxer = downloadState.muxer
            self.cfMatcher = downloadState.cfMatcher
        }
    }
}

extension Node.NetworkState {
    enum Outcome {
        case download(Node.DownloadState)
        case synched([BlockHeader])
    }

    typealias Downloaders = [(String, NodeDownloaderProtocol?)]

    func startDownload(currentHeight: ConsensusChainHeight.CurrentHeight,
                       outpoints: @escaping () -> Future<Outpoints>,
                       pubKeys: @escaping () -> Future<Set<ScriptPubKey>>,
                       nodeDelegate: NodeDelegate,
                       eventLoop: EventLoop) -> Future<Outcome> {
        let synched: Bool
        switch currentHeight {
        case .synced:
            synched = true
        case .guessed, .unknown:
            synched = false
        }
        
        let properties = self.loadWorksheetData(skipLocator: synched,
                                                processed: self.nodeProperties.processedIndex,
                                                eventLoop: eventLoop)
        
        return properties.map { properties in
            if properties.fullySynched() {
                return .synched(properties.backlog)
            }
            
            let downloaders = properties
                .downloaders(blockHeaderMaxDownloadCount: self.blockHeaderMaxDownloadCount,
                             blockHeaderBacklogCount: self.blockHeaderBacklogCount,
                             cfConcurrentInvocations: self.cfConcurrentInvocations,
                             cfCommandTimeout: self.cfCommandTimeout,
                             fullBlockCommandTimeout: self.fullBlockCommandTimeout,
                             verifyMinimumFilters: self.verifyMinimumFilters,
                             muxer: self.muxer,
                             nodeDelegate: nodeDelegate,
                             nodeProperties: self.nodeProperties,
                             cfMatcher: self.cfMatcher,
                             outpoints: outpoints,
                             pubKeys: pubKeys,
                             threadPool: self.threadPool)
            let downloadState = Node.DownloadState(networkState: self,
                                                   backlog: { downloaders.blockHeader.backlog },
                                                   almostComplete: downloaders.cancelReadySignalProducer(),
                                                   canceller: downloaders.cancelActionProducer(),
                                                   outcomes: downloaders.start())
            return .download(downloadState)
        }
    }
    
    // MARK: Load Worksheet Data
    func loadWorksheetData(skipLocator: Bool,
                           processed: Int,
                           eventLoop: EventLoop) -> Future<Node.Worksheet.Properties> {
        return self.chainHeightsAndCfHeaderTip(processed: processed)
        .flatMap { lowerHeight, upperHeight, cfHeaderTip in
            let backlog = self.blockHeaderBacklogFuture(lowerHeight: lowerHeight,
                                                        upperHeight: upperHeight)
            let locator: Future<BlockLocator?> = {
                if skipLocator {
                    return eventLoop.makeSucceededFuture(nil)
                } else {
                    return self.bhRepo
                    .latestBlockLocator()
                    .map(BlockLocator?.some)
                }
            }()
            let cfHeader = Node.CFHeaderDownload
                .cfHeaderFuture(upperHeight: upperHeight,
                                cfHeaderTip: cfHeaderTip,
                                maxHeaders: maxHeaders,
                                maxStops: maxCfHeaderStops,
                                bhRepo: self.bhRepo,
                                cfHeaderRepo: self.cfHeaderRepo,
                                eventLoop: eventLoop)
            let filter = Node.CompactFilterDownload
                .filterFuture(cfHeaderTip: cfHeaderTip,
                              processed: processed,
                              cfDownloadFilterCount: self.cfDownloadFilterCount,
                              cfBundleSize: self.cfBundleSize,
                              bhRepo: self.bhRepo,
                              cfHeaderRepo: self.cfHeaderRepo,
                              eventLoop: eventLoop)
            let block = Node.FullBlockDownload
                .blockFuture(cfHeaderTip: cfHeaderTip,
                             processed: processed,
                             fullBlockDownloaderBatchSize: self.fullBlockDownloaderBatchSize,
                             bhRepo: self.bhRepo,
                             cfHeaderRepo: self.cfHeaderRepo,
                             resultHandler: { self.nodeProperties.processedIndex = $0 },
                             eventLoop: eventLoop)
            let verify = Node.VerifyDownload
                .verifyFuture(cfHeaderTip: cfHeaderTip,
                              verifyCheckpoint: self.nodeProperties.verifyCheckpoint,
                              step: self.verifyRandomStepRange,
                              batch: self.fullBlockDownloaderBatchSize,
                              bhRepo: self.bhRepo,
                              cfHeaderRepo: self.cfHeaderRepo,
                              eventLoop: eventLoop)

            return backlog.and(locator)
            .and(cfHeader.and(filter))
            .flatMap { backlogLocator, cfHeaderFilter in
                block.and(verify)
                .map { block, verify in
                .init(bhHeight: upperHeight,
                      backlog: backlogLocator.0,
                      locator: backlogLocator.1,
                      cfHeader: cfHeaderFilter.0,
                      filter: cfHeaderFilter.1,
                      fullBlock: block,
                      verify: verify,
                      bhRepo: bhRepo,
                      cfHeaderRepo: cfHeaderRepo,
                      eventLoop: eventLoop)
                }
            }
        }
    }
    
    func chainHeightsAndCfHeaderTip(processed: Int)
    -> Future<(lowerHeight: Int, upperHeight: Int, cfHeaderTip: Int)> {
        self.bhRepo.heights()
            .and(self.cfHeaderRepo.cfHeadersTip(processed: processed))
        .map {
            ($0.0.lowerHeight, $0.0.upperHeight, $0.1)
        }
        .always(Node.logError())
    }
    
    func blockHeaderBacklogFuture(lowerHeight: Int, upperHeight: Int) -> Future<[BlockHeader]> {
        let countToLoad = min(upperHeight - lowerHeight, self.blockHeaderBacklogCount - 1)
        return self.bhRepo.find(from: upperHeight - countToLoad)
        .always(Node.logError())
    }
}

// MARK: Factory from .started
extension Node.NetworkState {
    static func createNetworkStateFuture<MD>(
        offset: Int,
        eventLoop: EventLoop,
        threadPool: NIOThreadPool,
        muxerDelegate: MD,
        currentPubKeys: Set<ScriptPubKey>,
        clientFactory: ClientFactory) -> Future<Node.NetworkState>
    where MD: MuxerDelegateProtocol {
        let nonBlockingFileIOClient = Settings.NONBLOCKING_FILE_IO(threadPool)
        
        let cfMatcher = Settings.CF_MATCHER_FACTORY()
        cfMatcher.updateOpcodes(
            currentPubKeys.map(\.opcodes)
        )
        
        return Self.openFileHelper(nonBlockingFileIO: nonBlockingFileIOClient,
                                   eventLoop: eventLoop)
        .and(cfMatcher.start(eventLoop: eventLoop))
        .map { openFile, _ in
            let bhRepo = FileBlockHeaderRepo(eventLoop: eventLoop,
                                             nonBlockingFileIO: nonBlockingFileIOClient,
                                             nioFileHandle: try! openFile.duplicate(),
                                             offset: offset)
            let cfHeaderRepo = FileCFHeaderRepo(eventLoop: eventLoop,
                                                nonBlockingFileIO: nonBlockingFileIOClient,
                                                nioFileHandle: try! openFile.duplicate(),
                                                offset: offset)
            let muxer = Muxer(muxerDelegate: muxerDelegate, clientFactory: clientFactory)
            muxer.renewConnections()
            
            return .init(threadPool: threadPool,
                         nonBlockingFileIO: nonBlockingFileIOClient,
                         bhRepo: bhRepo,
                         cfHeaderRepo: cfHeaderRepo,
                         openFile: openFile,
                         muxer: muxer,
                         cfMatcher: cfMatcher)
        }
    }

    static func openFileHelper(nonBlockingFileIO: NonBlockingFileIOClient,
                               eventLoop: EventLoop) -> Future<NIOFileHandle> {
        nonBlockingFileIO.openFile(path: Settings.BITCOIN_BLOCKHEADER_FILEPATH,
                                   mode: [.read, .write],
                                   flags: .allowFileCreation(posixMode: 0o600),
                                   eventLoop: eventLoop)
    }
}
