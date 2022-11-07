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
import fltrTx
import var HaByLo.logger
import NIO

extension Node {
    final class CompactFilterDownload: NodeDownloaderProtocol {
        typealias FilterMatches = [(height: Int, match: Bool)]
        
        private var batches: ArraySlice<Node.CFBatch>?
        private let cfHeaderRepo: FileCFHeaderRepo
        private let muxer: Muxer
        private let threadPool: NIOThreadPool
        private let cfMatcher: CF.MatcherClient
        private let eventLoop: EventLoop
        private let tokenBucket: TokenBucket<[(Int, Bool)]>
        private let pubKeys: () -> Future<Set<ScriptPubKey>>
        private let nodeDelegate: NodeDelegate
        private let cfTimeout: TimeAmount
        
        init(batches: ArraySlice<Node.CFBatch>,
             cfHeaderRepo: FileCFHeaderRepo,
             muxer: Muxer,
             nodeDelegate: NodeDelegate,
             threadPool: NIOThreadPool,
             cfMatcher: CF.MatcherClient,
             pubKeys: @escaping () -> Future<Set<ScriptPubKey>>,
             eventLoop: EventLoop,
             maxConcurrentInvocations: Int,
             cfTimeout: TimeAmount) {
            self.batches = batches
            self.cfHeaderRepo = cfHeaderRepo
            self.muxer = muxer
            self.nodeDelegate = nodeDelegate
            self.threadPool = threadPool
            self.cfMatcher = cfMatcher
            self.pubKeys = pubKeys
            self.eventLoop = eventLoop
            self.tokenBucket = .init(maxConcurrentInvocations: maxConcurrentInvocations,
                                     eventLoop: eventLoop)
            self.cfTimeout = cfTimeout
        }
        
        var almostComplete: Bool {
            self.eventLoop.assertInEventLoop()

            return self.tokenBucket.almostComplete
        }

        func cancel() {
            self.eventLoop.assertInEventLoop()

            self.tokenBucket.cancel()
            self.batches = nil
        }
        
        func start() -> Future<NodeDownloaderOutcome> {
            self.eventLoop.assertInEventLoop()
            return self.pubKeys().flatMap { pubKeys in
                self.cfDownloadAndStore(batches: self.batches ?? [],
                                               pubKeys: pubKeys,
                                               eventLoop: self.eventLoop)
                .map { NodeDownloaderOutcome.processed }
                .flatMapError { error in
                    switch error {
                    case let e as CompoundError:
                        let containsNoCFNode: Bool = {
                            for c in e.errors {
                                switch c {
                                case let e as Node.Error:
                                    switch e {
                                    case .muxerNoCFNode:
                                        return true
                                    case .cannotCloseFileHandle,
                                     .closeAlreadyCalled,
                                     .invalidState,
                                     .muxerNoClient,
                                     .muxerNoFullNode:
                                        break
                                     case .rollback,
                                            .timeoutErrorWaitingForDelegate:
                                        preconditionFailure()
                                    }
                                default:
                                    break
                                }
                            }
                            
                            return false
                        }()
                        
                        if containsNoCFNode {
                            return self.eventLoop.makeSucceededFuture(.drop)
                        } else {
                            break
                        }
                    default:
                        break
                    }
                    
                    return self.eventLoop.makeFailedFuture(error)
                }
            }
        }
    }
}

extension Node.CompactFilterDownload {
    struct CompoundError: Swift.Error {
        let errors: [Swift.Error]
        
        var count: Int {
            self.errors.count
        }
    }
    
    func cfDownloadAndStore<Batches>(batches: Batches,
                                     pubKeys: Set<ScriptPubKey>,
                                     eventLoop: EventLoop) -> Future<Void>
    where Batches: Collection, Batches.Element == Node.CFBatch {
        let downloadFutures: [Future<Void>] = self.cfDownload(batches: batches,
                                                              pubKeys: pubKeys,
                                                              eventLoop: eventLoop)
            .map(self.store(_:))

        let cfDownloadPromise = eventLoop.makePromise(of: Void.self)
        Future.whenAllComplete(downloadFutures, on: eventLoop)
        .whenSuccess { results in
            let errors: [Swift.Error] = results.compactMap { result in
                switch result {
                case .success:
                    return nil
                case .failure(let error):
                    return error
                }
            }
            
            if errors.count > 0 {
                cfDownloadPromise.fail(CompoundError(errors: errors))
            } else {
                cfDownloadPromise.succeed(())
            }
        }

        return cfDownloadPromise.futureResult
    }
    
    func cfDownload<Batches>(batches: Batches,
                             pubKeys: Set<ScriptPubKey>,
                             eventLoop: EventLoop) -> [Future<Node.FilterMatches>]
    where Batches: Collection, Batches.Element == Node.CFBatch {
        assert(batches.count > 0)
        
        return batches.map {
            let downloadDeferred = self.cfDownloadDeferred(batch: $0,
                                                           pubKeys: pubKeys,
                                                           eventLoop: eventLoop)
            return self.tokenBucket.submitWorkItem(downloadDeferred)
        }
    }

    func cfDownloadDeferred(batch: Node.CFBatch,
                            pubKeys: Set<ScriptPubKey>,
                            eventLoop: EventLoop) -> () -> Future<Node.FilterMatches> {
        {
            guard let client: Client = {
                if let client = self.muxer.next(),
                   let services = try? client.services(),
                   services.isFilterNode {
                    return client
                }
                
                return self.muxer.random(with: \.isFilterNode)
            }() else {
                return eventLoop.makeFailedFuture(Node.Error.muxerNoCFNode)
            }
            
            assert(batch.count > 0)

            let startHeight = try! batch.first!.0.height()
            let stopHash = try! batch.last!.0.blockHash()
            let count = batch.count
            
            let (writeFuture, resultFuture) = client
            .outgoingGetCfBundleWithWriteFuture(startHeight: startHeight,
                                                stopHash: stopHash,
                                                count: count,
                                                with: self.cfTimeout)

            let filterHeightRange = (startHeight ..< startHeight + count)
            writeFuture.whenSuccess {
                filterHeightRange.forEach {
                    self.nodeDelegate.filterEvent(.download($0))
                }
            }
            
            let promise = eventLoop.makePromise(of: Node.FilterMatches.self)
            resultFuture.whenComplete {
                switch $0 {
                case .success(let compactFilters):
                    let work: NIOThreadPool.WorkItem = { state in
                        switch state {
                        case .active:
                            let futures = compactFilters.enumerated().map { index, cfilter -> Future<(Int, Bool)> in
                                var cfilter = cfilter
                                let height = startHeight + index

                                do {
                                    try cfilter.addRank(from: batch[index].0)
                                    try cfilter.validate(cfHeader: batch[index].1)
                                } catch {
                                    return eventLoop.makeFailedFuture(error)
                                }
                                

                                return cfilter.filter(matcher: self.cfMatcher,
                                                      pubKeys: pubKeys,
                                                      eventLoop: self.eventLoop)
                                .map {
                                    (height, $0)
                                }
                            }
                            
                            promise.completeWith(Future.whenAllSucceed(futures, on: eventLoop))
                        case .cancelled:
                            promise.fail(ChannelError.ioOnClosedChannel)
                        }
                    }
                    
                    self.threadPool.submit(work)
                case .failure(let error):
                    promise.fail(error)
                }
            }

            promise.futureResult
            .whenComplete {
                switch $0 {
                case .success(let results):
                    results.forEach { height, matches in
                        self.nodeDelegate.filterEvent(
                            matches ? .matching(height) : .nonMatching(height)
                        )
                    }
                case .failure(let error):
                    logger.error("Node.CompactFilterDownload \(#function) - Failed with error[\(error)] for heights [\(filterHeightRange)]")
                    filterHeightRange.forEach {
                        self.nodeDelegate.filterEvent(.failure($0))
                    }
                }
            }
            
            return promise.futureResult
        }
    }
    
    func store(_ future: Future<Node.FilterMatches>) -> Future<Void> {
        future.flatMap { matches in
            let storeFutures = matches.map {
                self.cfHeaderRepo.addFlags(id: $0.0, flags: $0.1 ? .matchingFilter : [.nonMatchingFilter, .processed])
                .recover(Node.terminalStoreFailure())
            }
            
            return Future.andAllSucceed(storeFutures, on: self.eventLoop)
        }
    }
}

extension Node.CompactFilterDownload {
    static func filterFuture(cfHeaderTip: Int,
                             processed: Int,
                             cfDownloadFilterCount: Int,
                             cfBundleSize: Int,
                             bhRepo: FileBlockHeaderRepo,
                             cfHeaderRepo: FileCFHeaderRepo,
                             eventLoop: EventLoop) -> Future<ArraySlice<Node.CFBatch>?> {
        cfHeaderRepo.records(from: Swift.max(processed, cfHeaderRepo.offset),
                             to: cfHeaderTip + 1,
                             limit: cfDownloadFilterCount) {
            $0 == .hasFilterHeader
        }
        .flatMap { indices in
            guard !indices.isEmpty else {
                return eventLoop.makeSucceededFuture(nil)
            }
            
            let subQueries = indices.rangeView.flatMap {
                Self.subQueries(range: $0, stride: cfBundleSize)
            }
            
            let blockHeaders: [Future<[BlockHeader]>] = subQueries.map { start, count in
                bhRepo.find(from: start, through: start + count - 1)
            }
            
            let cfHeaders = subQueries.map { start, count in
                cfHeaderRepo.find(from: start - 1, through: start + count - 1)
                .map { serialHeaders in
                    zip(serialHeaders.dropFirst(), serialHeaders).map {
                        CF.Header($0, previousHeader: $1)!
                    }
                }
            }

            let blockHeadersFuture = Future.whenAllSucceed(blockHeaders, on: eventLoop)
            let cfHeadersFuture = Future.whenAllSucceed(cfHeaders, on: eventLoop)
            
            return blockHeadersFuture.and(cfHeadersFuture).map { blockHeaders, cfHeaders in
                assert(blockHeaders.count == cfHeaders.count)
                
                return blockHeaders.enumerated().map { index, innerBh in
                    let innerCf = cfHeaders[index]
                    assert(innerBh.count == innerCf.count)
                    
                    return Array(zip(innerBh, innerCf))
                }[...]
            }
        }
        .always(Node.logError())
    }
    
    static func subQueries(range: Range<Int>, stride: Int) -> [(start: Int, count: Int)] {
        assert(stride > 0)
        var queries: [(start: Int, count: Int)] = []
        var count = 0
        
        var currentIndex = range.lowerBound
        while currentIndex < range.upperBound {
            count = min(range.distance(from: currentIndex, to: range.upperBound), stride)
            queries.append((currentIndex, count))
            range.formIndex(&currentIndex, offsetBy: count)
        }
        
        return queries
    }
}
