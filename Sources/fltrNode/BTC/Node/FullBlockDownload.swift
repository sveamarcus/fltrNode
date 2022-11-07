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
import var HaByLo.logger
import fltrTx
import fltrWAPI
import struct Foundation.IndexSet
import NIO

extension Node {
    final class FullBlockDownload: NodeDownloaderProtocol {
        enum DownloadResult {
            case processed(Int)
            case rollback(Int)
            
            var processedHeight: Int? {
                switch self {
                case .processed(let height): return height
                case .rollback: return nil
                }
            }
            
            var proceed: Bool {
                switch self {
                case .processed: return true
                case .rollback: return false
                }
            }
        }
        
        private let eventLoop: EventLoop
        private var sequenceFuture: SequenceFuture<(Future<BlockCommand>, BlockHeader), DownloadResult>
        var almostComplete = false
        
        init<S: Sequence>(_ headers: S,
                          outpoints: @escaping () -> Future<Outpoints>,
                          fullBlockCommandTimeout: TimeAmount,
                          pubKeys: @escaping () -> Future<Set<ScriptPubKey>>,
//                          resultHandler: @escaping (DownloadResult) -> Void,
                          cfHeaderRepo: FileCFHeaderRepo,
                          muxer: Muxer,
                          nodeDelegate: NodeDelegate,
                          threadPool: NIOThreadPool,
                          eventLoop: EventLoop)
        where S.Element == BlockHeader {
            self.eventLoop = eventLoop
            
            let downloadFutures = headers.map {
                Self.downloadFutures(header: $0,
                                     fullBlockCommandTimeout: fullBlockCommandTimeout,
                                     muxer: muxer,
                                     nodeDelegate: nodeDelegate,
                                     eventLoop: eventLoop)
            }
            
            let zipped = zip(downloadFutures, headers)
            
            self.sequenceFuture = .init(zipped,
                                        eventLoop: eventLoop)
                { previousResult, zipped in
                    let downloadFuture = zipped.0
                    let header = zipped.1
                    
                    guard previousResult.proceed
                    else { return eventLoop.makeSucceededFuture(previousResult) }
                    
                    return downloadFuture.flatMap { block in
                        outpoints().and(pubKeys())
                        .hop(to: eventLoop)
                        .flatMap { outpoints, pubKeys in
                            Self.match(block: block,
                                       header: header,
                                       outpoints: outpoints,
                                       pubKeys: pubKeys,
                                       nodeDelegate: nodeDelegate,
                                       threadPool: threadPool,
                                       eventLoop: eventLoop)
                            .flatMap { height, events in
                                guard let events = events
                                else {
                                    logger.info("Node \(#function) - ✸No Match✸, False positive"
                                                    + " for block download of height \(height)")
                                    return eventLoop.makeSucceededFuture(.processed(height))
                                }
                                
                                return Self.walletNotifyTransactions(height: height,
                                                                     events: events,
                                                                     nodeDelegate: nodeDelegate,
                                                                     eventLoop: eventLoop)
                            }
                        }
                    }
                    .flatMap { downloadResult -> Future<DownloadResult> in
                        Self.store(processed: downloadResult, cfHeaderRepo: cfHeaderRepo)
                        .map { downloadResult }
                    }
               }
        }
        
        func cancel() {
            self.sequenceFuture.cancel()
        }

        func start() -> Future<NodeDownloaderOutcome> {
            self.sequenceFuture.start(.processed(0))
            .always { _ in
                self.almostComplete = true
            }
            .map {
                switch $0 {
                case .processed:
                    return .processed
                case .rollback(let height):
                    return .rollback(height)
                }
            }
            .flatMapError { error in
                switch error {
                case let e as Node.Error:
                    switch e {
                    case .muxerNoFullNode:
                        return self.eventLoop.makeSucceededFuture(.drop)
                    case .cannotCloseFileHandle,
                            .closeAlreadyCalled,
                            .invalidState,
                            .muxerNoClient,
                            .muxerNoCFNode:
                        break
                    case .rollback,
                            .timeoutErrorWaitingForDelegate:
                        preconditionFailure()
                    }
                default:
                    break
                }
                
                return self.eventLoop.makeFailedFuture(error)
            }
        }
    }
}

extension Node.FullBlockDownload {
    static func downloadFutures(header: BlockHeader,
                                fullBlockCommandTimeout: TimeAmount,
                                muxer: Muxer,
                                nodeDelegate: NodeDelegate,
                                eventLoop: EventLoop) -> Future<BlockCommand> {
        guard let client: Client = {
            if let client = muxer.next(),
               let services = try? client.services(),
               services.isFullNode {
                return client
            }
            
            return muxer.random(with: \.isFullNode)
        }() else {
            return eventLoop.makeFailedFuture(Node.Error.muxerNoFullNode)
        }

        let (writeFuture, resultFuture) = client.outgoingGetdataBlockWithWriteFuture(
            try! header.blockHash(),
            timeout: fullBlockCommandTimeout
        )

        writeFuture.whenSuccess {
            nodeDelegate.filterEvent(.blockDownload(try! header.height()))
        }
        
        return resultFuture
    }
    
    @usableFromInline
    static func match(block: BlockCommand,
                      header: BlockHeader,
                      outpoints: Outpoints,
                      pubKeys: Set<ScriptPubKey>,
                      nodeDelegate: NodeDelegate,
                      threadPool: NIOThreadPool,
                      eventLoop: EventLoop) -> Future<(height: Int, events: [TransactionEvent]?)> {
        threadPool.runIfActive(eventLoop: eventLoop) { () -> (height: Int, events: [TransactionEvent]?) in
            var block = block
            try block.verify(header: header)

            let transactions = try block.getTransactions()
            let receivedFunding: [FundingOutpoint] = transactions.flatMap { tx in
                tx.findFunding(for: pubKeys)
            }

            let receivedOutpoints = receivedFunding.map { $0.outpoint }
            let outpoints = Set(outpoints + receivedOutpoints)
            var spentOutpoints: [SpentOutpoint] = []
            for tx in transactions {
                let foundSpent = tx.findSpent(for: outpoints)
                let newSpentOutpoints: [SpentOutpoint] = try foundSpent.map { outpoint in
                    guard let tx = Tx.AnyTransaction(tx)
                    else { throw Tx.Transaction.TransactionError.cannotDeserializeTransaction }

                    let outputs = tx.findOutputs(in: pubKeys)
                    return SpentOutpoint(outpoint: outpoint,
                                         outputs: outputs,
                                         tx: tx)
                }
                
                spentOutpoints.append(contentsOf: newSpentOutpoints)
            }
            
            let height = try! header.height()
            let events = receivedFunding.map(TransactionEvent.new)
                + spentOutpoints.map(TransactionEvent.spent)
            return events.isEmpty
                ? (height: height, events: nil)
                : (height: height, events: events)
        }
        .always {
            let height = try! header.height()
            switch $0 {
            case .success((let height, .none)):
                nodeDelegate.filterEvent(.blockNonMatching(height))
            case .success((let height, .some)):
                nodeDelegate.filterEvent(.blockMatching(height))
            case .failure:
                nodeDelegate.filterEvent(.blockFailed(height))
            }
        }
    }
    
    static func walletNotifyTransactions(height: Int,
                                         events: [TransactionEvent],
                                         nodeDelegate: NodeDelegate,
                                         eventLoop: EventLoop) -> Future<DownloadResult> {
        logger.info("Node", #function, "- Matching transaction in block", height)

        let promise = eventLoop.makePromise(of: DownloadResult.self)
        Node.nodeDelegateWith(timeout: .now() + Settings.BITCOIN_WALLET_DELEGATE_COMMIT_TIMEOUT,
                              eventLoop: eventLoop) { commit in
            nodeDelegate.transactions(height: height,
                                      events: events) { eventResult in
                commit()
                switch eventResult {
                case .relaxed:
                    promise.succeed(.processed(height))
                case .strict:
                    promise.succeed(.rollback(height + 1))
                }
            }
            
        }
        .whenFailure {
            preconditionFailure("\($0)")
        }
        return promise.futureResult
    }

    static func store(processed height: DownloadResult,
                      cfHeaderRepo: FileCFHeaderRepo) -> Future<Void> {
        switch height {
        case .rollback(let height):
            return cfHeaderRepo.addFlags(id: height - 1, flags: .processed)
            .recover(Node.terminalStoreFailure())
        case .processed(let height):
            return cfHeaderRepo.addFlags(id: height, flags: .processed)
            .recover(Node.terminalStoreFailure())
        }
    }
}

extension Node.FullBlockDownload {
    static func blockFuture(cfHeaderTip: Int,
                            processed: Int,
                            fullBlockDownloaderBatchSize max: Int,
                            bhRepo: FileBlockHeaderRepo,
                            cfHeaderRepo: FileCFHeaderRepo,
                            resultHandler: @escaping (Int) -> Void,
                            eventLoop: EventLoop) -> Future<ArraySlice<BlockHeader>?> {
        func findNonProcessed(start: Int) -> Future<[Int]> {
            guard start <= cfHeaderTip
            else {
                return eventLoop.makeSucceededFuture([])
            }
            
            return cfHeaderRepo.records(from: start, to: cfHeaderTip + 1) {
                $0.contains(.processed)
            }
            .flatMap { processedSet in
                guard let first = processedSet.first
                else {
                    let range = start ..< (start + max)
                    return eventLoop.makeSucceededFuture(Array(range))
                }
                
                var destination = IndexSet()
                if first == start,
                   let range = processedSet.rangeView.first {
                    // store processed count for next round
                    resultHandler(range.last!)
                } else {
                    destination.insert(integersIn: start ..< first)
                }

                var result = [Int]()
                let destinationRange = (first ..< (cfHeaderTip + 1))
                destination.insert(integersIn: destinationRange)
                destination.subtract(processedSet)
                assert(
                    destination.isEmpty
                    || result.isEmpty
                    || !destination.contains(integersIn: IndexSet(result))
                )
                
                result.append(contentsOf:
                    destination.prefix(result.count.distance(to: max))
                )
                assert(result.count <= max)

                return eventLoop.makeSucceededFuture(result)
            }
        }

        let start = Swift.max(processed, cfHeaderRepo.offset)
        return findNonProcessed(start: start)
        .flatMap {
            let flagFutures = $0.map {
                cfHeaderRepo.find(id: $0)
            }
            
            return Future.whenAllSucceed(flagFutures, on: eventLoop)
            .map { flagHeaders in
                var result: [Int] = []
                for flag in flagHeaders {
                    if flag.recordFlag == [ .hasFilterHeader, .matchingFilter ] {
                        result.append(flag.id)
                    } else {
                        break
                    }
                }
                return result
            }
        }
        .flatMap { (ids: [Int]) -> EventLoopFuture<[BlockHeader]> in
            let futures: [EventLoopFuture<BlockHeader>] = ids.map { bhRepo.find(id: $0) }
            
            return Future.whenAllSucceed(futures, on: eventLoop)
        }
        .map { (headers: [BlockHeader]) -> ArraySlice<BlockHeader>? in
            headers.isEmpty ? nil : headers[...]
        }
        .always(Node.logError())
    }
}
