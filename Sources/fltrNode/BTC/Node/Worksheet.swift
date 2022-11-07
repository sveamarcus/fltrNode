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
import HaByLo
import NIO

extension Node {
    enum Worksheet {}
}

// MARK: Properties
extension Node.Worksheet {
    struct Properties {
        let bhHeight: Int
        let backlog: [BlockHeader]
        let locator: BlockLocator?
        let cfHeader: Node.CFHeaderPair?
        let filter: ArraySlice<Node.CFBatch>?
        let fullBlock: ArraySlice<BlockHeader>?
        let verify: [(BlockHeader, CF.Header)]?
        
        let bhRepo: FileBlockHeaderRepo
        let cfHeaderRepo: FileCFHeaderRepo
        let eventLoop: EventLoop
    }
}

extension Node.Worksheet.Properties {
    func fullySynched() -> Bool {
        self.locator == nil
        && self.cfHeader == nil
        && self.filter == nil
        && self.fullBlock == nil
        && self.verify == nil
    }
    
    func downloaders(blockHeaderMaxDownloadCount: Int,
                     blockHeaderBacklogCount: Int,
                     cfConcurrentInvocations: Int,
                     cfCommandTimeout: TimeAmount,
                     fullBlockCommandTimeout: TimeAmount,
                     verifyMinimumFilters: Int,
                     muxer: Muxer,
                     nodeDelegate: NodeDelegate,
                     nodeProperties: NodeProperties,
                     cfMatcher: CF.MatcherClient,
                     outpoints: @escaping () -> Future<Outpoints>,
                     pubKeys: @escaping () -> Future<Set<ScriptPubKey>>,
                     threadPool: NIOThreadPool) -> Node.Worksheet.Downloaders {
        let blockHeader: Node.BlockHeaderDownload = {
            if let locator = self.locator {
                return .init(count: blockHeaderMaxDownloadCount,
                             locator: locator,
                             backlog: self.backlog,
                             maxHeadersOverwrite: blockHeaderBacklogCount,
                             bhRepo: self.bhRepo,
                             muxer: muxer,
                             nodeDelegate: nodeDelegate,
                             threadPool: threadPool,
                             eventLoop: self.eventLoop)
            } else {
                return .init(cancelled: self.backlog, eventLoop: self.eventLoop)
            }
        }()
        
        let cfHeader: Node.CFHeaderDownload? = self.cfHeader.map { cfHeader in
            .init(start: cfHeader.start,
                  stops: cfHeader.stops,
                  walletCreation: nodeProperties.walletCreation,
                  cfHeaderRepo: self.cfHeaderRepo,
                  muxer: muxer,
                  nodeDelegate: nodeDelegate,
                  threadPool: threadPool,
                  eventLoop: self.eventLoop)
        }
        
        let filter: Node.CompactFilterDownload? = self.filter.map { filter in
            .init(batches: filter,
                  cfHeaderRepo: self.cfHeaderRepo,
                  muxer: muxer,
                  nodeDelegate: nodeDelegate,
                  threadPool: threadPool,
                  cfMatcher: cfMatcher,
                  pubKeys: pubKeys,
                  eventLoop: self.eventLoop,
                  maxConcurrentInvocations: cfConcurrentInvocations,
                  cfTimeout: cfCommandTimeout)
        }
        
        let fullBlock: Node.FullBlockDownload? = self.fullBlock.map { fullBlock in
            .init(fullBlock,
                  outpoints: outpoints,
                  fullBlockCommandTimeout: fullBlockCommandTimeout,
                  pubKeys: pubKeys,
                  cfHeaderRepo: self.cfHeaderRepo,
                  muxer: muxer,
                  nodeDelegate: nodeDelegate,
                  threadPool: threadPool,
                  eventLoop: eventLoop)
        }
        
        let verify: Node.VerifyDownload? = self.verify.map { verify in
            .init(headers: verify,
                  muxer: muxer,
                  threadPool: threadPool,
                  eventLoop: self.eventLoop,
                  fullBlockTimeout: fullBlockCommandTimeout,
                  cfTimeout: cfCommandTimeout,
                  minimumFilters: verifyMinimumFilters,
                  nodeProperties: nodeProperties)
        }
        
        return Node.Worksheet.Downloaders(blockHeader: blockHeader,
                                           cfHeader: cfHeader,
                                           filter: filter,
                                           fullBlock: fullBlock,
                                           verify: verify,
                                           eventLoop: self.eventLoop)
    }
}

// MARK: Downloaders
extension Node.Worksheet {
    struct Downloaders {
        let blockHeader: Node.BlockHeaderDownload
        let cfHeader: Node.CFHeaderDownload?
        let filter: Node.CompactFilterDownload?
        let fullBlock: Node.FullBlockDownload?
        let verify: Node.VerifyDownload?
        let eventLoop: EventLoop
    }
}

extension Node.Worksheet.Downloaders {
    var joined: Node.NetworkState.Downloaders {
        func name<T>(of t: Optional<T>) -> String
        where T: NodeDownloaderProtocol {
            return "\(T.self)"
        }
        
        return [ ("\(type(of: self.blockHeader))", self.blockHeader),
                 ("\(name(of: self.cfHeader))", self.cfHeader),
                 ("\(name(of: self.filter))", self.filter),
                 ("\(name(of: self.fullBlock))", self.fullBlock),
                 ("\(name(of: self.verify))", self.verify), ]
    }
    
    func start() -> Future<[NodeDownloaderOutcome]> {
        let filtered: [(String, NodeDownloaderProtocol)] = self.joined.compactMap { name, downloader in
            if let downloader = downloader {
                return (name, downloader)
            } else {
                return nil
            }
        }
        
        let outcomes = filtered.map { name, downloader -> Future<NodeDownloaderOutcome> in
            let future = downloader.start()
            logger.trace("NetworkState \(#function) - 📲 [\(name)] starting")

            // Add watchdogs since there is a bug with hanging downloaders
            let deadline = NIODeadline.now() + Settings.BITCOIN_COMMAND_TIMEOUT * 10 + .seconds(5)
            let timer = self.eventLoop.scheduleTask(deadline: deadline) {
                logger.info("NetworkState \(#function) - 📲⏰ Maybe timeout of long running downloader [\(name)]")
            }
            
            future.whenComplete { _ in
                logger.trace("NetworkState \(#function) - 📲 [\(name)] finished")
                timer.cancel()
            }
            
            return future
        }
        
        return Future.whenAllComplete(outcomes, on: self.eventLoop)
        .map { results in
            results.enumerated().compactMap { index, result in
                do {
                    return try result.get()
                } catch {
                    logger.error("Node.Download - Downloader \(filtered[index].0) resulted with error: \(error)")
                    return nil
                }
            }
        }
    }
    
    func cancelReadySignalProducer() -> () -> Bool {
        {
            self.joined.compactMap { name, downloader in
                downloader?.almostComplete
            }
            .reduce(true) {
                $0 && $1
            }
        }
    }

    func cancelActionProducer() -> () -> Void {
        {
            self.joined.forEach { name, downloader in
                downloader.map {
                    logger.trace("Node.Worksheet.Downloaders \(#function) - Cancelling downloader \($0)")
                    $0.cancel()
                }
            }
        }
    }
}
