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
    final class VerifyDownload: NodeDownloaderProtocol {
        enum DownloadResult {
            case processed
            case rollback(Int)
            
            var proceed: Bool {
                switch self {
                case .processed: return true
                case .rollback: return false
                }
            }
        }
        
        func cancel() {
            self.sequenceFuture.cancel()
        }
        
        var almostComplete: Bool {
            self.sequenceFuture.almostComplete
        }
        
        func start() -> EventLoopFuture<NodeDownloaderOutcome> {
            self.sequenceFuture.start(.processed)
            .always(Node.logError())
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
                    case .muxerNoFullNode, .muxerNoCFNode:
                        return self.eventLoop.makeSucceededFuture(.drop)
                    case .cannotCloseFileHandle,
                            .closeAlreadyCalled,
                            .invalidState,
                            .muxerNoClient:
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
        
        @Setting(\.BITCOIN_VERIFY_MIN_TRANSACTIONS) private var minimumFilters: Int
        private var sequenceFuture: SequenceFuture<(Future<(BlockCommand, CF.CfilterCommand)>, (BlockHeader, CF.Header)), DownloadResult>
        private let eventLoop: EventLoop
        
        init(headers: [(BlockHeader, CF.Header)],
             muxer: Muxer,
             threadPool: NIOThreadPool,
             eventLoop: EventLoop,
             fullBlockTimeout: TimeAmount,
             cfTimeout: TimeAmount,
             minimumFilters: Int,
             nodeProperties: NodeProperties) {
            self.eventLoop = eventLoop
            let download = Self.downloadFutures(headers: headers,
                                                muxer: muxer,
                                                eventLoop: eventLoop,
                                                fullBlockTimeout: fullBlockTimeout,
                                                cfTimeout: cfTimeout)
            let zipped = zip(download, headers)
            self.sequenceFuture = .init(zipped, eventLoop: eventLoop) { previousOutcome, input in
                let download = input.0
                let blockHeader = input.1.0
                let cfHeader = input.1.1
                
                return Self.sequenceHandler(previousOutcome: previousOutcome,
                                            download: download,
                                            blockHeader: blockHeader,
                                            cfHeader: cfHeader,
                                            minimumFilters: minimumFilters,
                                            nodeProperties: nodeProperties,
                                            threadPool: threadPool,
                                            eventLoop: eventLoop)
            }
        }
    }
}

extension Node.VerifyDownload {
    enum Error: Swift.Error {
        case skipNotEnoughScriptPubKeys
    }
}

extension Node.VerifyDownload {
    static func verifyFuture(cfHeaderTip: Int,
                             verifyCheckpoint: Int,
                             step: Range<Int>,
                             batch: Int,
                             bhRepo: FileBlockHeaderRepo,
                             cfHeaderRepo: FileCFHeaderRepo,
                             eventLoop: EventLoop) -> Future<[(BlockHeader, CF.Header)]?> {
        func makeSequence(start: Int, step: Range<Int>, stop: Int) -> UnfoldSequence<Int, (Int?, Bool)> {
            sequence(first: start) { last in
                let next = last + step.randomElement()!
                
                guard next <= stop
                else { return nil }
                
                return next
            }
        }
        
        let idSequence = makeSequence(start: max(verifyCheckpoint, bhRepo.offset),
                                      step: step,
                                      stop: cfHeaderTip)
        .dropFirst()
        .prefix(batch)
        let ids = Array(idSequence)
        
        guard !ids.isEmpty
        else { return eventLoop.makeSucceededFuture(nil) }
        
        let headerFutures = ids.map {
            bhRepo.find(id: $0)
            .and(
                cfHeaderRepo.findWire(id: $0)
            )
        }
        
        return Future.whenAllSucceed(headerFutures, on: eventLoop)
        .map { .some($0) }
        .always(Node.logError())
    }
    
    static func downloadFutures(headers: [(BlockHeader, CF.Header)],
                                muxer: Muxer,
                                eventLoop: EventLoop,
                                fullBlockTimeout: TimeAmount,
                                cfTimeout: TimeAmount)
    -> [Future<(BlockCommand, CF.CfilterCommand)>] {
        func nextClient() -> (full: Client, filter: Client)? {
            guard let full: Client = {
                if let client = muxer.next(),
                   let services = try? client.services(),
                   services.isFullNode {
                    return client
                }

                return muxer.random(with: \.isFullNode)
            }(),
            let filter: Client = {
                if let client = muxer.next(),
                   let services = try? client.services(),
                   services.isFilterNode {
                    return client
                }
                
                return muxer.random(with: \.isFilterNode)
            }()
            else {
                return nil
            }
            
            return (full, filter)
        }
            
        
        return headers.map { blockHeader, cfHeader in
            guard let (full, filter) = nextClient()
            else { return eventLoop.makeFailedFuture(Node.Error.muxerNoFullNode) }
            
            let blockFuture = full.outgoingGetdataNoWitnessesBlock(try! blockHeader.blockHash(), timeout: fullBlockTimeout)
            let filterFuture = filter.outgoingGetcfilter(serial: blockHeader, with: cfTimeout)
            
            return blockFuture.and(filterFuture)
        }
    }
    
    static func sequenceHandler(previousOutcome: DownloadResult,
                                download: Future<(BlockCommand, CF.CfilterCommand)>,
                                blockHeader: BlockHeader,
                                cfHeader: CF.Header,
                                minimumFilters: Int,
                                nodeProperties: NodeProperties,
                                threadPool: NIOThreadPool,
                                eventLoop: EventLoop) -> Future<DownloadResult> {
        guard previousOutcome.proceed
        else { return eventLoop.makeSucceededFuture(previousOutcome) }

        return download.flatMap { block, filter in
            var block = block, filter = filter
            do {
                try block.verify(header: blockHeader)
                try filter.addRank(from: blockHeader)
                try filter.validate(cfHeader: cfHeader)
            } catch {
                return eventLoop.makeFailedFuture(error)
            }
            
            return Self.verify(block: block,
                               blockHeader: blockHeader,
                               filter: filter,
                               minimumFilters: minimumFilters,
                               nodeProperties: nodeProperties,
                               threadPool: threadPool,
                               eventLoop: eventLoop)
        }
    }
    
    static func verify(block: BlockCommand,
                       blockHeader: BlockHeader,
                       filter: CF.CfilterCommand,
                       minimumFilters: Int,
                       nodeProperties: NodeProperties,
                       threadPool: NIOThreadPool,
                       eventLoop: EventLoop) -> Future<DownloadResult> {
        let future: Future<Bool> = threadPool.runIfActive(eventLoop: eventLoop) {
            let decoded = Set(try filter.decode())
            let outputs = Self.harvestOuts(block: block)
            let encodedOutputs: [UInt64] = try outputs.map {
                return try CF.MatcherClient.cfHash($0, filter.key(), filter.f)
            }
            
            guard decoded.count >= minimumFilters
            else {
                throw Error.skipNotEnoughScriptPubKeys
            }
            
            return Self.matchAll(from: encodedOutputs, in: decoded)
        }
        
        return future.map { match in
            guard match
            else {
                return .rollback(nodeProperties.verifyCheckpoint + 1)
            }
            
            nodeProperties.verifyCheckpoint = try! blockHeader.height()
            return .processed
        }
    }
    
    static func harvestOuts(block: BlockCommand) -> [[UInt8]] {
        var result: [[UInt8]] = []
        
        for t in try! block.getTransactions() {
            guard !t.isCoinbase
            else { continue }
            
            for o in t.vout.map(\.scriptPubKey) {
                if let first = o.first {
                    if first == OpCodes.OP_RETURN {
                        continue
                    } else {
                        result.append(o)
                    }
                } else { // empty
                    continue
                }
            }
        }
        
        return result
    }
    
    static func matchAll(from data: [UInt64], in set: Set<UInt64>) -> Bool {
        data.reduce(true) {
            $0 && set.contains($1)
        }
    }
}
