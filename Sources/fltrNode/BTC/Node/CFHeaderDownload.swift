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
import HaByLo
import NIO

extension Node {
    final class CFHeaderDownload: NodeDownloaderProtocol {
        private let sequenceFuture: SequenceFuture<BlockHeader, CF.SerialHeader>
        private let startElement: CF.SerialHeader
        private let eventLoop: EventLoop
        
        init<S: Sequence>(start: CF.SerialHeader,
                          stops: S,
                          walletCreation: NodeProperties.WalletCreation,
                          cfHeaderRepo: FileCFHeaderRepo,
                          muxer: Muxer,
                          nodeDelegate: NodeDelegate,
                          threadPool: NIOThreadPool,
                          eventLoop: EventLoop)
        where S.Element == BlockHeader {
            self.sequenceFuture = .init(stops, eventLoop: eventLoop) { cfHeader, blockHeader in
                let serialHeaders = Self.cfHeaderDownload(start: cfHeader,
                                                          stop: blockHeader,
                                                          muxer: muxer,
                                                          eventLoop: eventLoop)
                
                let storeHeaders = serialHeaders.map { serialHeaders -> ArraySlice<CF.SerialHeader> in
                    var callNodeDelegate = false
                    
                    let result = serialHeaders.map { header -> CF.SerialHeader in
                        var header = header
                        if walletCreation.skip(blockHeader) {
                            header.add(flags: [.hasFilterHeader, .nonMatchingFilter, .processed])
                            callNodeDelegate = true
                        }

                        return header
                    }[...]
                    
                    if callNodeDelegate {
                        threadPool.submit {
                            guard case .active = $0
                            else { return }
                            
                            try! (cfHeader.id ... blockHeader.height()).forEach {
                                nodeDelegate.filterEvent(.download($0))
                                nodeDelegate.filterEvent(.nonMatching($0))
                            }
                        }
                    }
                    
                    return result
                }
                
                Self.store(cfHeaders: storeHeaders,
                           cfHeaderRepo: cfHeaderRepo)
                
                return serialHeaders.map(\.last!)
            }
            
            self.startElement = start
            self.eventLoop = eventLoop
        }
        
        var almostComplete: Bool {
            self.sequenceFuture.almostComplete
        }
        
        func cancel() {
            self.sequenceFuture.cancel()
        }
        
        func start() -> Future<NodeDownloaderOutcome> {
            self.sequenceFuture.start(self.startElement)
            .map { _ in .processed }
            .flatMapError {
                switch $0 {
                case Node.Error.muxerNoCFNode:
                    return self.eventLoop.makeSucceededFuture(.drop)
                case Node.HeadersError.illegalHashChain(let height):
                    return self.eventLoop.makeSucceededFuture(.rollback(height))
                default:
                    return self.eventLoop.makeFailedFuture($0)
                }
            }
        }
    }
}

extension Node.CFHeaderDownload {
    static func cfHeaderDownload(start: CF.SerialHeader,
                                 stop: BlockHeader,
                                 muxer: Muxer,
                                 eventLoop: EventLoop) -> Future<ArraySlice<CF.SerialHeader>> {
        guard let client = muxer.random(with: \.isFilterNode) else {
            return eventLoop.makeFailedFuture(Node.Error.muxerNoCFNode)
        }
        
        return Self.cfHeaderDownload(start: start,
                                     stop: stop,
                                     client: client,
                                     eventLoop: eventLoop)
    }
    
    static func cfHeaderDownload(start: CF.SerialHeader,
                                 stop: BlockHeader,
                                 client: Client,
                                 eventLoop: EventLoop) -> Future<ArraySlice<CF.SerialHeader>> {
        func getCfHeaders(client: Client,
                          start height: Int,
                          stop hash: BlockChain.Hash<BlockHeaderHash>) -> Future<[CF.Header]> {
            client.outgoingGetcfheaders(startHeight: height, stop: hash)
            .flatMapThrowing {
                guard $0.stop == hash else {
                    throw Node.HeadersError.unexpectedStopHash
                }
                
                return $0.headers
            }
        }
        
        func cfHeaderWireToSerial(client: Client,
                                  start serial: CF.SerialHeader,
                                  wire payload: [CF.Header]) throws -> ArraySlice<CF.SerialHeader> {
            guard let first = payload.first else {
                throw Node.HeadersError.emptyPayload(Client.Key(client))
            }

            guard serial.headerHash == first.previousHeaderHash else {
                throw Node.HeadersError.illegalHashChain(first: serial.id)
            }
            
            return try payload.reduce(into: [serial, ]) { accumulator, wire in
                let last = accumulator.last!
                guard let nextSerialHeader = CF.SerialHeader(wire, previousHeader: last) else {
                    throw Node.HeadersError.illegalHashChain(first: last.id)
                }
                accumulator.append(nextSerialHeader)
            }
            .dropFirst()
        }

        return getCfHeaders(client: client, start: start.id + 1, stop: try! stop.blockHash())
        .flatMapThrowing { wireHeaders in
            try cfHeaderWireToSerial(client: client, start: start, wire: wireHeaders)
        }
    }

    static func store(cfHeaders: Future<ArraySlice<CF.SerialHeader>>,
                      cfHeaderRepo: FileCFHeaderRepo) {
        cfHeaders.whenSuccess {
            $0.forEach {
                cfHeaderRepo.write($0)
                .whenFailure(Node.terminalStoreFailure())
            }
        }
    }
}

extension Node.CFHeaderDownload {
    static func cfHeaderFuture(upperHeight: Int,
                               cfHeaderTip: Int,
                               maxHeaders: Int,
                               maxStops: Int,
                               bhRepo: FileBlockHeaderRepo,
                               cfHeaderRepo: FileCFHeaderRepo,
                               eventLoop: EventLoop) -> Future<Node.CFHeaderPair?> {
        guard cfHeaderTip < upperHeight else {
            return eventLoop.makeSucceededFuture(Optional<Node.CFHeaderPair>.none)
        }
        
        let bhRepoIndex = min(upperHeight, cfHeaderTip + maxHeaders)
        
        let blockHeaderSequence = sequence(first: bhRepoIndex) { previous in
            let candidate = previous + maxHeaders
            return candidate <= upperHeight ? candidate : nil
        }
        
        let cfHeaderFuture = cfHeaderRepo.find(id: cfHeaderTip)
        let bhFutures: [Future<BlockHeader>] = blockHeaderSequence
            .prefix(maxStops)
            .map { bhRepo.find(id: $0) }
        
        return cfHeaderFuture.and(
            Future.whenAllSucceed(bhFutures, on: eventLoop)
        )
        .map { cfHeader, blockHeaders in
            (start: cfHeader, stops: blockHeaders[...])
        }
        .always(Node.logError())
    }
}
