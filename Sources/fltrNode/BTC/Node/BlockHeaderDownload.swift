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
import HaByLo
import NIO

extension Node {
    final class BlockHeaderDownload: NodeDownloaderProtocol {
        fileprivate var state: InternalState! // self closure in init
        
        init(count: Int,
             locator: BlockLocator,
             backlog: [BlockHeader],
             maxHeadersOverwrite: Int,
             bhRepo: FileBlockHeaderRepo,
             muxer: Muxer,
             nodeDelegate: NodeDelegate,
             threadPool: NIOThreadPool,
             eventLoop: EventLoop) {
            assert(count > 0)
            
            let sequenceHandler: (Int, BlockLocator) -> Future<BlockLocator> = { count, locator in
                guard case let .running(state, _) = self.state
                else {
                    return eventLoop.makeFailedFuture(EventLoopQueueingCancel())
                }
                
                if count > 0 {
                    state.almostComplete = true
                }
                
                let headers = Self.blockHeaderDownloadAndProcess(locator: locator,
                                                                 blockHeaderBacklog: state.backlog,
                                                                 muxer: muxer,
                                                                 nodeDelegate: nodeDelegate,
                                                                 threadPool: threadPool,
                                                                 eventLoop: eventLoop)
                .map {
                    Self.toSerial(full: $0)
                }
                
                let storePromise = eventLoop.makePromise(of: Void.self)
                headers.whenComplete {
                    storePromise.completeWith(
                        Self.store(serial: $0,
                                   bhRepo: bhRepo,
                                   eventLoop: eventLoop)
                    )
                }
                
                return storePromise.futureResult.and(headers)
                .map { _, headers in
                    state.backlog = [ state.backlog.suffix(maxHeadersOverwrite),
                                      headers ]
                    .joined()
                    .suffix(maxHeadersOverwrite)
                    
                    return BlockChain.refreshBlockLocator(new: headers, previous: locator)
                }
            }
            let sequenceFuture = SequenceFuture<Void, BlockLocator>(count: count,
                                                                    eventLoop: eventLoop,
                                                                    future: sequenceHandler)
            let runningState = InternalState.RunningState(backlog: backlog,
                                                          locator: locator,
                                                          eventLoop: eventLoop)
            self.state = .running(runningState, sequence: sequenceFuture)
        }

        init(cancelled backlog: [BlockHeader],
             eventLoop: EventLoop) {
            self.state = .cancelled(backlog, eventLoop)
        }

        
        var almostComplete: Bool {
            self.state.almostComplete
        }
        
        var backlog: [BlockHeader] {
            switch self.state! {
            case .cancelled(let backlog, _):
                return backlog
            case .running(let state, _):
                return state.backlog
            }
        }
        
        func cancel() {
            switch self.state! {
            case .running(let state, let sequence):
                sequence.cancel()
                self.state = .cancelled(state.backlog, state.eventLoop)
            case .cancelled:
                break
            }
        }
        
        func start() -> EventLoopFuture<NodeDownloaderOutcome> {
            switch self.state! {
            case .running(let state, let sequence):
                return sequence.start(state.locator)
                .map { _ in .processed }
                .flatMapError { error in
                    let outcome: NodeDownloaderOutcome? = {
                        switch error {
                        case Node.Error.rollback(let height):
                            return .rollback(height)
                        case Node.Error.muxerNoClient:
                            return .drop
                        case Node.HeadersError.emptyPayload(let key):
                            return .synch(state.backlog.last!, key)
                        default:
                            return nil
                        }
                    }()
                    
                    if let outcome = outcome {
                        return state.eventLoop.makeSucceededFuture(outcome)
                    } else {
                        return state.eventLoop.makeFailedFuture(error)
                    }
                }
                .always { _ in
                    state.almostComplete = true
                }
            case .cancelled(_, let eventLoop):
                return eventLoop.makeSucceededFuture(.processed)
            }
        }
    }
}

extension Node.BlockHeaderDownload {
    fileprivate enum InternalState {
        case cancelled([BlockHeader], EventLoop)
        case running(RunningState, sequence: SequenceFuture<Void, BlockLocator>)
    }
}

extension Node.BlockHeaderDownload.InternalState {
    fileprivate final class RunningState {
        var almostComplete = false
        var backlog: [BlockHeader]
        let locator: BlockLocator
        let eventLoop: EventLoop
        var status: NodeDownloaderOutcome? = nil
        
        fileprivate init(backlog: [BlockHeader],
                         locator: BlockLocator,
                         eventLoop: EventLoop) {
            self.backlog = backlog
            self.locator = locator
            self.eventLoop = eventLoop
        }
    }
}

extension Node.BlockHeaderDownload.InternalState {
    var almostComplete: Bool {
        switch self {
        case .cancelled:
            return true
        case .running(let state, _):
            return state.almostComplete
        }
    }
}

extension Node.BlockHeaderDownload {
    static func blockHeaderDownloadAndProcess(locator: BlockLocator,
                                              blockHeaderBacklog: [BlockHeader],
                                              muxer: Muxer,
                                              nodeDelegate: NodeDelegate,
                                              threadPool: NIOThreadPool,
                                              eventLoop: EventLoop) -> Future<[BlockHeader]> {
        return Self.blockHeaderDownload(
            locator: locator,
            blockHeaderBacklog: blockHeaderBacklog,
            muxer: muxer,
            nioThreadPool: threadPool,
            eventLoop: eventLoop
        )
        .flatMap {
            Self.walletNotifyNewTip(full: $0.headers,
                                    rollback: $0.rollback,
                                    nodeDelegate: nodeDelegate,
                                    eventLoop: eventLoop)
        }
        .flatMapThrowing {
            try Self.verifyBlockHeaderHashChain(full: $0,
                                                eventLoop: eventLoop)
        }
    }
    
    static func blockHeaderDownload(locator: BlockLocator,
                                    blockHeaderBacklog: [BlockHeader],
                                    muxer: Muxer,
                                    nioThreadPool: NIOThreadPool,
                                    eventLoop: EventLoop) -> Future<(headers: [BlockHeader], rollback: Int?)> {
        let promise = eventLoop.makePromise(of: (headers: [BlockHeader], rollback: Int?).self)
        
        guard let client: Client = muxer.random() else {
            promise.fail(Node.Error.muxerNoClient)
            return promise.futureResult
        }

        client.outgoingGetheaders(locator: locator)
        .whenComplete {
            switch $0 {
            case .success(let headers):
                guard headers.count > 0 else {
                    promise.fail(Node.HeadersError.emptyPayload(Client.Key(client)))
                    return
                }
                
                guard let backlogIndex = blockHeaderBacklog.firstIndex(where: {
                    try! $0.blockHash() == headers[0].previousBlockHash()
                }) else {
                    promise.fail(
                        Node.HeadersError.couldNotConnectWithBlockHeaderBacklog(try! headers[0].previousBlockHash())
                    )
                    return
                }
                
                var rollback: Int? = nil
                let afterBacklogIndex = blockHeaderBacklog.index(after: backlogIndex)
                if afterBacklogIndex < blockHeaderBacklog.endIndex {
                    let maxCompare = blockHeaderBacklog[backlogIndex...].count
                    let headersPrefix = headers.prefix(maxCompare)
                    let downloadHashes = headersPrefix
                        .map { try! $0.previousBlockHash() }
                    let backlogEndIndex = backlogIndex.advanced(by: downloadHashes.count)
                    let backlogHashes = blockHeaderBacklog[backlogIndex..<backlogEndIndex]
                        .map({ try! $0.blockHash() })

                    if downloadHashes.dropFirst() == backlogHashes.dropFirst() {
                        promise.fail(
                            Node.HeadersError.illegalRollbackIsSubset
                        )
                        return
                    } else {
                        rollback = blockHeaderBacklog[backlogIndex].id
                    }
                }
                
                nioThreadPool.runIfActive(eventLoop: eventLoop) { () throws -> [BlockHeader] in
                    try headers.enumerated().map {
                        var copy: BlockHeader = $0.element
                        try copy.addHeight(blockHeaderBacklog[backlogIndex].height() + 1 + $0.offset)
                        try copy.addBlockHash()
                        return copy
                    }
                }
                .whenComplete {
                    switch $0 {
                    case .success(let hashedHeaders):
                        promise.succeed((headers: hashedHeaders, rollback: rollback))
                    case .failure(let error):
                        promise.fail(error)
                    }
                }
            case .failure(let error):
                promise.fail(error)
            }
        }
        
        return promise.futureResult
    }
    
    static func walletNotifyNewTip(full headers: [BlockHeader],
                                   rollback: Int?,
                                   nodeDelegate: NodeDelegate,
                                   eventLoop: EventLoop) -> Future<[BlockHeader]> {
        let timeout: NIODeadline = .now() + Settings.BITCOIN_WALLET_DELEGATE_COMMIT_TIMEOUT

        let rollbackFuture: Future<Void>
        if let rollback = rollback {
            rollbackFuture = eventLoop.makeFailedFuture(Node.Error.rollback(newHeight: rollback))
        } else {
            rollbackFuture = eventLoop.makeSucceededFuture(())
        }
        
        
        return rollbackFuture.flatMap {
            Node.nodeDelegateWith(timeout: timeout, eventLoop: eventLoop) { commit in
                nodeDelegate.newTip(height: headers.last!.id, commit: commit)
            }
        }
        .map {
            headers
        }
    }
    
    static func verifyBlockHeaderHashChain(full headers: [BlockHeader],
                                           eventLoop: EventLoop) throws -> [BlockHeader] {
        try zip(headers, headers.dropFirst()).forEach { first, second in
            guard try first.blockHash() == second.previousBlockHash() else {
                throw Node.HeadersError.illegalHashChain(first: first.id)
            }
        }
        
        return headers
    }

    static func toSerial(full blockHeaders: [BlockHeader]) -> [BlockHeader] {
        blockHeaders.map {
            var copy = $0
            try! copy.serial()
            return copy
        }
    }

    static func store(serial blockHeaders: Result<[BlockHeader], Swift.Error>,
                      bhRepo: FileBlockHeaderRepo,
                      eventLoop: EventLoop) -> Future<Void> {
        let promise = eventLoop.makePromise(of: Void.self)
        
            switch blockHeaders {
            case .success(let headers):
                promise.completeWith(
                    bhRepo.append(headers)
                )
            case .failure(Node.Error.rollback(let newHeight)):
                bhRepo.delete(from: newHeight + 1)
                .flatMapError {
                    switch $0 {
                    case File.Error.seekError:
                        return eventLoop.makeSucceededVoidFuture()
                    default:
                        return eventLoop.makeFailedFuture($0)
                    }
                }
                .recover(Node.terminalStoreFailure())
                .whenComplete { _ in
                    promise.fail(Node.Error.rollback(newHeight: newHeight))
                }
            case .failure(let error):
                promise.fail(error)
            }
        
        return promise.futureResult
    }
}
