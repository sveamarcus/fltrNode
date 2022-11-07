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
import fltrWAPI
import Foundation
import HaByLo
import NIO
import NIOTransportServices
import NIOConcurrencyHelpers

// MARK: Node
public final class Node {
    typealias CFHeaderPair = (start: CF.SerialHeader, stops: ArraySlice<BlockHeader>)
    typealias CFBatch = [(BlockHeader, CF.Header)]
    typealias FilterMatches = [(Int, Bool)]
    typealias Mempool = [Tx.TxId : Tx.Transaction]
    
    let eventLoop: EventLoop
    let eventLoopGroup: NIOTSEventLoopGroup
    
    private let stateMachine: Reactor
    
    init(eventLoopGroup: NIOTSEventLoopGroup,
         nodeEventLoop: EventLoop,
         threadPool: NIOThreadPool,
         delegate: NodeDelegate) {
        self.eventLoop = nodeEventLoop
        self.eventLoopGroup = eventLoopGroup
        self.stateMachine = Reactor(eventLoop: nodeEventLoop,
                                    eventLoopGroup: eventLoopGroup,
                                    threadPool: threadPool,
                                    nodeDelegate: delegate)
    }
}

extension EventLoop {
    func assertNotInEventLoopEmbedded() {
        assert(self is EmbeddedEventLoopLocked ? true : !self.inEventLoop)
    }
}

extension Node {
    public func start() -> Future<Void> {
        self.eventLoop.submit {
            try self.stateMachine.start()
        }
    }
    
    public func stop() -> Future<Void> {
        let promise = self.eventLoop.makePromise(of: Void.self)
        
        self.eventLoop.execute {
            self.stateMachine.stop(promise)
        }
        
        return promise.futureResult
    }

    public func addOutpoints(_ outpoints: [Tx.Outpoint]) -> Future<Void> {
        self.eventLoopSubmit {
            self.stateMachine.addOutpoints(outpoints)
        }
    }
    
    public func removeOutpoint(_ outpoint: Tx.Outpoint) -> Future<Void> {
        self.eventLoopSubmit {
            self.stateMachine.removeOutpoint(outpoint)
        }
    }

    public func addScriptPubKeys(_ pubKeys: [ScriptPubKey]) -> Future<Void> {
        self.eventLoopSubmit {
            self.stateMachine.addScriptPubKeys(pubKeys)
        }
    }
    
    private func eventLoopExeceute(_ fn: @escaping () -> Void) {
        if self.eventLoop.inEventLoop {
            return fn()
        } else {
            return self.eventLoop.execute(fn)
        }
    }
    
    private func eventLoopFlatSubmit<T>(_ fn: @escaping () -> Future<T>) -> Future<T> {
        if self.eventLoop.inEventLoop {
            return fn()
        } else {
            return self.eventLoop.flatSubmit(fn)
        }
    }

    private func eventLoopSubmit<T>(_ fn: @escaping () -> T) -> Future<T> {
        if self.eventLoop.inEventLoop {
            return self.eventLoop.makeSucceededFuture(fn())
        } else {
            return self.eventLoop.submit(fn)
        }
    }
    
    @discardableResult
    public func addTransaction<T: TransactionProtocol>(_ tx: T) throws -> Tx.TxId {
        try self.stateMachine.addTransaction(tx)
    }
    
    public func sendTransaction<T: TransactionProtocol>(_ tx: T) -> Future<Tx.TxId> {
        self.eventLoopFlatSubmit {
            self.stateMachine.sendTransaction(tx)
        }
    }
    
    @discardableResult
    public func removeTransaction(_ txId: Tx.TxId) -> Bool {
        if let _ = self.stateMachine.removeTransaction(txId) {
            return true
        } else {
            return false
        }
    }
    
    public func blockHeaderLookup(for height: Int) -> Future<BlockHeader?> {
        return self.eventLoop.flatSubmit {
            self.stateMachine.blockHeaderLookup(for: height)
        }
    }
    
    static func closeHelper(openFileHandle: NIOFileHandle, eventLoop: EventLoop) -> Future<Void> {
        eventLoop.submit {
            try openFileHandle.close()
        }
    }
    
    static func closeHelper(activeThreadPool: NIOThreadPool?,
                            eventLoop: EventLoop) -> Future<Void> {
        guard let activeThreadPool = activeThreadPool
        else { return eventLoop.makeSucceededVoidFuture() }
        
        let poolClose = eventLoop.makePromise(of: Void.self)
        activeThreadPool.shutdownGracefully(queue: .global()) {
            if let error = $0 {
                poolClose.fail(error)
            } else {
                poolClose.succeed(())
            }
        }
        return poolClose.futureResult
    }
    
    func _testingCurrentHeight() -> Future<ConsensusChainHeight.CurrentHeight> {
        self.eventLoop.submit {
            self.stateMachine.currentHeight
        }
    }
    
    func _testingSetState(_ state: Node.ReactorState) -> Future<Void> {
        self.eventLoop.submit {
            self.stateMachine._testingSetState(state)
        }
    }
    
    func _testingState() -> Future<Node.ReactorState> {
        self.eventLoop.submit {
            self.stateMachine._testingState()
        }
    }
    
    func _testingMuxerDelegate() -> Future<MuxerDelegateProtocol> {
        self.eventLoop.submit {
            self.stateMachine
        }
    }
    
    static func logError<T>(event: StaticString = #function,
                            file: StaticString = #fileID,
                            line: Int = #line) -> (Result<T, Swift.Error>) -> Void {
        { result in
            switch result {
            case .failure(let error):
                logger.error("Node [\(file):\(line)]", event, "- Unexpected Error:", error)
            case .success:
                break
            }
        }
    }
    
    static func nodeNetworkIsReady(muxer: Muxer,
                                   minimum: Int) -> Bool {
        muxer.count() >= minimum
    }
    
    static func terminalStoreFailure(_ caller: StaticString = #function,
                                     file: StaticString = #fileID,
                                     line: Int = #line) -> (Swift.Error) -> Void {
        { error in
            switch error {
            case let error as NIO.IOError where error.errnoCode == 9:
                fallthrough
            case is NIOThreadPoolError.ThreadPoolInactive:
                fallthrough
            case ChannelError.ioOnClosedChannel:
                logger.error("Node Store FAIL [\(file):\(line)]", caller, "- NIOThreadPool or connection closed. Store failed.")
            default:
                logger.error("Node Store FAIL [\(file):\(line)]", caller, "error:", error)
                preconditionFailure()
            }
        }
    }
    
    static func nodeDelegateWith(timeout: NIODeadline,
                                 eventLoop: EventLoop,
                                 delegateFunction: (@escaping () -> Void) -> Void) -> Future<Void> {
        let promise = eventLoop.makePromise(of: Void.self)
        let schedule = eventLoop.scheduleTask(deadline: timeout) {
            promise.fail(Node.Error.timeoutErrorWaitingForDelegate)
        }
        
        let commit: () -> Void = {
            schedule.cancel()
            
            promise.succeed(())
        }
        
        delegateFunction(commit)
        
        return promise.futureResult
    }


    public enum HeadersError: Swift.Error, CustomStringConvertible {
        case couldNotConnectWithBlockHeaderBacklog(BlockChain.Hash<BlockHeaderHash>)
        case emptyPayload(Client.Key)
        case illegalHashChain(first: Int)
        case illegalUnsolicitedHeaders(hash: BlockChain.Hash<BlockHeaderHash>)
        case illegalUnsolicitedHeadersReceivedEmpty
        case internalError(StaticString)
        case unexpectedStopHash
        case illegalRollbackIsSubset

        public var description: String {
            switch self {
            case .couldNotConnectWithBlockHeaderBacklog(let hash):
                return "HeadersError.couldNotConnectWithBlockHeaderBacklog(\(hash))"
            case .emptyPayload(let clientKey):
                return "HeadersError.emptyPayload(\(clientKey))"
            case .illegalHashChain(let first):
                return "HeadersError.illegalHashChain(first: \(first))"
            case .illegalUnsolicitedHeaders(let hash):
                return "HeadersError.illegalUnsolicitedHeaders(hash: \(hash))"
            case .illegalUnsolicitedHeadersReceivedEmpty:
                return "HeadersError.illegalUnsolicitedHeadersReceivedEmpty"
            case .internalError(let string):
                return "HeadersError.internalError(\(string))"
            case .unexpectedStopHash:
                return "HeadersError.unexpectedStopHash"
            case .illegalRollbackIsSubset:
                return "HeadersError.illegalRollbackIsSubset"
            }
        }
    }
    
    public enum Error: Swift.Error {
        case cannotCloseFileHandle(String)
        case closeAlreadyCalled(currentFuture: Future<Void>)
        case invalidState(String, StaticString)
        case muxerNoCFNode
        case muxerNoClient
        case muxerNoFullNode
        case rollback(newHeight: Int)
        case timeoutErrorWaitingForDelegate
    }
}

// MARK: EventLoopQueueing
#if canImport(Combine)
import protocol Combine.Cancellable
protocol EventLoopQueueingCancellable: Cancellable {
    func cancel()
}
#else
protocol EventLoopQueueingCancellable {
    func cancel()
}
#endif
struct EventLoopQueueingCancel: Error {}


protocol AlmostCompleteProtocol {
    var almostComplete: Bool { get }
}
