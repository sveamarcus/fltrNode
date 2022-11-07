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
import Dispatch
import Foundation
import NIO
import NIOConcurrencyHelpers

final class Muxer {
    private let lock: NIOConcurrencyHelpers.NIOLock = .init()
    private var stateMachine = Muxer.StateMachine()
    var muxerDelegate: Optional<MuxerDelegateProtocol>
    private let clientFactory: ClientFactory
    private let unsolicitedFilters = UnsolicitedFilters()
    private let timer: RepeatingTimer
    
    @Setting(\.BITCOIN_NUMBER_OF_CLIENTS) private var maxConnections: Int
    
    init(muxerDelegate: MuxerDelegateProtocol?, clientFactory: ClientFactory) {
        self.muxerDelegate = muxerDelegate
        self.clientFactory = clientFactory
        
        self.timer = .init(interval: Settings.MUXER_RECONNECT_TIMER)
        try! self.timer.start(.init {
            self.renewConnections()
        })
    }
}

extension Muxer {
    func renewConnections() {
        let action = self.lock.withLock {
            self.stateMachine.requestConnections()
        }
        
        self.execute(action)
    }
    
    func stop() {
        let action: Action = self.lock.withLock {
            return self.stateMachine.requestShutdown()
        }
        
        self.execute(action)
    }
    
    private func clientFromConnectFuture(_ client: Client) {
        let action = self.lock.withLock {
            self.stateMachine.provideNewClient(client)
        }
        
        self.execute(action)
    }
    
    private func cancelFromConnectFuture() {
        let action = self.lock.withLock {
            self.stateMachine.cancelPendingConnection()
        }
        
        self.execute(action)
    }
    
    
    func execute(_ action: Action) {
        switch action {
        case .makeConnections(let number):
            let clients: [Future<Client>] = (0..<number).map { _ in
                self.lock.withLock {
                    self.clientFactory(delegate: self)
                }
            }
            
            clients.forEach { clientFuture in
                clientFuture.whenComplete {
                    switch $0 {
                    case .success(let client):
                        client.connectFuture.whenComplete { result in
                            switch result {
                            case .success(()):
                                client.closeFuture.whenComplete { _ in
                                    self.dead(client)
                                }
                                
                                self.clientFromConnectFuture(client)
                            case .failure(let error):
                                logger.error("Muxer \(#function) - Connection failed with error \(error)")
                                self.cancelFromConnectFuture()
                            }
                        }
                    case .failure(let error):
                        logger.error("Muxer \(#function) - Client factory failed with error \(error)")
                        self.cancelFromConnectFuture()
                    }
                }
            }
        case .guessHeight(let client):
            (try? client.versionHeight())
            .map {
                self.muxerDelegate?.guess(height: $0)
            }
        case .stopped:
            try! self.timer.stop()
            self.muxerDelegate?.muxerStopComplete()
            self.muxerDelegate = nil
        case .acquire, .acquireAll:
            preconditionFailure()
        case .none:
            break
        }
    }

    func dead(_ client: Client) {
        let action = self.lock.withLock {
            self.stateMachine.requestRemove(client)
        }
        
        self.execute(action)
    }
    
    func allFuture<T>(eventLoop: EventLoop,
                      _ fn: (Client) -> Future<T>)
    -> Future<[(result: Result<T, Swift.Error>, from: Client.Key)]> {
        let action = self.lock.withLock {
            self.stateMachine.requestAllClients()
        }
        
        guard case let .acquireAll(clients) = action else {
            return eventLoop.makeFailedFuture(Muxer.Error.noActiveConnectionsFound)
        }
        
        let clientKeys = clients.map(Client.Key.init)
        let futures = clients.map(fn)
        return Future.whenAllComplete(futures, on: eventLoop)
        .map { results -> [(result: Result<T, Swift.Error>, from: Client.Key)] in
            zip(results, clientKeys)
            .map { (result: $0.0, from: $0.1) }
        }
    }
    
    func next<T>(_ fn: (Client) -> Future<T>) -> Future<(result: T, from: Client.Key)>? {
        let action = self.lock.withLock {
            self.stateMachine.requestNextClient()
        }
        
        guard case let .acquire(client) = action else {
            return nil
        }

        return fn(client).map {
            (result: $0, from: Client.Key(client))
        }
    }
    
    func next<T>(with predicate: (ServicesOptionSet) -> Bool, _ fn: (Client) -> Future<T>) -> Future<(result: T, from: Client.Key)>? {
        let action = self.lock.withLock {
            self.stateMachine.requestRandom(with: predicate)
        }
        
        guard case let .acquire(client) = action else {
            return nil
        }
        
        return fn(client).map {
            (result: $0, from: Client.Key(client))
        }
    }

    
    func next<T>(_ property: (Client) -> T) -> (result: T, from: Client.Key)? {
        let action = self.lock.withLock {
            self.stateMachine.requestNextClient()
        }
        
        guard case let .acquire(client) = action else {
            return nil
        }
        
        return (result: property(client), from: Client.Key(client))
    }

    func all<T>(_ selector: (Client) throws -> T) -> [(result: T, from: Client.Key)] {
        let action = self.lock.withLock {
            self.stateMachine.requestAllClients()
        }
        
        guard case let .acquireAll(clients) = action else {
            return []
        }
        
        return clients.compactMap {
            guard let result = try? selector($0) else {
                return nil
            }
            
            return (result: result, from: Client.Key($0))
        }
    }
    
    func count() -> Int {
        let action = self.lock.withLock {
            self.stateMachine.requestAllClients()
        }
        
        guard case let .acquireAll(clients) = action else {
            return 0
        }

        return clients.count
    }
    
    func dropForDiversity() {
        let action = self.lock.withLock {
            self.stateMachine.requestAllClients()
        }
        
        guard case let .acquireAll(clients) = action else {
            return
        }

        let dropLimit = (self.maxConnections + 1) / 2

        guard clients.count >= dropLimit else {
            return
        }
        
        var sorted = clients.sorted()[...]
        var dropList = sorted.filter {
            (try? !$0.services().isFilterNode) ?? true
                || (try? !$0.services().isFullNode) ?? true
        }[...]
        
        let remaining = dropLimit - min(dropList.count, dropLimit)
        
        (0 ..< min(dropList.count, dropLimit)).forEach { _ in
            dropList.popFirst()?.close()
        }
        
        (0..<remaining).forEach { _ in
            sorted.popFirst()?.close()
        }
    }
    
    func next() -> Client? {
        let action = self.lock.withLock {
            self.stateMachine.requestNextClient()
        }
        
        guard case let .acquire(client) = action else {
            return nil
        }
        
        return client
    }
    
    func random(with predicate: (ServicesOptionSet) -> Bool = { _ in true }) -> Client? {
        let action = self.lock.withLock {
            self.stateMachine.requestRandom(with: predicate)
        }
        
        guard case let .acquire(client) = action else {
            return nil
        }
        
        return client
    }
    
    func random(with services: ServicesOptionSet) -> Client? {
        self.random(with: { $0.contains(services) })
    }
    
    func next(_ client: Client.Key) -> Client? {
        let action = self.lock.withLock {
            self.stateMachine.requestNamedClient(client)
        }
        
        guard case let .acquire(client) = action else {
            return nil
        }
        
        return client
    }
}

extension Muxer {
    func stateForTesting() -> (state: StateMachine.State, connections: Set<Client.Key>, pending: Int) {
        self.lock.withLock {
            self.stateMachine.stateForTesting()
        }
    }
    
    func setStateForTesting(state: StateMachine.State? = nil, connections: Set<Client.Key>? = nil, pending: Int? = nil) {
        self.lock.withLockVoid {
            self.stateMachine.setStateForTesting(state: state, connections: connections, pending: pending)
        }
    }
}

// MARK: UnsolicitedDelegate
extension Muxer: ClientUnsolicitedDelegate {
    func unsolicited(addr: [PeerAddress], source: Client.Key) {
        if case .pass = self.unsolicitedFilters.unsolicited(addr: addr, source: source) {
            self.muxerDelegate?.unsolicited(addr: addr, source: source)
        }
    }
    
    func unsolicited(block: BlockChain.Hash<BlockHeaderHash>, source: Client.Key) {
        if case .pass = self.unsolicitedFilters.unsolicited(block: block, source: source) {
            self.muxerDelegate?.unsolicited(block: block, source: source)
        }
    }
    
    func unsolicited(tx: BlockChain.Hash<TransactionLegacyHash>, source: Client.Key) {
        if case .pass = self.unsolicitedFilters.unsolicited(tx: tx, source: source) {
            self.muxerDelegate?.unsolicited(tx: tx, source: source)
        }
    }
    
    var mempool: Node.Mempool {
        self.muxerDelegate?.mempool ?? [:]
    }
}

protocol MuxerDelegateProtocol: ClientUnsolicitedDelegate {
    func guess(height: Int)
    func muxerStopComplete()
}

// MARK: Error
extension Muxer {
    enum Error: Swift.Error {
        case noActiveConnectionsFound
    }
}

// MARK: StateMachine
extension Muxer {
    enum Action: Equatable {
        case none
        case makeConnections(Int)
        case guessHeight(Client)
        case stopped
        case acquire(Client)
        case acquireAll([Client])
    }
}
 
extension Muxer {
    struct StateMachine {
        enum State {
            case running
            case stopped
            case stopping
        }

        @Setting(\.BITCOIN_NUMBER_OF_CLIENTS) private var maxConnections: Int
        private var state: State = .running
        private var connections: Set<Client.Key> = .init()
        fileprivate(set) var pending: Int = 0
    }
}

extension Muxer.StateMachine {
    mutating func requestConnections() -> Muxer.Action {
        switch self.state {
        case .running:
            let current = self.connections.count
            assert(self.pending + current <= self.maxConnections)
            let new = self.maxConnections - current - self.pending
            if new > 0 {
                self.pending += new
                return .makeConnections(new)
            } else {
                return .none
            }
        case .stopped, .stopping:
            return .none
        }
    }
    
    mutating func provideNewClient(_ btcClient: Client) -> Muxer.Action {
        self.pending -= 1

        switch self.state {
        case .running:
            self.connections.insert(Client.Key(btcClient))
            
            return .guessHeight(btcClient)
        case .stopping, .stopped:
            btcClient.close()
            
            return .none
        }
    }
    
    mutating func cancelPendingConnection() -> Muxer.Action {
        assert(self.pending > 0)
        self.pending -= 1
        
        return .none
    }
    
    mutating func requestShutdown() -> Muxer.Action {
        switch self.state {
        case .running:
            guard self.connections.count > 0 else {
                self.state = .stopped
                return .stopped
            }
            
            self.state = .stopping
            self.connections.forEach { $0.client.close() }
            return .none
        case .stopping:
            return .none
        case .stopped:
            preconditionFailure()
        }
    }
    
    func requestAllClients() -> Muxer.Action {
        guard case .running = self.state else {
            return .none
        }

        let clients = self.connections.map(\.client).filter {
            $0.isConnected
        }
        
        guard !clients.isEmpty else {
            return .none
        }
        
        return .acquireAll(clients)
    }

    func requestRandom(with filter: (ServicesOptionSet) -> Bool) -> Muxer.Action {
        guard case .running = self.state else {
            return .none
        }
        
        let apply: (Client.Key) -> ServicesOptionSet = {
            return (try? $0.client.services()) ?? []
        }
        
        let filtered = self.connections.filter {
            filter(apply($0))
        }
        
        if let client = filtered.randomElement()?.client {
            return .acquire(client)
        } else {
            return .none
        }
    }

    func requestNextClient() -> Muxer.Action {
        guard case .running = self.state else {
            return .none
        }
        
        let ordered = self.connections.sorted(by: { $0.client.queueDepth < $1.client.queueDepth })
        guard let first = ordered.first?.client else {
            return .none
        }
        
        if let random = ordered
            .prefix(while: { $0.client.queueDepth == first.queueDepth })
            .randomElement()?
            .client {
            return .acquire(random)
        } else {
            return .acquire(first)
        }
    }
    
    func requestNamedClient(_ key: Client.Key) -> Muxer.Action {
        guard case .running = self.state else {
            return .none
        }

        if let index = self.connections.firstIndex(of: key) {
            let client = self.connections[index]
            return .acquire(client.client)
        } else {
            return .none
        }
    }
    
    mutating func requestRemove(_ client: Client) -> Muxer.Action {
        switch self.state {
        case .stopping:
            self.connections.remove(Client.Key(client))
            if self.connections.isEmpty {
                self.state = .stopped
                return .stopped
            }
        case .running:
            self.connections.remove(Client.Key(client))
        case .stopped:
            assert(self.connections.isEmpty)
            break
        }
        
        return .none
    }
}

extension Muxer.StateMachine {
    func stateForTesting() -> (state: State, connections: Set<Client.Key>, pending: Int) {
        (self.state, self.connections, self.pending)
    }
    
    mutating func setStateForTesting(state: State? = nil, connections: Set<Client.Key>? = nil, pending: Int? = nil) {
        state.map {
            self.state = $0
        }
        
        connections.map {
            self.connections = $0
        }
        
        pending.map {
            self.pending = $0
        }
    }
}

// MARK: Client.Key
extension Client {
    public struct Key: Hashable & Equatable {
        let client: Client
        
        var value: ObjectIdentifier {
            ObjectIdentifier(self.client)
        }
        
        init(_ btcClient: Client) {
            self.client = btcClient
        }
        
        public static func == (lhs: Self, rhs: Self) -> Bool {
            ObjectIdentifier(lhs.client) == ObjectIdentifier(rhs.client)
        }
        
        public func hash(into hasher: inout Hasher) {
            hasher.combine(ObjectIdentifier(self.client))
        }
    }
}

// MARK: Description
extension Muxer: CustomStringConvertible {
    var description: String {
        String(describing: self.stateMachine)
    }
}

extension Muxer.StateMachine: CustomStringConvertible {
    var description: String {
        let prefix = "[count:\(self.connections.count)]"
        let connections = self.connections.enumerated().map {
            "[\($0.offset + 1):" + String(describing: $0.element.client) + "]"
        }
        
        return ([ prefix ] + connections).joined(separator: "\n")
    }
}
