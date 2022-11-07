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
import Foundation
import HaByLo
import NIO
import NIOTransportServices
import Network

protocol ClientUnsolicitedDelegate: AnyObject {
    func unsolicited(addr: [PeerAddress], source: Client.Key)
    func unsolicited(block: BlockChain.Hash<BlockHeaderHash>, source: Client.Key)
    func unsolicited(tx: BlockChain.Hash<TransactionLegacyHash>, source: Client.Key)
    
    var mempool: Node.Mempool { get }
}

public class Client {
    @Setting(\.PROTOCOL_SERVICE_OPTIONS) private(set) var protocolServiceOptions: ServicesOptionSet
    @Setting(\.VERSION_VERSION) private(set) var versionVersion: Int32
    @Setting(\.PROTOCOL_USER_AGENT) private(set) var userAgent: String
    @Setting(\.PROTOCOL_RELAY) private(set) var protocolRelay: Bool

    private var unsolicitedDelegate: ClientUnsolicitedDelegate?

    @UnfairLock private var state: ClientState

    fileprivate init(host: IPv6Address,
                     port: PeerAddress.IPPort,
                     eventLoop: EventLoop,
                     pendingHandler: PendingCommandHandler,
                     queueHandler: QueueHandler,
                     connectFuture: Future<Void>,
                     delegate: ClientUnsolicitedDelegate? = nil) {
        
        self.unsolicitedDelegate = delegate
        
        self.state = .init(eventLoop: eventLoop)
        try! state.initialize(host: host,
                              port: port,
                              eventLoop: eventLoop,
                              pendingHandler: pendingHandler,
                              queueHandler: queueHandler,
                              connectFuture: connectFuture)
    }

    #if DEBUG
    deinit {
        logger.trace("Client.deinit 🌚 was at state(", self.state, ")")
    }
    #endif

    var queueDepth: Int {
        do {
            let pending = try self.state.getPendingHandler().isPending ? 1 : 0
            let queueCount = try self.state.getQueueHandler().count
            return pending + queueCount
        } catch {
            return 0
        }
    }
    
    var mempool: Node.Mempool {
        self.unsolicitedDelegate?.mempool ?? [:]
    }
    
    // Testing
    var _embedded: EmbeddedChannelLocked? {
        try? self.state.getChannel() as? EmbeddedChannelLocked
    }
}

// MARK: Connect
extension Client {
    static func connect(eventLoop: EventLoop,
                        host: IPv6Address,
                        port: PeerAddress.IPPort,
                        height: Int,
                        delegate: ClientUnsolicitedDelegate?) -> Client {
        let timeout = Settings.BITCOIN_COMMAND_TIMEOUT
        let throttleTimeout = Settings.BITCOIN_COMMAND_THROTTLE_TIMEOUT
        
        let protocolServiceOptions = Settings.PROTOCOL_SERVICE_OPTIONS
        let versionVersion = Settings.VERSION_VERSION
        let userAgent = Settings.PROTOCOL_USER_AGENT
        let protocolRelay = Settings.PROTOCOL_RELAY

        func version() -> VersionCommand {
            let recipientAddress = PeerAddress(time: 0,
                                               services: [],
                                               address: .v6(host),
                                               port: port)
            let senderAddress = PeerAddress(time: 0,
                                            services: protocolServiceOptions,
                                            address: .v6(.zero),
                                            port: 0)
            let versionCommand = VersionCommand(versionNumber: versionVersion,
                                                services: protocolServiceOptions,
                                                timestamp: Date(),
                                                recipientAddress: recipientAddress,
                                                senderAddress: senderAddress,
                                                nonce: .random(in: 0 ... .max),
                                                userAgent: userAgent,
                                                startHeight: UInt32(height),
                                                relay: protocolRelay)
            return versionCommand
        }

        
        let versionPromise: Promise<VersionCommand> = eventLoop.makePromise()
        let pendingHandler = PendingCommandHandler()
        let queueHandler = QueueHandler()
        
        func embeddedConnect(eventLoop: EmbeddedEventLoopLocked) -> Future<Channel> {
            let channel = EmbeddedChannelLocked(loop: eventLoop)
            let socketAddress: SocketAddress = try! .init(ipAddress: "1.1.1.1", port: 1)
            
            versionPromise.succeed(version())
            
            return channel.pipeline.addHandlers([
                IdleStateHandler(allTimeout: throttleTimeout),
                PrintBitcoinCommandValueHandler(),
                BundleHandler(),
                PrepareBundleRequestHandler(),
                pendingHandler,
                LatencyDecoratorHandler(),
                PendingTimeoutHandler(),
                queueHandler,
            ]).flatMap {
                channel.connect(to: socketAddress)
            }.map {
                channel
            }
        }
        
        func niotsConnect(eventLoop: EventLoop) -> Future<Channel>? {
            NIOTSConnectionBootstrap(validatingGroup: eventLoop)?
            .connectTimeout(timeout)
            .channelInitializer { channel in
                #if DEBUG
                channel.pipeline.addHandlers([
                    IdleStateHandler(allTimeout: throttleTimeout),
//                    PrintEverythingHandler(),
                    ByteToMessageHandler(FrameDecoder()),
                    CommandWriterHandler(),
                    FrameToCommandHandler(),
                    VersionHandshakeHandler(done: versionPromise,
                                            outgoing: version()),
                    PendingToValueHandler(),
                    PrintBitcoinCommandValueHandler(),
                    BundleHandler(),
                    PrepareBundleRequestHandler(),
                    pendingHandler,
                    LatencyDecoratorHandler(),
                    PendingTimeoutHandler(),
                    queueHandler,
                ])
                #else
                channel.pipeline.addHandlers([
                    IdleStateHandler(allTimeout: throttleTimeout),
                    ByteToMessageHandler(FrameDecoder()),
                    CommandWriterHandler(),
                    FrameToCommandHandler(),
                    VersionHandshakeHandler(done: versionPromise,
                                            outgoing: version()),
                    PendingToValueHandler(),
                    BundleHandler(),
                    PrepareBundleRequestHandler(),
                    pendingHandler,
                    LatencyDecoratorHandler(),
                    PendingTimeoutHandler(),
                    queueHandler,
                ])
                #endif
            }
            .channelOption(NIOTSChannelOptions.waitForActivity, value: false)
            .connect(endpoint: .hostPort(host: .ipv6(host),
                                         port: NWEndpoint.Port(rawValue: port)!)
            )
        }
        
        let channelFuture: Future<Channel> 
        switch (eventLoop, niotsConnect(eventLoop: eventLoop)) {
        case (_, .some(let channel)):
            channelFuture = channel
        case (let el as EmbeddedEventLoopLocked, _):
            channelFuture = embeddedConnect(eventLoop: el)
        default:
            fatalError("Client only supports embedded or NIOTS EventLoops currently")
        }
        
        let connectPromise: Promise<Void> = eventLoop.makePromise(file: #file, line: #line)
        let client = Client(host: host,
                            port: port,
                            eventLoop: eventLoop,
                            pendingHandler: pendingHandler,
                            queueHandler: queueHandler,
                            connectFuture: connectPromise.futureResult,
                            delegate: delegate)
            
        let connection: Future<Void> = channelFuture
        .flatMapThrowing { channel -> (Client, Channel) in
            try client.state.connect(channel: channel)
            return (client, channel)
        }
        .flatMap { clientChannel in
            clientChannel.1.pipeline.addHandler(DelegatingHandler(clientChannel.0), position: .last)
        }
        
        connectPromise.futureResult
        .whenSuccess { _ in
            let mempool = delegate?.mempool ?? [:]
            let inventory: [Inventory] = mempool.keys.map(Inventory.tx)
            guard inventory.count > 0
            else {
                return
            }
            
            connectPromise.futureResult.whenSuccess {
                client.immediate(InvCommand(inventory: inventory), promise: nil)
            }
        }
        
        connection.cascadeFailure(to: versionPromise)
        connection.and(versionPromise.futureResult)
        .whenComplete {
            switch $0 {
            case .failure(let error):
                logger.error("Client", #function, "- Connection failed with error (", error, ")")
                client.state.error(error)
                connectPromise.fail(error)
                client.unsolicitedDelegate = nil
            case .success(let (_, versionFuture)):
                do {
                    try client.state.connected(version: versionFuture)
                    client.state.closeFuture.whenComplete { _ in
                        client.unsolicitedDelegate = nil
                    }
                    connectPromise.succeed(())
                } catch {
                    client.state.error(error)
                    connectPromise.fail(error)
                }
            }
        }

        return client
    }
}

// MARK: Outgoing
extension Client {
    func close() {
        self.state.disconnect()
    }
    
    // TODO: Evaluate promise based command for better cancellation handling
//    func pending<T: BitcoinCommandValue>(_ command: BitcoinCommandValue, promise: EventLoopPromise<T>) {
//        let pendingCommandPromise = self.state.eventLoop.makePromise(of: BitcoinCommandValue.self,
//                                                                     file: #file, line: #line)
//        pendingCommandPromise
//        .futureResult
//        .map { $0 as! T }
//        .cascade(to: promise)
//
//        let pendingCommand = PendingBitcoinCommand(
//            promise: pendingCommandPromise,
//            bitcoinCommandValue: command
//        )
//        self.state.writeAndFlush(pendingCommand)
//        .whenComplete {
//            switch $0 {
//            case .success:
//                command.writtenToNetworkPromise?.succeed(())
//            case .failure(let error):
//                command.writtenToNetworkPromise?.succeed(())
//                pendingCommandPromise.fail(error)
//            }
//        }
//    }
    
    func pending<T: BitcoinCommandValue>(_ command: T) -> Future<BitcoinCommandValue> {
        let pendingCommandPromise = self.state.eventLoop.makePromise(of: BitcoinCommandValue.self,
                                                                     file: #file, line: #line)
        let pendingCommand = PendingBitcoinCommand(
            promise: pendingCommandPromise,
            bitcoinCommandValue: command
        )
        self.state.writeAndFlush(
            pendingCommand
        )
        .whenComplete {
            switch $0 {
            case .success:
                command.writtenToNetworkPromise?.succeed(())
            case .failure(let error):
                command.writtenToNetworkPromise?.succeed(())
                pendingCommandPromise.fail(error)
            }
            
        }

        return pendingCommandPromise.futureResult
    }
    
    func immediate<T: BitcoinCommandValue>(_ value: T) -> Future<Void> {
        self.state.writeAndFlush(value)
    }
    
    func immediate<T: BitcoinCommandValue>(_ value: T, promise: Promise<Void>?) {
        self.state.writeAndFlush(value, promise: promise)
    }
}

// MARK: Incoming/HandlerDelegate
extension Client: BitcoinCommandHandlerDelegate {
    func channelError(_ error: Error) {
        self.state.error(error)
    }
    
    func incoming(_ incomingCommand: FrameToCommandHandler.IncomingCommand) {
        switch incomingCommand {
        case .ping(let ping):
            self.immediate(PongCommand(nonce: ping.nonce), promise: nil)
        case .getdata(let getdataCommand):
            let mempool = self.unsolicitedDelegate?.mempool ?? [:]
            
            enum TxOrNotfound {
                case tx(Tx.Transaction)
                case notfound(Inventory)
            }
            
            let result: [TxOrNotfound] = getdataCommand.inventory.map {
                switch $0 {
                case .tx(let hash), .witnessTx(let hash):
                    guard let tx = mempool[hash]
                    else { return .notfound($0) }
                    
                    return .tx(tx)
                case .block, .compactBlock, .error, .filteredBlock, .witnessBlock:
                    return .notfound($0)
                }
            }
            let txs: [Tx.Transaction] = result.compactMap {
                switch $0 {
                case .tx(let tx):
                    return tx
                case .notfound:
                    return nil
                }
            }
            let notfounds: [Inventory] = result.compactMap {
                switch $0 {
                case .notfound(let inventory):
                    return inventory
                case .tx:
                    return nil
                }
            }

            txs.forEach {
                self.immediate($0, promise: nil)
            }
            
            if notfounds.count > 0 {
                self.immediate(NotfoundCommand(inventory: notfounds), promise: nil)
            }
        case .getheaders(let getheadersCommand):
            let locator = getheadersCommand.blockLocator
            let lastLocator = locator.hashes.last
            logger.trace("Client", #function,
                         "- Incoming getheaders with #hashes[", locator.hashes.count,
                         "] lastHash[", lastLocator ?? "none",
                         "] stopHash[", locator.stop, "]")
            let inventory = locator.hashes.map {
                Inventory.witnessBlock($0)
            }
            
            let notfound = NotfoundCommand(inventory: inventory)
            self.immediate(notfound, promise: nil)
        case .version:
            logger.error("Client", #function, "- VersionCommand resent while in connected state.")
            self.state.error(BitcoinNetworkError.illegalVersion)
        case .getaddr, .getcfcheckpt, .getcfilters, .getcfheaders:
            logger.error("Client", #function, "- Received unimplemented command[", incomingCommand, "] CLOSING CONNECTION")
            self.close()
        case .unhandled:
            logger.error("Client", #function, "- Not implemented[", incomingCommand, "]")
            assertionFailure("not implemented \(String(reflecting: incomingCommand))")
        }
    }
    
    func unsolicited(_ unsolicitedCommand: FrameToCommandHandler.UnsolicitedCommand) {
        switch unsolicitedCommand {
        case .addr(let addrCommand):
            logger.trace("Client", #function,
                         " - Addr received with address count", addrCommand.addresses.count)
            self.updateServices(addrCommand)
            
            self.state.eventLoop.assertInEventLoop()
            self.unsolicitedDelegate?.unsolicited(addr: addrCommand.addresses, source: Key(self))
        case .alert(let alert):
            logger.info("Client", #function, "- Alert received (", alert, ")")
        case .feefilter(let feefilterCommand):
            try? self.state.setFeefilter(feefilterCommand.feerate)
        case .inv(let invCommand):
            unsolicitedInv(invCommand)
        case .reject(let message):
            logger.info("Client \(#function) - CLOSING. Reject received (\(message))")
            self.close()
        case .sendaddrv2:
            logger.trace("Client \(#function) - Ignoring sendaddrv2 command")
        case .sendcmpct(let command):
            if let current = self.state.sendcmpct {
                guard command.version > current.version else {
                    logger.trace("Client", #function, "- Inferior sendcmpct proposal received version [", command.version, "] including new blocks [", command.includingNewBlocks ,"]")
                    return
                }
            }
            logger.trace("Client", #function, "- Sendcmpct received version [", command.version, "] including new blocks [", command.includingNewBlocks ,"]")
            try? self.state.setSendcmpct(command)
        case .sendheaders(let command):
            try? self.state.setSendheaders(command)
        }
    }

    private func updateServices(_ addr: AddrCommand) {
        do {
            guard let addrFiltered = addr.addresses.filter({
                $0.address == self.state.hostPort?.host ?? .loopback
                    && $0.port == self.state.hostPort?.port ?? 0
            }).last else {
                return
            }
            
            try self.state.setServices(addrFiltered.services)
        } catch {
            logger.error("Client", #function,
                         "- Could not set updated services configuration [", error, "]")
        }
    }
    
    private func unsolicitedInv(_ invCommand: InvCommand) {
        self.state.eventLoop.assertInEventLoop()

        for inv in invCommand.inventory {
            switch inv {
            case .block(let hash), .witnessBlock(let hash):
                self.unsolicitedDelegate?.unsolicited(block: hash, source: Key(self))
            case .tx(let hash), .witnessTx(let hash):
                self.unsolicitedDelegate?.unsolicited(tx: hash, source: Key(self))
            case .error, .compactBlock, .filteredBlock:
                continue
            }
        }
    }
}

// MARK: ClientState
extension Client {
    struct ClientState {
        enum State {
            case disconnected(EventLoop)
            case initialized(InitializedState)
            case connecting(ConnectingState)
            case connected(ConnectedState)
            case error(Error, EventLoop)
        }
        
        private var state: State
        
        init(eventLoop: EventLoop) {
            self.state = .disconnected(eventLoop)
        }
        
        enum ClientStateError: Error, CustomStringConvertible, LocalizedError {
            case invalidTransition(state: State, action: StaticString)

            var errorDescription: String? { self.description }
            
            var description: String {
                switch self {
                case .invalidTransition(let state, let action):
                    return "invalidTransition: ERROR during state (\(state)) for action (\(action))"
                }
            }
        }
        
        struct InitializedState: ClientStateImmutablePropertyDelegate {
            final class _Immutable {
                let host: IPv6Address
                let port: PeerAddress.IPPort
                let eventLoop: EventLoop
                let pendingHandler: PendingCommandHandler
                let queueHandler: QueueHandler
                let connectFuture: Future<Void>
                let closePromise: Promise<Void>

                init(host: IPv6Address,
                     port: PeerAddress.IPPort,
                     eventLoop: EventLoop,
                     pendingHandler: PendingCommandHandler,
                     queueHandler: QueueHandler,
                     connectFuture: Future<Void>,
                     closePromise: Promise<Void>) {
                    self.host = host
                    self.port = port
                    self.eventLoop = eventLoop
                    self.pendingHandler = pendingHandler
                    self.queueHandler = queueHandler
                    self.connectFuture = connectFuture
                    self.closePromise = closePromise
                }
            }

            let _immutable: _Immutable
        }
        
        struct ConnectingState: ClientStateImmutablePropertyDelegate {
            let _immutable: InitializedState._Immutable
            let channel: Channel
            
            init(_ initializedState: InitializedState, channel: Channel) {
                self._immutable = initializedState._immutable
                self.channel = channel
                
                channel.closeFuture.cascade(to: self.closePromise)
            }
        }
        
        final class ConnectedState: ClientStateImmutablePropertyDelegate {
            let _immutable: InitializedState._Immutable
            let channel: Channel
            let relay: Bool
            
            var services: ServicesOptionSet
            let userAgent: String
            let version: Int
            let versionHeight: Int
            var feefilter: Optional<UInt64> = .none
            var sendcmpct: Optional<SendcmpctCommand> = .none
            var sendheaders: Optional<SendheadersCommand> = .none
            
            init(_ connectingState: ConnectingState, version: VersionCommand) {
                self._immutable = connectingState._immutable
                self.channel = connectingState.channel
                self.relay = version.relay
                self.services = version.services
                self.userAgent = version.userAgent
                self.version = Int(version.versionNumber)
                self.versionHeight = Int(version.startHeight)
            }
        }
    }
}

// MARK: State transitions
fileprivate extension Client.ClientState {
    mutating func initialize(host: IPv6Address,
                             port: PeerAddress.IPPort,
                             eventLoop: EventLoop,
                             pendingHandler: PendingCommandHandler,
                             queueHandler: QueueHandler,
                             connectFuture: Future<Void>) throws {
        switch self.state {
        case .disconnected:
            self.state = .initialized(
                InitializedState(_immutable: InitializedState._Immutable(
                    host: host,
                    port: port,
                    eventLoop: eventLoop,
                    pendingHandler: pendingHandler,
                    queueHandler: queueHandler,
                    connectFuture: connectFuture,
                    closePromise: eventLoop.makePromise(of: Void.self)
                ))
            )
        case .connected, .connecting, .error, .initialized:
            throw ClientStateError.invalidTransition(state: self.state, action: #function)
        }
    }
}
    
fileprivate extension Client.ClientState {
    mutating func connect(channel: Channel) throws {
        switch self.state {
        case .initialized(let state):
            self.state = .connecting(
                ConnectingState(state, channel: channel)
            )
        case .connected, .connecting, .disconnected, .error:
            throw ClientStateError.invalidTransition(state: self.state, action: #function)
        }
    }
}

fileprivate extension Client.ClientState {
    mutating func connected(version: VersionCommand) throws {
        switch self.state {
        case .connecting(let state):
            self.state = .connected(
                ConnectedState(state, version: version)
            )
        case .connected, .disconnected, .error, .initialized:
            throw ClientStateError.invalidTransition(state: self.state, action: #function)
        }
    }
}

fileprivate extension Client.ClientState {
    mutating func disconnect() {
        switch self.state {
        case .connected(let state):
            state.channel.close(promise: nil)
            self.state = .disconnected(state.eventLoop)
        case .connecting(let state):
            state.channel.close(promise: nil)
            self.state = .disconnected(state.eventLoop)
        case .disconnected, .error, .initialized:
            break
        }
    }
}

fileprivate extension Client.ClientState {
    mutating func error(_ error: Error) {
        switch self.state {
        case .connected(let state):
            self.state = .error(error, state.eventLoop)
        case .connecting(let state):
            state.closePromise.fail(error)
            self.state = .error(error, state.eventLoop)
        case .disconnected(let eventLoop):
            self.state = .error(error, eventLoop)
        case .initialized(let state):
            state.closePromise.fail(error)
            self.state = .error(error, state.eventLoop)
        case .error(let oldError, _):
            logger.error("Client", #function, "- Calling error [", error,
                         "] on already failed client [", oldError, "]")
        }
    }
}

// MARK: State Accessors/getters
extension Client.ClientState {
    var connectFuture: Future<Void> {
        switch self.state {
        case .connected(let state as ClientStateImmutablePropertyDelegate),
             .connecting(let state as ClientStateImmutablePropertyDelegate),
             .initialized(let state as ClientStateImmutablePropertyDelegate):
            return state.connectFuture
        case .disconnected(let eventLoop):
            return eventLoop.makeFailedFuture(BitcoinNetworkError.disconnected)
        case .error(let error, let eventLoop):
            return eventLoop.makeFailedFuture(error)
        }
    }
    
    var closeFuture: Future<Void> {
        switch self.state {
        case .connected(let state as ClientStateImmutablePropertyDelegate),
             .connecting(let state as ClientStateImmutablePropertyDelegate),
             .initialized(let state as ClientStateImmutablePropertyDelegate):
            return state.closePromise.futureResult
        case .disconnected(let eventLoop), .error(_, let eventLoop):
            return eventLoop.makeSucceededFuture(())
        }
    }
    
    var eventLoop: EventLoop {
        switch self.state {
        case .connected(let state as ClientStateImmutablePropertyDelegate),
             .connecting(let state as ClientStateImmutablePropertyDelegate),
             .initialized(let state as ClientStateImmutablePropertyDelegate):
            return state.eventLoop
        case .disconnected(let eventLoop):
            return eventLoop
        case .error(_, let eventLoop):
            return eventLoop
        }
    }
    
    var feefilter: UInt64? {
        switch self.state {
        case .connected(let state):
            return state.feefilter
        case .connecting, .disconnected, .error, .initialized:
            return nil
        }
    }
    
    func getChannel() throws -> Channel {
        switch self.state {
        case .connected(let state):
            return state.channel
        case .connecting(let state):
            return state.channel
        case .disconnected, .error, .initialized:
            throw ClientStateError.invalidTransition(state: self.state, action: #function)
        }
    }
    
    func getPendingHandler() throws -> PendingCommandHandler {
        switch self.state {
        case .connected(let state as ClientStateImmutablePropertyDelegate),
             .connecting(let state as ClientStateImmutablePropertyDelegate):
            return state.pendingHandler
        case .disconnected, .error, .initialized:
            throw ClientStateError.invalidTransition(state: self.state, action: #function)
        }
    }
    
    func getQueueHandler() throws -> QueueHandler {
        switch self.state {
        case .connected(let state as ClientStateImmutablePropertyDelegate),
             .connecting(let state as ClientStateImmutablePropertyDelegate):
            return state.queueHandler
        case .disconnected, .error, .initialized:
            throw ClientStateError.invalidTransition(state: self.state, action: #function)
        }
    }
    
    func getServices() throws -> ServicesOptionSet {
        switch self.state {
        case .connected(let state):
            return state.services
        case .connecting, .disconnected, .error, .initialized:
            throw ClientStateError.invalidTransition(state: self.state, action: #function)
        }
    }
    
    func getUserAgent() throws -> String {
        switch self.state {
        case .connected(let state):
            return state.userAgent
        case .connecting, .disconnected, .error, .initialized:
            throw ClientStateError.invalidTransition(state: self.state, action: #function)
        }
    }
    
    func getProtocolVersion() throws -> Int {
        switch self.state {
        case .connected(let state):
            return state.version
        case .connecting, .disconnected, .error, .initialized:
            throw ClientStateError.invalidTransition(state: self.state, action: #function)
        }
    }
    
    func getVersionHeight() throws -> Int {
        switch self.state {
        case .connected(let state):
            return state.versionHeight
        case .connecting, .disconnected, .error, .initialized:
            throw ClientStateError.invalidTransition(state: self.state, action: #function)
        }
    }
    
    var hasError: Error? {
        switch self.state {
        case .error(let error, _):
            return error
        case .connected, .connecting, .disconnected, .initialized:
            return nil
        }
    }
    
    var hostPort: (host: IPv6Address, port: PeerAddress.IPPort)? {
        switch self.state {
        case .initialized(let state as ClientStateImmutablePropertyDelegate),
             .connecting(let state as ClientStateImmutablePropertyDelegate),
             .connected(let state as ClientStateImmutablePropertyDelegate):
            return (state.host, state.port)
        case .disconnected, .error:
            return nil
        }
    }

    var isConnected: Bool {
        switch self.state {
        case .connected:
            return true
        case .disconnected, .connecting, .error, .initialized:
            return false
        }
    }
    
    var sendcmpct: SendcmpctCommand? {
        switch self.state {
        case .connected(let state):
            return state.sendcmpct
        case .connecting, .disconnected, .error, .initialized:
            return nil
        }
    }
    
    var sendheaders: SendheadersCommand? {
        switch self.state {
        case .connected(let state):
            return state.sendheaders
        case .connecting, .disconnected, .error, .initialized:
            return nil
        }
    }
}

// MARK: State Setters
fileprivate extension Client.ClientState {
    func setFeefilter(_ feefilter: UInt64) throws {
        switch self.state {
        case .connected(let state):
            state.feefilter = feefilter
        case .connecting, .disconnected, .error, .initialized:
            throw ClientStateError.invalidTransition(state: self.state, action: #function)
        }
    }
}

fileprivate extension Client.ClientState {
    func setServices(_ services: ServicesOptionSet) throws {
        switch self.state {
        case .connected(let state):
            state.services = services
        case .connecting, .disconnected, .error, .initialized:
            throw ClientStateError.invalidTransition(state: self.state, action: #function)
        }
    }
}

fileprivate extension Client.ClientState {
    func setSendcmpct(_ sendcmpct: SendcmpctCommand) throws {
        switch self.state {
        case .connected(let state):
            state.sendcmpct = sendcmpct
        case .connecting, .disconnected, .error, .initialized:
            throw ClientStateError.invalidTransition(state: self.state, action: #function)
        }
    }
}

fileprivate extension Client.ClientState {
    func setSendheaders(_ sendheaders: SendheadersCommand) throws {
        switch self.state {
        case .connected(let state):
            state.sendheaders = sendheaders
        case .connecting, .disconnected, .error, .initialized:
            throw ClientStateError.invalidTransition(state: self.state, action: #function)
        }
    }
}

// MARK: Public/Internal accessors
extension Client {
    var closeFuture: Future<Void> {
        self.state.closeFuture
    }
    
    var connectFuture: Future<Void> {
        self.state.connectFuture
    }
    
    var eventLoop: EventLoop {
        self.state.eventLoop
    }

    var hostPort: (host: IPv6Address, port: PeerAddress.IPPort)? {
        self.state.hostPort
    }
    
    var isConnected: Bool {
        self.state.isConnected
    }
    
    func services() throws -> ServicesOptionSet {
        try self.state.getServices()
    }

    func versionHeight() throws -> Int {
        try self.state.getVersionHeight()
    }
}

// MARK: Write/WriteAndFlush
fileprivate extension Client.ClientState {
    private enum WritePayload {
        case immediate(BitcoinCommandValue)
        case immediateAndFlush(BitcoinCommandValue)
        case pending(PendingBitcoinCommand)
        case pendingAndFlush(PendingBitcoinCommand)
    }
    
    private func write(_ mode: WritePayload) -> Future<Void> {
        let writePromise = self.eventLoop.makePromise(of: Void.self,
                                                      file: #file, line: #line)
        self.write(mode, promise: writePromise)
        return writePromise.futureResult
    }
    
    private func write(_ mode: WritePayload, promise: Promise<Void>?) {
        func connectChainedWrite(connectFuture: Future<Void>,
                                 writePromise: Promise<Void>?,
                                 getChannel: @escaping () throws -> Channel,
                                 flushing: Bool,
                                 value: QueueHandler.OutboundIn) {
            connectFuture.whenComplete {
                switch ($0, Result { try getChannel() }) {
                case (.failure(let error), _):
                    writePromise?.fail(error)
                case (.success, .failure(let error)):
                    writePromise?.fail(error)
                case (.success, .success(let channel)):
                    if flushing {
                        channel.writeAndFlush(value, promise: writePromise)
                    } else {
                        channel.write(value, promise: writePromise)
                    }
                }
            }
        }
        
        switch (self.state, mode) {
        case (.connecting(let state as ClientStateImmutablePropertyDelegate), .immediateAndFlush(let value)),
             (.initialized(let state as ClientStateImmutablePropertyDelegate), .immediateAndFlush(let value)):
            connectChainedWrite(connectFuture: state.connectFuture,
                                writePromise: promise,
                                getChannel: self.getChannel,
                                flushing: true,
                                value: .immediate(value))
        case (.connecting(let state as ClientStateImmutablePropertyDelegate), .immediate(let value)),
             (.initialized(let state as ClientStateImmutablePropertyDelegate), .immediate(let value)):
            connectChainedWrite(connectFuture: state.connectFuture,
                                writePromise: promise,
                                getChannel: self.getChannel,
                                flushing: false,
                                value: .immediate(value))
        case (.connecting(let state as ClientStateImmutablePropertyDelegate), .pendingAndFlush(let value)),
             (.initialized(let state as ClientStateImmutablePropertyDelegate), .pendingAndFlush(let value)):
            connectChainedWrite(connectFuture: state.connectFuture,
                                writePromise: promise,
                                getChannel: self.getChannel,
                                flushing: true,
                                value: .pending(value))
        case (.connecting(let state as ClientStateImmutablePropertyDelegate), .pending(let value)),
             (.initialized(let state as ClientStateImmutablePropertyDelegate), .pending(let value)):
            connectChainedWrite(connectFuture: state.connectFuture,
                                writePromise: promise,
                                getChannel: self.getChannel,
                                flushing: false,
                                value: .pending(value))
        case (.connected(let state), .immediateAndFlush(let value)):
            state.channel.writeAndFlush(QueueHandler.OutboundIn.immediate(value), promise: promise)
        case (.connected(let state), .immediate(let value)):
            state.channel.write(QueueHandler.OutboundIn.immediate(value), promise: promise)
        case (.connected(let state), .pendingAndFlush(let value)):
            state.channel.writeAndFlush(QueueHandler.OutboundIn.pending(value), promise: promise)
        case (.connected(let state), .pending(let value)):
            state.channel.write(QueueHandler.OutboundIn.pending(value), promise: promise)
        case (.disconnected, _):
            promise?.fail(BitcoinNetworkError.disconnected)
        case (.error(let error, _), _):
            promise?.fail(error)
        }
    }
    
    func write(_ immediate: BitcoinCommandValue) -> EventLoopFuture<Void> {
        self.write(
            .immediate(immediate)
        )
    }
    
    func write(_ immediate: BitcoinCommandValue, promise: EventLoopPromise<Void>?) {
        self.write(
            .immediate(immediate), promise: promise
        )
    }
    
    func write(_ pending: PendingBitcoinCommand) -> EventLoopFuture<Void> {
        self.write(
            .pending(pending)
        )
    }
    
    func write(_ pending: PendingBitcoinCommand, promise: EventLoopPromise<Void>?) {
        self.write(
            .pending(pending), promise: promise
        )
    }
    
    func writeAndFlush(_ immediate: BitcoinCommandValue) -> EventLoopFuture<Void> {
        self.write(
            .immediateAndFlush(immediate)
        )
    }
    
    func writeAndFlush(_ immediate: BitcoinCommandValue, promise: EventLoopPromise<Void>?) {
        self.write(
            .immediateAndFlush(immediate), promise: promise
        )
    }
    
     func writeAndFlush(_ pending: PendingBitcoinCommand) -> EventLoopFuture<Void> {
        self.write(
            .pendingAndFlush(pending)
        )
     }

     func writeAndFlush(_ pending: PendingBitcoinCommand, promise: EventLoopPromise<Void>?) {
        self.write(
            .pendingAndFlush(pending), promise: promise
        )
     }
}


fileprivate protocol ClientStateImmutablePropertyDelegate {
    var _immutable: Client.ClientState.InitializedState._Immutable { get }
}

extension ClientStateImmutablePropertyDelegate {
    var host: IPv6Address { self._immutable.host }
    var port: PeerAddress.IPPort { self._immutable.port }
    var eventLoop: EventLoop { self._immutable.eventLoop }
    var pendingHandler: PendingCommandHandler { self._immutable.pendingHandler }
    var queueHandler: QueueHandler { self._immutable.queueHandler }
    var connectFuture: Future<Void> { self._immutable.connectFuture }
    var closePromise: Promise<Void> { self._immutable.closePromise }
}

// MARK: CustomStringConvertible
extension Client: CustomStringConvertible {
    public var description: String {
        var string = "Client("
        string += String(describing: ObjectIdentifier(self))
        string += ") state("
        string += String(describing: self.state)
        string += ")"
        return string
    }
}

extension Client.ClientState: CustomStringConvertible {
    public var description: String {
        switch self.state {
        case .connected(let state):
            let host = state.host.asIPv4?.debugDescription ?? state.host.asString
            let services = String(reflecting: state.services)
            let relay = state.relay ? "✅" : "❎"
            let version = (try? self.getProtocolVersion()).map(String.init) ?? "nil"
            let versionHeight = (try? self.getVersionHeight()).map(String.init) ?? "nil"
            let userAgent = (try? self.getUserAgent().trimmingCharacters(in: .init(charactersIn: "/"))) ?? "nil"
            
            return ".connected(\(host):\(state.port)) "
                + "S[\(services)] Relay[\(relay)] V#[\(version)] VHEIGHT#[\(versionHeight)] UA$[\(userAgent)]"
        case .connecting(let state as ClientStateImmutablePropertyDelegate),
             .initialized(let state as ClientStateImmutablePropertyDelegate):
            return ".connecting(\(state.host.asIPv4?.debugDescription ?? state.host.asString):\(state.port))"
        case .disconnected:
            return ".disconnected"
        case .error(let error, _):
            return ".error(\(error))"
        }
    }
}

// MARK: Equatable & Hashable
extension Client: Equatable & Hashable {
    public static func == (lhs: Client, rhs: Client) -> Bool {
        return lhs === rhs
    }
    
    public func hash(into hasher: inout Hasher) {
        withUnsafeBytes(of: state) {
            hasher.combine(bytes: $0)
        }
    }
}

// MARK: Comparable
extension Client: Comparable {
    public static func < (lhs: Client, rhs: Client) -> Bool {
        lhs.queueDepth < rhs.queueDepth
    }
}
