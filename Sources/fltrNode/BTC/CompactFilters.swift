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
import struct fltrTx.ScriptPubKey
import FileRepo
import HaByLo
import NIO
import Stream64

public enum CF {}

// MARK: FilterType
extension CF {
    @usableFromInline
    enum FilterType: UInt8 {
        case basic = 0x00
        
        var parameters: (M: UInt64, P: Int) {
            switch self {
            case .basic:
                return (784931, 19)
            }
        }
    }
}

// MARK: GetcfiltersCommand
extension CF {
    struct GetcfiltersCommand: BitcoinCommandValue {
        let description = "getcfilters"
        let filterType: FilterType
        let startHeight: UInt32
        let stopHash: BlockChain.Hash<BlockHeaderHash>
        
        let timeout: TimeAmount?
        let writtenToNetworkPromise: EventLoopPromise<Void>?

        init(filterType: CF.FilterType,
             startHeight: UInt32,
             stopHash: BlockChain.Hash<BlockHeaderHash>,
             timeout: TimeAmount? = nil,
             writtenToNetworkPromise: EventLoopPromise<Void>? = nil) {
            self.filterType = filterType
            self.startHeight = startHeight
            self.stopHash = stopHash
            self.timeout = timeout
            self.writtenToNetworkPromise = writtenToNetworkPromise
        }
    }
}

extension CF.GetcfiltersCommand: BitcoinCommandEncodable & BitcoinCommandDecodable {
    func write(to buffer: inout ByteBuffer) {
        buffer.writeInteger(self.filterType.rawValue)
        buffer.writeInteger(self.startHeight, endianness: .little)
        buffer.writeBytes(self.stopHash.littleEndian)
    }
    
    init?(fromBuffer: inout ByteBuffer) {
        let save = fromBuffer
        
        guard let filterType = fromBuffer.readInteger(as: UInt8.self).flatMap(CF.FilterType.init(rawValue:)),
            let startHeight: UInt32 = fromBuffer.readInteger(endianness: .little),
            let stopHashLittleEndian: [UInt8] = fromBuffer.readBytes(length: 32)?.reversed() else {
                fromBuffer = save
                return nil
        }
        
        self.init(filterType: filterType,
                  startHeight: startHeight,
                  stopHash: .little(stopHashLittleEndian))
    }
}

// MARK: Composed type for multiple cfilters (bundle)
struct CFBundleCommand: BitcoinCommandValue, CustomDebugStringConvertible {
    let command: CF.GetcfiltersCommand
    let count: Int
    
    init(command: CF.GetcfiltersCommand,
         count: Int) {
        self.command = command
        self.count = count
    }
    
    var debugDescription: String {
        self.command.debugDescription + " #[\(self.count)]"
    }
    
    var description: String {
        self.command.description
    }
    
    func write(to buffer: inout ByteBuffer) {
        self.command.write(to: &buffer)
    }
    
    var timeout: TimeAmount? {
        self.command.timeout
    }
    
    var writtenToNetworkPromise: EventLoopPromise<Void>? {
        self.command.writtenToNetworkPromise
    }
}

// MARK: GetcfheadersCommand
extension CF {
    struct GetcfheadersCommand: BitcoinCommandValue {
        let description = "getcfheaders"
        let filterType: FilterType
        let startHeight: UInt32
        let stopHash: BlockChain.Hash<BlockHeaderHash>
        
        let timeout: TimeAmount?

        init(filterType: CF.FilterType,
             startHeight: UInt32,
             stop: BlockChain.Hash<BlockHeaderHash>,
             timeout: TimeAmount? = nil) {
            self.filterType = filterType
            self.startHeight = startHeight
            self.stopHash = stop
            self.timeout = timeout
        }
    }
}

extension CF.GetcfheadersCommand: BitcoinCommandEncodable & BitcoinCommandDecodable {
    func write(to buffer: inout ByteBuffer) {
        buffer.writeInteger(self.filterType.rawValue)
        buffer.writeInteger(self.startHeight, endianness: .little)
        buffer.writeBytes(self.stopHash.littleEndian)
    }
    
    init?(fromBuffer: inout ByteBuffer) {
        let save = fromBuffer
     
        guard let filterType = fromBuffer.readInteger(as: UInt8.self).flatMap(CF.FilterType.init(rawValue:)),
            let startHeight: UInt32 = fromBuffer.readInteger(endianness: .little),
            let stop: [UInt8] = fromBuffer.readBytes(length: 32) else {
                fromBuffer = save
                return nil
        }
        
        self.init(filterType: filterType, startHeight: startHeight, stop: .little(stop))
    }
}

// MARK: CfheadersCommand
extension CF {
    struct CfheadersCommand: BitcoinCommandValue {
        let description = "cfheaders"
        let filterType: FilterType
        let stop: BlockChain.Hash<BlockHeaderHash>
        let headers: [CF.Header]
    }
}

extension CF.CfheadersCommand: BitcoinCommandEncodable & BitcoinCommandDecodable {
    func write(to buffer: inout ByteBuffer) {
        buffer.writeInteger(self.filterType.rawValue)
        buffer.writeBytes(self.stop.littleEndian)
        if let exists = self.headers.first?.previousHeaderHash.littleEndian {
            buffer.writeBytes(exists)
        } else {
            buffer.writeBytes((0..<32).map { _ in UInt8(0) })
        }
        buffer.writeBytes(self.headers.count.variableLengthCode)
        self.headers.forEach {
            $0.writeFilterHash(&buffer)
        }
    }

    init?(fromBuffer: inout ByteBuffer) {
        let save = fromBuffer
     
        guard let filterType = fromBuffer.readInteger(as: UInt8.self).flatMap(CF.FilterType.init(rawValue:)),
            let stopLittleEndian: [UInt8] = fromBuffer.readBytes(length: 32),
            let previousHeaderHashLittleEndian: [UInt8] = fromBuffer.readBytes(length: 32),
            let hashesCount = fromBuffer.readVarInt() else {
                fromBuffer = save
                return nil
        }
        
        var previousHeaderHash: BlockChain.Hash<CompactFilterHeaderHash> = .little(previousHeaderHashLittleEndian)
        var headers: [CF.Header] = []
        for _ in 0..<hashesCount {
            guard let header = CF.Header(buffer: &fromBuffer,
                                         previousHeaderHash: previousHeaderHash) else {
                fromBuffer = save
                return nil
            }
            headers.append(header)
            
            previousHeaderHash = header.headerHash
        }
        
        self.init(filterType: filterType, stop: .little(stopLittleEndian), headers: headers)
    }
}

// MARK: Header
public extension CF {
    struct Header {
        let filterHash: BlockChain.Hash<CompactFilterHash>
        let previousHeaderHash: BlockChain.Hash<CompactFilterHeaderHash>
        let headerHash: BlockChain.Hash<CompactFilterHeaderHash>
        
        init(filterHash: BlockChain.Hash<CompactFilterHash>,
             previousHeaderHash: BlockChain.Hash<CompactFilterHeaderHash>,
             headerHash: BlockChain.Hash<CompactFilterHeaderHash>) {
            self.filterHash = filterHash
            self.previousHeaderHash = previousHeaderHash
            self.headerHash = headerHash
        }
        
        init?(_ serial: SerialHeader, previousHeader: SerialHeader) {
            self.filterHash = serial.filterHash
            self.headerHash = serial.headerHash

            guard serial.id - 1 == previousHeader.id else {
                return nil
            }
            
            self.previousHeaderHash = previousHeader.headerHash
        }
    }
    
    struct SerialHeader: Identifiable {
        @usableFromInline
        var _recordFlag: File.RecordFlag
        
        @inlinable
        public var recordFlag: File.RecordFlag {
            return self._recordFlag
        }

        public let id: Int
        public let filterHash: BlockChain.Hash<CompactFilterHash>
        public let headerHash: BlockChain.Hash<CompactFilterHeaderHash>
        
        @usableFromInline
        mutating func add(flags: File.RecordFlag) {
            self._recordFlag.formUnion(flags)
        }
        
        @inlinable
        public init(recordFlag: File.RecordFlag = [ .hasFilterHeader ],
             id: Int,
             filterHash: BlockChain.Hash<CompactFilterHash>,
             headerHash: BlockChain.Hash<CompactFilterHeaderHash>) {
            self.id = id
            self.filterHash = filterHash
            self.headerHash = headerHash
            self._recordFlag = recordFlag
        }
        
        init?(_ wire: CF.Header, previousHeader: SerialHeader) {
            self._recordFlag = [ .hasFilterHeader ]
            self.filterHash = wire.filterHash
            self.headerHash = wire.headerHash

            guard previousHeader.headerHash == wire.previousHeaderHash else {
                return nil
            }

            self.id = previousHeader.id + 1
        }
    }
}

extension CF.Header: Equatable {}
extension CF.SerialHeader: Equatable {}

extension CF.Header {
    func witeHeaderHash(_ buffer: inout ByteBuffer) {
        buffer.writeBytes(self.headerHash.littleEndian)
    }
    
    func writeFilterHash(_ buffer: inout ByteBuffer) {
        buffer.writeBytes(self.filterHash.littleEndian)
    }
    
    init?(buffer: inout ByteBuffer, previousHeaderHash: BlockChain.Hash<CompactFilterHeaderHash>) {
        let save = buffer
        
        guard let filterHashLittleEndian: [UInt8] = buffer.readBytes(length: 32) else {
            buffer = save
            return nil
        }
        
        self.filterHash = .little(filterHashLittleEndian)
        self.previousHeaderHash = previousHeaderHash
        
        self.headerHash = self.filterHash.appendHash(previousHeaderHash)
    }
}

// MARK: GetcfcheckptCommand
extension CF {
    struct GetcfcheckptCommand: BitcoinCommandValue {
        let description = "getcfcheckpt"
        let filterType: FilterType
        // TODO: update stop to instead of [UInt8] use BlockChain.Hash
        let stop: [UInt8]
    }
}

extension CF.GetcfcheckptCommand: BitcoinCommandEncodable & BitcoinCommandDecodable {
    func write(to buffer: inout ByteBuffer) {
        buffer.writeInteger(self.filterType.rawValue)
        buffer.writeBytes(self.stop.reversed())
    }
    
    init?(fromBuffer: inout ByteBuffer) {
        let save = fromBuffer
        
        guard let filterType = fromBuffer.readInteger(as: UInt8.self).flatMap(CF.FilterType.init(rawValue:)),
            let stop: [UInt8] = fromBuffer.readBytes(length: 32)?.reversed() else {
                fromBuffer = save
                return nil
        }
        
        self.init(filterType: filterType, stop: stop)
    }
}

// MARK: CfilterCommand
extension CF {
    struct CfilterCommand: BitcoinCommandValue {
        let description = "cfilter"
        
        private var stateMachine: StateMachine
        
        @usableFromInline
        @Setting(\.BITCOIN_CF_GOLOMB_CODED_SET_CLIENT) var golombCodedSetClientFactory: () -> GolombCodedSetClient
    }
}

extension CF.CfilterCommand {
    fileprivate enum StateMachine {
        case wire(WireState)
        case hashAndRank(RankState)
        case valid(FullState)
        case serial(SerialState)
    }
}

// MARK: CfilterCommand / Immutable
protocol CFImmutableProperties {
    var _immutable: CF.CfilterCommand.WireState._Immutable { get }
}

extension CFImmutableProperties {
    var compressed: [UInt8] {
        self._immutable.compressed
    }

    var f: UInt64 {
        UInt64(self.n) * self.filterType.parameters.M
    }
    
    var filterType: CF.FilterType {
        self._immutable.filterType
    }
    
    var n: Int {
        self._immutable.n
    }
}

extension CF.CfilterCommand: CFImmutableProperties {
    var _immutable: WireState._Immutable {
        switch self.stateMachine {
        case .hashAndRank(let state as CFImmutableProperties),
             .serial(let state as CFImmutableProperties),
             .valid(let state as CFImmutableProperties),
             .wire(let state as CFImmutableProperties):
            return state._immutable
        }
    }
}

// MARK: CfilterCommand / CFError
extension CF.CfilterCommand {
    public enum CFError: Error {
        case blockHashMismatch(state: String, event: StaticString, fullHeaderHash: String)
        case blockHeightMismatch(state: String, event: StaticString)
        case invalidFilterData(event: StaticString)
        case invalidState(state: String, event: StaticString)
        case invalidTransition(state: String, event: StaticString)
        case validationFailureFilterHash(computed: BlockChain.Hash<CompactFilterHash>,
                                         expected: BlockChain.Hash<CompactFilterHash>)
        case validationFailureHeaderHash(computed: BlockChain.Hash<CompactFilterHeaderHash>,
                                         expected: BlockChain.Hash<CompactFilterHeaderHash>)
    }
}

// MARK: CfilterCommand / States
extension CF.CfilterCommand {
    struct WireState: CFImmutableProperties {
        final class _Immutable {
            let filterType: CF.FilterType
            let n: Int
            let compressed: [UInt8]

            internal init(filterType: CF.FilterType, n: Int, compressed: [UInt8]) {
                self.filterType = filterType
                self.n = n
                self.compressed = compressed
            }
        }
        
        let _immutable: _Immutable
        fileprivate let blockHash: BlockChain.Hash<BlockHeaderHash>
        fileprivate let filterHash: BlockChain.Hash<CompactFilterHash>
    }
}

extension CF.CfilterCommand {
    fileprivate struct RankState: CFImmutableProperties {
        fileprivate let _immutable: CF.CfilterCommand.WireState._Immutable
        fileprivate let blockHash: BlockChain.Hash<BlockHeaderHash>
        fileprivate let filterHash: BlockChain.Hash<CompactFilterHash>
        fileprivate let blockHeight: Int
    }
}

extension CF.CfilterCommand {
    fileprivate struct FullState: CFImmutableProperties {
        fileprivate let _immutable: CF.CfilterCommand.WireState._Immutable
        fileprivate let blockHash: BlockChain.Hash<BlockHeaderHash>
        fileprivate let filterHash: BlockChain.Hash<CompactFilterHash>
        fileprivate let blockHeight: Int
    }
}

extension CF.CfilterCommand {
    fileprivate struct SerialState: CFImmutableProperties {
        fileprivate let _immutable: CF.CfilterCommand.WireState._Immutable
        fileprivate let blockHeight: Int
    }
}

// MARK: CfilterCommand / Initializer
extension CF.CfilterCommand {
    enum Initializer {
        case serial(filterType: CF.FilterType,
                    blockHeight: Int,
                    n: Int,
                    compressed: [UInt8])
        case wire(filterType: CF.FilterType,
                  blockHash: BlockChain.Hash<BlockHeaderHash>,
                  filterHash: BlockChain.Hash<CompactFilterHash>,
                  n: Int,
                  compressed: [UInt8])
    }

    init(_ i: Initializer) {
        switch i {
        case .serial(let filterType, let blockHeight, let n, let compressed):
            let _immutable: WireState._Immutable = .init(filterType: filterType, n: n, compressed: compressed)
            
            self.stateMachine = .serial(SerialState(_immutable: _immutable,
                                                    blockHeight: blockHeight))
        case .wire(let filterType, let blockHash, let filterHash, let n, let compressed):
            let _immutable: WireState._Immutable = .init(filterType: filterType, n: n, compressed: compressed)

            self.stateMachine = .wire(WireState(_immutable: _immutable,
                                                blockHash: blockHash,
                                                filterHash: filterHash))
        }
    }
}

// MARK: CfilterCommand / Rank Transition
extension CF.CfilterCommand {
    mutating func addRank(from serialHeader: BlockHeader) throws {
        switch self.stateMachine {
        case .wire(let state):
            guard state.blockHash == (try serialHeader.blockHash()) else {
                throw CFError.blockHashMismatch(state: String(describing: state),
                                                event: #function,
                                                fullHeaderHash: try String(describing: serialHeader.blockHash()))
            }
            self.stateMachine = .hashAndRank(
                RankState(_immutable: state._immutable,
                          blockHash: state.blockHash,
                          filterHash: state.filterHash,
                          blockHeight: try serialHeader.height())
            )
        case .hashAndRank, .valid, .serial:
            throw CFError.invalidTransition(state: String(describing: self.stateMachine),
                                            event: #function)
        }
    }
}

// MARK: CfilterCommand / Validate Transition
extension CF.CfilterCommand {
    mutating func validate(cfHeader: CF.Header) throws {
        switch self.stateMachine {
        case .hashAndRank(let state):
            let concatenated = state.filterHash.appendHash(cfHeader.previousHeaderHash)
            
            guard state.filterHash == cfHeader.filterHash
            else {
                throw CFError.validationFailureFilterHash(computed: state.filterHash, expected: cfHeader.filterHash)
            }
            
            guard concatenated == cfHeader.headerHash
            else {
                throw CFError.validationFailureHeaderHash(computed: concatenated, expected: cfHeader.headerHash)
            }
            
            self.stateMachine = .valid(
                FullState(_immutable: state._immutable,
                          blockHash: state.blockHash,
                          filterHash: state.filterHash,
                          blockHeight: state.blockHeight)
            )
        case .serial, .valid, .wire:
            throw CFError.invalidTransition(state: String(describing: self.stateMachine),
                                            event: #function)
        }
    }
}

extension CF {
    @usableFromInline
    enum Key {
        case header(BlockHeader)
        case hash(BlockChain.Hash<BlockHeaderHash>)
        
        var bytes: AnyBidirectionalCollection<UInt8> {
            switch self {
            case .header(let header):
                do {
                    return try header.blockHash().littleEndian.prefix(16)
                } catch {
                    preconditionFailure("header.blockHash() failed due to incorrect state"
                        + " on passed key (for compact filter key)")
                }
            case .hash(let hash):
                return hash.littleEndian.prefix(16)
            }
        }
    }
}

// MARK: CfilterCommand / GCS Matching
extension CF.CfilterCommand {
    @usableFromInline
    func filter(matcher: CF.MatcherClient, pubKeys: Set<ScriptPubKey>, eventLoop: EventLoop) -> Future<Bool> {
        let filters: [UInt64]
        do {
            filters = try self.decode()
        } catch {
            return eventLoop.makeFailedFuture(error)
        }
        
        switch Result(catching: { try self.key() }) {
        case .success(let key):
            return matcher.match(opcodes: pubKeys.map(\.opcodes),
                                 key: key,
                                 f: self.f,
                                 filters: filters,
                                 eventLoop: eventLoop)
        case .failure(let error):
            return eventLoop.makeFailedFuture(error)
        }
    }
    
    @usableFromInline
    func decode() throws -> [UInt64] {
        switch self.stateMachine {
        case .valid, .serial:
            let gcsClient = self.golombCodedSetClientFactory()
            guard let decoded = gcsClient.decode(compressed: self.compressed,
                                                 n: self.n,
                                                 p: self.filterType.parameters.P)
            else {
                throw CFError.invalidFilterData(event: #function)
            }
            
            return decoded
        case .wire, .hashAndRank:
            throw CFError.invalidState(state: String(describing: self.stateMachine),
                                       event: #function)
        }
    }
}

// MARK: CfilterCommand / Serial Transition
extension CF.CfilterCommand {
    mutating func serial() throws {
        switch self.stateMachine {
        case .valid(let state):
            self.stateMachine = .serial(
                SerialState(_immutable: state._immutable,
                            blockHeight: state.blockHeight)
            )
        case .hashAndRank, .serial, .wire:
            throw CFError.invalidTransition(state: String(describing: self.stateMachine),
                                            event: #function)
        }
    }
}

// MARK: CfilterCommand / Getters
extension CF.CfilterCommand {
    func getHeight() throws -> Int {
        switch self.stateMachine {
        case .hashAndRank(let state):
            return state.blockHeight
        case .serial(let state):
            return state.blockHeight
        case .valid(let state):
            return state.blockHeight
        case .wire:
            throw CFError.invalidTransition(state: String(describing: self.stateMachine),
                                            event: #function)
        }
    }
}

extension CF.CfilterCommand {
    func getBlockHash() throws -> BlockChain.Hash<BlockHeaderHash> {
        switch self.stateMachine {
        case .hashAndRank(let state):
            return state.blockHash
        case .valid(let state):
            return state.blockHash
        case .wire(let state):
            return state.blockHash
        case .serial:
            throw CFError.invalidTransition(state: String(describing: self.stateMachine),
                                            event: #function)
        }
    }
    
    func key() throws -> [UInt8] {
        Array(try self.getBlockHash().littleEndian.prefix(16))
    }
}

// MARK: CfilterCommand / BitcoinCommandEncodable & BitcoinCommandDecodable
extension CF.CfilterCommand: BitcoinCommandEncodable {
    func write(to buffer: inout ByteBuffer) {
        let compressed = self.compressed.dropLast(7)
        buffer.writeInteger(self.filterType.rawValue)
        buffer.writeBytes(try! self.getBlockHash().littleEndian)

        let varIntBytes: [UInt8] = Array(self.n.variableLengthCode)
        buffer.writeBytes((varIntBytes.count + compressed.count).variableLengthCode)
        buffer.writeBytes(varIntBytes)
        buffer.writeBytes(compressed)
    }
    
    init?(fromBuffer: inout ByteBuffer) {
        guard let filterType = fromBuffer.readInteger(as: UInt8.self).flatMap(CF.FilterType.init(rawValue:)),
              let blockHash: [UInt8] = fromBuffer.readBytes(length: 32),
              let remainingSize = fromBuffer.readVarInt().map(Int.init),
              let hashSlice = fromBuffer.getSlice(at: fromBuffer.readerIndex, length: remainingSize),
              hashSlice.readableBytes == fromBuffer.readableBytes
        else {
            return nil
        }
        
        let filterHash: BlockChain.Hash<CompactFilterHash> = .makeHash(from: hashSlice.readableBytesView)

        fromBuffer.writeBytes(Array<UInt8>(repeating: 0, count: 7))

        guard let n = fromBuffer.readVarInt(),
              let compressed = fromBuffer.readBytes(length: fromBuffer.readableBytes)
        else {
            return nil
        }
        
        self.init(.wire(filterType: filterType,
                        blockHash: .little(blockHash),
                        filterHash: filterHash,
                        n: Int(n),
                        compressed: compressed))
    }
}

// MARK: Pretty print
extension CF.CfilterCommand.WireState._Immutable: CustomDebugStringConvertible {
    var debugDescription: String {
        "t[\(self.filterType)] filter#[\(self.n)] bytes#[\(self.compressed.count)]"
    }
}

extension CF.CfilterCommand: CustomDebugStringConvertible {
    var debugDescription: String {
        var out: [String] = ["cfilter🎰"]
        
        switch self.stateMachine {
        case .hashAndRank(let rank):
            out.append(rank._immutable.debugDescription)
            out.append("height#[\(rank.blockHeight) bhHash[\(rank.blockHash)]")
        case .serial(let serial):
            out.append(serial._immutable.debugDescription)
            out.append("height#[\(serial.blockHeight)]")
        case .valid(let valid):
            out.append(valid._immutable.debugDescription)
            out.append("🌱VALIDATED🌿 height#[\(valid.blockHeight) bhHash[\(valid.blockHash)] filterHash[\(valid.filterHash)]")
        case .wire(let wire):
            out.append(wire._immutable.debugDescription)
            out.append("bhHash[\(wire.blockHash)]")
        }
        
        return out.joined(separator: " ")
    }
}
