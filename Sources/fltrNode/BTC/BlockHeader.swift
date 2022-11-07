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
import struct NIO.ByteBuffer
import struct Foundation.Date
import protocol Foundation.LocalizedError

public struct BlockHeader {
    fileprivate enum State {
        case wire(WireState)
        case full(FullState)
        case unranked(UnrankedState)
        case unhashed(UnhashedState)
        case serial(SerialState)
    }
    
    fileprivate var state: State
    
    final class _ImmutableStorage {
        let version: Int32
        let merkleRoot: BlockChain.Hash<MerkleHash>
        let timestamp: UInt32
        let difficulty: UInt32
        let nonce: UInt32
        let transactions: UInt64
        
        init(version: Int32,
             merkleRoot: BlockChain.Hash<MerkleHash>,
             timestamp: UInt32,
             difficulty: UInt32,
             nonce: UInt32,
             transactions: UInt64) {
            self.version = version
            self.merkleRoot = merkleRoot
            self.timestamp = timestamp
            self.difficulty = difficulty
            self.nonce = nonce
            self.transactions = transactions
        }
    }

    fileprivate struct WireState {
        let _storage: _ImmutableStorage
        let previousBlockHash: BlockChain.Hash<BlockHeaderHash>
    }
    
    fileprivate struct FullState {
        let _storage: _ImmutableStorage
        let previousBlockHash: BlockChain.Hash<BlockHeaderHash>
        let id: Int
        let hash: BlockChain.Hash<BlockHeaderHash>
    }
    
    fileprivate struct UnrankedState {
        let _storage: _ImmutableStorage
        let previousBlockHash: BlockChain.Hash<BlockHeaderHash>
        let hash: BlockChain.Hash<BlockHeaderHash>
    }
    
    fileprivate struct UnhashedState {
        let _storage: _ImmutableStorage
        let previousBlockHash: BlockChain.Hash<BlockHeaderHash>
        let id: Int
    }
    
    fileprivate struct SerialState {
        let _storage: _ImmutableStorage
        let id: Int
        let hash: BlockChain.Hash<BlockHeaderHash>
    }
}

// MARK: Error
extension BlockHeader {
    fileprivate enum FSMError: Error {
        case illegalState(state: State, event: String)
    }
}

// Start state selector
extension BlockHeader {
    internal enum StartState {
        case wire(version: Int32,
                  previousBlockHash: BlockChain.Hash<BlockHeaderHash>,
                  merkleRoot: BlockChain.Hash<MerkleHash>,
                  timestamp: UInt32,
                  difficulty: UInt32,
                  nonce: UInt32,
                  transactions: UInt64)
        case serial(version: Int32,
                    merkleRoot: BlockChain.Hash<MerkleHash>,
                    timestamp: UInt32,
                    difficulty: UInt32,
                    nonce: UInt32,
                    id: Int,
                    hash: BlockChain.Hash<BlockHeaderHash>)
    }
}

// Start state WIRE, SERIAL
extension BlockHeader {
    internal init(_ format: StartState) {
        switch format {
        case let .wire(version: version, previousBlockHash: previousBlockHash, merkleRoot: merkleRoot, timestamp: timestamp, difficulty: difficulty, nonce: nonce, transactions: transactions):
            self.state = .wire(
                .init(
                    _storage: .init(
                        version: version,
                        merkleRoot: merkleRoot,
                        timestamp: timestamp,
                        difficulty: difficulty,
                        nonce: nonce,
                        transactions: transactions
                    ),
                    previousBlockHash: previousBlockHash
                )
            )
        case let .serial(version: version, merkleRoot: merkleRoot, timestamp: timestamp, difficulty: difficulty, nonce: nonce, id: id, hash: hash):
            self.state = .serial(
                .init(
                    _storage: .init(
                        version: version,
                        merkleRoot: merkleRoot,
                        timestamp: timestamp,
                        difficulty: difficulty,
                        nonce: nonce,
                        transactions: 0
                    ),
                    id: id,
                    hash: hash
                )
            )
        }
    }
}

// Transition WIRE -> UNRANKED
extension BlockHeader.UnrankedState {
    fileprivate init(fromWire state: BlockHeader.WireState, with hash: BlockChain.Hash<BlockHeaderHash>) {
        self._storage = state._storage
        self.previousBlockHash = state.previousBlockHash
        self.hash = hash
    }
}

// Transition UNHASHED -> FULL
extension BlockHeader.FullState {
    fileprivate init(fromUnhashed state: BlockHeader.UnhashedState, with hash: BlockChain.Hash<BlockHeaderHash>) {
        self._storage = state._storage
        self.previousBlockHash = state.previousBlockHash
        self.id = state.id
        self.hash = hash
    }
}

// Transition WIRE -> UNHASHED
extension BlockHeader.UnhashedState {
    fileprivate init(fromWire state: BlockHeader.WireState, with id: Int) {
        self._storage = state._storage
        self.previousBlockHash = state.previousBlockHash
        self.id = id
    }
}

// Transition UNRANKED -> FULL
extension BlockHeader.FullState {
    fileprivate init(fromUnranked state: BlockHeader.UnrankedState, with id: Int) {
        self._storage = state._storage
        self.previousBlockHash = state.previousBlockHash
        self.hash = state.hash
        self.id = id
    }
}

// Transition FULL -> WIRE
extension BlockHeader.WireState {
    fileprivate init(fromSerial state: BlockHeader.FullState) {
        self._storage = state._storage
        self.previousBlockHash = state.previousBlockHash
    }
}

// Transition FULL -> SERIAL
extension BlockHeader.SerialState {
    fileprivate init(fromFull state: BlockHeader.FullState) {
        self._storage = state._storage
        self.id = state.id
        self.hash = state.hash
    }
}

// Transition SERIAL -> FULL
extension BlockHeader.FullState {
    fileprivate init(fromSerial state: BlockHeader.SerialState, with previousBlockHash: BlockChain.Hash<BlockHeaderHash>) {
        self._storage = state._storage
        assert(previousBlockHash.count == 32)
        self.previousBlockHash = previousBlockHash
        self.id = state.id
        self.hash = state.hash
    }
}

// MARK: MAKE HASH
// Required fields for hashing
fileprivate protocol HashData {
    var version: Int32 { get }
    var previousBlockHash: BlockChain.Hash<BlockHeaderHash> { get }
    var merkleRoot: BlockChain.Hash<MerkleHash> { get }
    var timestamp: UInt32 { get }
    var difficulty: UInt32 { get }
    var nonce: UInt32 { get }
}

protocol _StorageProperties {
    var _storage: BlockHeader._ImmutableStorage { get }
}

extension _StorageProperties {
    var version: Int32 { _storage.version }
    var merkleRoot: BlockChain.Hash<MerkleHash> { _storage.merkleRoot }
    var timestamp: UInt32 { _storage.timestamp }
    var difficulty: UInt32 { _storage.difficulty }
    var nonce: UInt32 { _storage.nonce }
    var transactions: UInt64 { _storage.transactions }
}

fileprivate protocol AddHashTransition {
    var completed: BlockHeader.State { get }
}

extension AddHashTransition {
    fileprivate func addHash() -> BlockHeader.State {
        return completed
    }
}

extension HashData {
    func computeHash() -> BlockChain.Hash<BlockHeaderHash> {
        func toBytes<T: Any>(_ pointer: inout T) -> AnyBidirectionalCollection<UInt8> {
            withUnsafePointer(to: &pointer) {
                .init(UnsafeBufferPointer(start: $0.withMemoryRebound(to: UInt8.self, capacity: 1){ $0 }, count: MemoryLayout<T>.size))
            }
        }
        var p1: Int32, p4: UInt32, p5: UInt32, p6: UInt32
        p1 = version; p4 = timestamp; p5 = difficulty; p6 = nonce
        let parts: [AnyBidirectionalCollection<UInt8>] = [
            toBytes(&p1),
            .init(self.previousBlockHash.littleEndian),
            .init(self.merkleRoot.littleEndian),
            toBytes(&p4),
            toBytes(&p5),
            toBytes(&p6),
        ]
        return .makeHash(from: parts.joined())
    }
}

//import CryptoKit
//@usableFromInline
//func toBytes<T: Any, Hash: HashFunction>(_ pointer: inout T, hasher: inout Hash) {
//    withUnsafeBytes(of: &pointer) {
//        hasher.update(bufferPointer: $0)
//    }
//}
//
//extension HashData {
//    func computeHash2() -> BlockChain.Hash {
//        var p1: Int32, p4: UInt32, p5: UInt32, p6: UInt32
//        p1 = version; p4 = timestamp; p5 = difficulty; p6 = nonce
//
//        var hasher = SHA256()
//        toBytes(&p1, hasher: &hasher)
//        Array(self.previousBlockHash.bigEndian).withUnsafeBytes {
//            hasher.update(bufferPointer: $0)
//        }
//        Array(self.merkleRoot.bigEndian).withUnsafeBytes {
//            hasher.update(bufferPointer: $0)
//        }
//        toBytes(&p4, hasher: &hasher)
//        toBytes(&p5, hasher: &hasher)
//        toBytes(&p6, hasher: &hasher)
//        let hash256 = hasher.finalize().withUnsafeBytes {
//            SHA256.hash(data: $0)
//        }
//
//        return .init(.little(Array(hash256)))
//    }
//}

extension BlockHeader.WireState: HashData & _StorageProperties {}
extension BlockHeader.WireState: AddHashTransition {
    var completed: BlockHeader.State {
        return .unranked(
            .init(fromWire: self, with: self.computeHash())
        )
    }
}

extension BlockHeader.UnhashedState: HashData & _StorageProperties {}
extension BlockHeader.UnhashedState: AddHashTransition {
    var completed: BlockHeader.State {
        return .full(
            .init(fromUnhashed: self, with: self.computeHash())
        )
    }
}

extension BlockHeader {
    public mutating func addBlockHash() throws {
        switch state {
        case .wire(let d as AddHashTransition), .unhashed(let d as AddHashTransition): self.state = d.addHash()
        case .full, .serial, .unranked: throw FSMError.illegalState(state: state, event: #function)
        }
    }
}

// MARK: FILL HEIGT
fileprivate protocol AddHeightTransition {
    func height(_: Int) -> BlockHeader.State
}

extension BlockHeader.WireState: AddHeightTransition {
    func height(_ id: Int) -> BlockHeader.State {
        .unhashed(
            .init(fromWire: self, with: id)
        )
    }
}

extension BlockHeader.UnrankedState: AddHeightTransition {
    func height(_ id: Int) -> BlockHeader.State {
        .full(
            .init(fromUnranked: self, with: id)
        )
    }
}

extension BlockHeader {
    public mutating func addHeight(_ id: Int) throws {
        switch state {
        case .wire(let d as AddHeightTransition), .unranked(let d as AddHeightTransition): self.state = d.height(id)
        case .full, .serial, .unhashed: throw FSMError.illegalState(state: state, event: #function)
        }
    }
}

// MARK: FILL PreviousBlockHash
extension BlockHeader.SerialState {
    fileprivate func previousBlockHash(_ previousBlockHash: BlockChain.Hash<BlockHeaderHash>) -> BlockHeader.State {
        .full(
            .init(fromSerial: self, with: previousBlockHash)
        )
    }
}

extension BlockHeader {
    public mutating func addPreviousBlockHash(_ previousBlockHash: BlockChain.Hash<BlockHeaderHash>) throws {
        switch state {
        case .serial(let d): self.state = d.previousBlockHash(previousBlockHash)
        case .full, .unhashed, .unranked, .wire: throw FSMError.illegalState(state: state, event: #function)
        }
    }
}

// MARK: SerialTransition
extension BlockHeader {
    public mutating func serial() throws {
        switch state {
        case .full(let d):
            state = .serial(SerialState(fromFull: d))
        case .serial, .unhashed, .unranked, .wire:
            throw FSMError.illegalState(state: state, event: #function)
        }
    }
}

// MARK: WireTransition
extension BlockHeader {
    public mutating func wire() throws {
        switch state {
        case .full(let d): state = .wire(.init(fromSerial: d))
        case .wire: return
        case .serial, .unhashed, .unranked: throw FSMError.illegalState(state: state, event: #function)
        }
    }
}

// MARK: GET HASH
extension BlockHeader {
    public func blockHash() throws -> BlockChain.Hash<BlockHeaderHash> {
        switch state {
        case .full(let state):
            return state.hash
        case .serial(let state):
            return state.hash
        case .unranked(let state):
            return state.hash
        case .unhashed, .wire:
            throw FSMError.illegalState(state: state, event: #function)
        }
    }
}

// MARK: PREVIOUS BLOCK HASH
// protects from reading a reversed previousBlockHash, since these are only relevant in combination with a hash
extension BlockHeader {
    public func previousBlockHash() throws -> BlockChain.Hash<BlockHeaderHash> {
        switch state {
        case .full(let state):
            return state.previousBlockHash
        case .unranked(let state):
            return state.previousBlockHash
        case .unhashed(let state):
            return state.previousBlockHash
        case .wire(let state):
            return state.previousBlockHash
        case .serial:
            throw FSMError.illegalState(state: state, event: #function)
        }
    }
}

// MARK: GET HEIGHT
extension BlockHeader {
    public func height() throws -> Int {
        switch state {
        case .full(let state):
            return state.id
        case .serial(let state):
            return state.id
        case .unhashed(let state):
            return state.id
        case .unranked, .wire:
            throw FSMError.illegalState(state: state, event: #function)
        }
    }
}

// MARK: Views
// WireView
public struct BlockHeaderWireView {
    private let _wireState: BlockHeader.WireState
    
    fileprivate init(_ _wireState: BlockHeader.WireState) {
        self._wireState = _wireState
    }
    
    public var version: Int32 {
        self._wireState._storage.version
    }
    public var previousBlockHash: BlockChain.Hash<BlockHeaderHash> {
        self._wireState.previousBlockHash
    }
    public var merkleRoot: BlockChain.Hash<MerkleHash> {
        self._wireState._storage.merkleRoot
    }
    public var timestamp: UInt32 {
        self._wireState._storage.timestamp
    }
    public var difficulty: UInt32 {
        self._wireState._storage.difficulty
    }
    public var nonce: UInt32 {
        self._wireState._storage.nonce
    }
    public var transactions: UInt64 {
        self._wireState._storage.transactions
    }
}

extension BlockHeader {
    public func wireView() throws -> BlockHeaderWireView {
        switch state {
        case .wire(let d):
            return .init(d)
        case .full, .serial, .unhashed, .unranked:
            throw FSMError.illegalState(state: state, event: #function)
        }
    }
}

// SerialView
public struct BlockHeaderSerialView {
    private let _serialState: BlockHeader.SerialState
    
    fileprivate init(_ _serialState: BlockHeader.SerialState) {
        self._serialState = _serialState
    }
    
    public var version: Int32 {
        self._serialState._storage.version
    }
    public var merkleRoot: BlockChain.Hash<MerkleHash> {
        self._serialState._storage.merkleRoot
    }
    public var timestamp: UInt32 {
        self._serialState._storage.timestamp
    }
    public var difficulty: UInt32 {
        self._serialState._storage.difficulty
    }
    public var nonce: UInt32 {
        self._serialState._storage.nonce
    }
    public var id: Int {
        self._serialState.id
    }
    public var hash: BlockChain.Hash<BlockHeaderHash> {
        self._serialState.hash
    }
}

extension BlockHeader {
    public func serialView() throws -> BlockHeaderSerialView {
        switch state {
        case .serial(let d):
            return .init(d)
        case .full:
            var copy = self
            try copy.serial()
            return try copy.serialView()
        case .unhashed, .unranked, .wire:
            throw FSMError.illegalState(state: state, event: #function)
        }
    }
}

// MARK: Properties
extension BlockHeader: _StorageProperties {
    var _storage: _ImmutableStorage {
        switch self.state {
        case .wire(let state):
            return state._storage
        case .full(let state):
            return state._storage
        case .serial(let state):
            return state._storage
        case .unhashed(let state):
            return state._storage
        case .unranked(let state):
            return state._storage
        }
    }
}

// MARK: CustomStringConvertible
extension BlockHeader: CustomStringConvertible {
    public var description: String {
        "BlockHeader: \(state)"
    }
}

extension BlockHeader.State: CustomStringConvertible {
    var description: String {
        func from(wire state: BlockHeader.WireState) -> String {
            ".wire(\(from(version: state._storage.version, previousBlockHash: state.previousBlockHash, merkleRoot: state.merkleRoot, timestamp: state._storage.timestamp, difficulty: state._storage.difficulty, nonce: state._storage.nonce)))"
        }
        func from(full state: BlockHeader.FullState) -> String {
            ".full(\(from(version: state._storage.version, previousBlockHash: state.previousBlockHash, merkleRoot: state._storage.merkleRoot, timestamp: state._storage.timestamp, difficulty: state._storage.difficulty, nonce: state._storage.nonce, id: state.id, hash: state.hash)))"
        }
        func from(serial state: BlockHeader.SerialState) -> String {
            ".serial(\(from(version: state._storage.version, merkleRoot: state._storage.merkleRoot, timestamp: state._storage.timestamp, difficulty: state._storage.difficulty, nonce: state._storage.nonce, id: state.id, hash: state.hash)))"
        }
        func from(unhashed state: BlockHeader.UnhashedState) -> String {
            ".unhashed(\(from(version: state._storage.version, previousBlockHash: state.previousBlockHash, merkleRoot: state.merkleRoot, timestamp: state._storage.timestamp, difficulty: state._storage.difficulty, nonce: state.nonce, id: state.id)))"
        }
        func from(unranked state: BlockHeader.UnrankedState) -> String {
            ".unranked(\(from(version: state._storage.version, previousBlockHash: state.previousBlockHash, merkleRoot: state._storage.merkleRoot, timestamp: state._storage.timestamp, difficulty: state._storage.difficulty, nonce: state._storage.nonce, hash: state.hash)))"
        }
        func from(version: Int32, previousBlockHash: BlockChain.Hash<BlockHeaderHash>? = nil, merkleRoot: BlockChain.Hash<MerkleHash>, timestamp: UInt32, difficulty: UInt32, nonce: UInt32, id: Int? = nil, hash: BlockChain.Hash<BlockHeaderHash>? = nil) -> String {
            "version: \(String(format:"%02x", version))"
                + (previousBlockHash == nil ? "" : ", previousBlockHash: \(previousBlockHash!)")
                + ", merkleRoot: \(merkleRoot), timestamp: \(Date(timeIntervalSince1970: .init(timestamp))), difficulty: *, nonce: \(nonce)"
                + (id == nil ? "" : ", id: \(id!)")
                + (hash == nil ? "" : ", hash: \(hash!)")
        }
        switch self {
        case .wire(let state): return from(wire: state)
        case .full(let state): return from(full: state)
        case .unhashed(let state): return from(unhashed: state)
        case .unranked(let state): return from(unranked: state)
        case .serial(let state): return from(serial: state)
        }
    }
}

// MARK: Equatable conformance
extension BlockHeader._ImmutableStorage: Equatable {
    static func == (lhs: BlockHeader._ImmutableStorage, rhs: BlockHeader._ImmutableStorage) -> Bool {
        lhs.difficulty == rhs.difficulty &&
            lhs.nonce == rhs.nonce &&
            lhs.timestamp == rhs.timestamp &&
            lhs.version == rhs.version &&
            lhs.merkleRoot == rhs.merkleRoot
    }
}
extension BlockHeader.State: Equatable {
    public static func == (lhs: BlockHeader.State, rhs: BlockHeader.State) -> Bool {
        switch (lhs, rhs) {
        case (.wire(let a), .wire(let b)): return a == b
        case (.full(let a), .full(let b)): return a == b
        case (.unhashed(let a), .unhashed(let b)): return a == b
        case (.unranked(let a), .unranked(let b)): return a == b
        case (.serial(let a), .serial(let b)): return a == b
        default: return false
        }
    }
}
extension BlockHeader.WireState: Equatable {}
extension BlockHeader.FullState: Equatable {}
extension BlockHeader.UnrankedState: Equatable {}
extension BlockHeader.UnhashedState: Equatable {}
extension BlockHeader.SerialState: Equatable {}
extension BlockHeader: Equatable {}
extension BlockHeaderWireView: Equatable {}
extension BlockHeaderSerialView: Equatable {}

// MARK: Hashable conformance
extension BlockHeader._ImmutableStorage: Hashable {
    func hash(into hasher: inout Hasher) {
        hasher.combine(self.difficulty)
        hasher.combine(self.nonce)
        hasher.combine(self.timestamp)
        hasher.combine(self.merkleRoot)
        hasher.combine(self.version)
    }
}
extension BlockHeader.State: Hashable {
    public func hash(into hasher: inout Hasher) {
        switch self {
        case .wire(let state): hasher.combine(state)
        case .full(let state): hasher.combine(state)
        case .unhashed(let state): hasher.combine(state)
        case .unranked(let state): hasher.combine(state)
        case .serial(let state): hasher.combine(state)
        }
    }
}
extension BlockHeader.WireState: Hashable {}
extension BlockHeader.FullState: Hashable {}
extension BlockHeader.UnrankedState: Hashable {}
extension BlockHeader.UnhashedState: Hashable {}
extension BlockHeader.SerialState: Hashable {}
extension BlockHeader: Hashable {}
extension BlockHeaderWireView: Hashable {}
extension BlockHeaderSerialView: Hashable {}

extension BlockHeader.FSMError: CustomStringConvertible, LocalizedError {
    var errorDescription: String? {
        localizedDescription
    }

    var description: String {
        localizedDescription
    }
    
    var localizedDescription: String {
        switch self {
        case .illegalState(state: let state, event: let event):
            return "Illegal state(\(state)) for event(\(event))"
        }
    }
}

// MARK: BitcoinCommandEncodable & BitcoinCommandDecodable
extension BlockHeader: BitcoinCommandEncodable {
    public func write(to buffer: inout ByteBuffer) {
        var copy = self
        var view: BlockHeaderWireView
        do {
            try copy.wire()
            view = try copy.wireView()
        } catch {
            logger.error("BlockHeader", #function, "Cannot read wire view from BlockHeader error [", error.localizedDescription, "]")
            return
        }
        buffer.writeInteger(view.version, endianness: .little)
        buffer.writeBytes(view.previousBlockHash.littleEndian)
        buffer.writeBytes(view.merkleRoot.littleEndian)
        buffer.writeInteger(view.timestamp, endianness: .little)
        buffer.writeInteger(view.difficulty, endianness: .little)
        buffer.writeInteger(view.nonce, endianness: .little)
        buffer.writeBytes(view.transactions.variableLengthCode)
    }
}

extension BlockHeader: BitcoinCommandDecodable {
    public init?(fromBuffer: inout ByteBuffer) {
        let save = fromBuffer
        guard let version: Int32 = fromBuffer.readInteger(endianness: .little),
            let previousBlockHash = fromBuffer.readBytes(length: 32),
            let merkleRoot = fromBuffer.readBytes(length: 32),
            let timestamp: UInt32 = fromBuffer.readInteger(endianness: .little),
            let difficulty: UInt32 = fromBuffer.readInteger(endianness: .little),
            let nonce: UInt32 = fromBuffer.readInteger(endianness: .little)
        else {
                fromBuffer = save
                return nil
        }
        
        let transactions: UInt64? = fromBuffer.readVarInt()
        
        self.init(
            .wire(version: version, previousBlockHash: .little(previousBlockHash),
                  merkleRoot: .little(merkleRoot), timestamp: timestamp,
                  difficulty: difficulty, nonce: nonce, transactions: transactions ?? 0)
        )
    }
}
