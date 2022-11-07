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
import HaByLo
import protocol Foundation.LocalizedError
import NIO

public extension Tx {
    struct Transaction: BitcoinCommandValue {
        public var description: String {
            "tx"
        }
        
        fileprivate enum State {
            case initial(InitialState)
            case hashed(HashedState)
            case validated(ValidState)
        }
        
        private var state: State
        
        @Setting(\.BYTEBUFFER_ALLOCATOR) fileprivate var allocator: ByteBufferAllocator
    }
}

// MARK: Initializer
public extension Tx.Transaction {
    enum Initialize {
        case wireLegacy(version: Int32,
                        vin: [Tx.In],
                        vout: [Tx.Out],
                        locktime: Tx.Locktime)
        case wireLegacyWithHash(version: Int32,
                                vin: [Tx.In],
                                vout: [Tx.Out],
                                locktime: Tx.Locktime,
                                cachingTxId: CachingTxId)
        case wireWitness(version: Int32,
                         vin: [Tx.In],
                         vout: [Tx.Out],
                         locktime: Tx.Locktime)
        case wireWitnessWithHash(version: Int32,
                                 vin: [Tx.In],
                                 vout: [Tx.Out],
                                 locktime: Tx.Locktime,
                                 cachingWtxId: CachingWtxId)
        case serial
    }
    
    init(_ input: Initialize) {
        switch input {
        case let .wireLegacy(version, vin, vout, locktime):
            self.state = .initial(
                InitialState(
                    _immutable: _Immutable(version: version,
                                           vin: vin,
                                           vout: vout,
                                           hasWitnesses: false,
                                           locktime: locktime),
                    cachingTxId: nil,
                    cachingWtxId: nil
                )
            )
        case let .wireLegacyWithHash(version, vin, vout, locktime, cachingTxId):
            self.state = .initial(
                InitialState(
                    _immutable: _Immutable(version: version,
                                           vin: vin,
                                           vout: vout,
                                           hasWitnesses: false,
                                           locktime: locktime),
                    cachingTxId: cachingTxId,
                    cachingWtxId: nil
                )
            )
        case let .wireWitness(version, vin, vout, locktime):
            self.state = .initial(
                InitialState(
                    _immutable: _Immutable(version: version,
                                                        vin: vin,
                                                        vout: vout,
                                                        hasWitnesses: true,
                                                        locktime: locktime),
                    cachingTxId: nil,
                    cachingWtxId: nil
                )
            )
        case let .wireWitnessWithHash(version, vin, vout, locktime, cachingWtxId):
            self.state = .initial(
                InitialState(
                    _immutable: _Immutable(version: version,
                                                        vin: vin,
                                                        vout: vout,
                                                        hasWitnesses: true,
                                                        locktime: locktime),
                    cachingTxId: nil,
                    cachingWtxId: cachingWtxId
                )
            )
        case .serial: fatalError() // TODO: incomplete
        }
    }
}

// MARK: StateMachineError
extension Tx.Transaction {
    public enum TransactionError: Error, CustomStringConvertible, LocalizedError {
        case cannotDeserializeTransaction
        case coinbaseInvalidScriptSig(size: Int)
        case duplicateInputs
        case invalidTransition(state: String, event: StaticString)
        case nullInputOutpoint
        case overMaxMoneyLimit(sum: UInt64)
        case overWeightLimit(weight: Int)
        case vinIsEmpty
        case voutIsEmpty
        
        public var description: String {
            switch self {
            case .cannotDeserializeTransaction:
                return "cannot deserialize transaction "
            case .coinbaseInvalidScriptSig(let size):
                return "coinbaseInvalidScriptSig(size:) error with size \(size)"
            case .duplicateInputs:
                return "duplicateInputs error for transaction"
            case .invalidTransition(let state, let event):
                return "invalidTransition error from (\(state)) for event (\(event))"
            case .nullInputOutpoint:
                return "nullInputOutpoint error for non coinbase transaction"
            case .overMaxMoneyLimit(let sum):
                return "overMaxMoneyLimit error, invalid transaction with sum \(sum)"
            case .overWeightLimit(let weight):
                return "overWeightLimit error, invalid transaction with weight \(weight)"
            case .vinIsEmpty:
                return "vinIsEmpty error, transaction invalid"
            case .voutIsEmpty:
                return "voutIsEmpty error, transaction invalid"
            }
        }
        public var errorDescription: String? {
            self.description
        }
    }
}

// MARK: ImmutableState
extension Tx.Transaction {
    public typealias CachingTxId = () -> Tx.TxId
    public typealias CachingWtxId = () -> Tx.WtxId

    fileprivate struct InitialState: TransactionImmutableDelegate {
        let _immutable: _Immutable
        // Opportunistic caching of hash on load
        let cachingTxId: CachingTxId?
        let cachingWtxId: CachingWtxId?
    }
}

extension Tx.Transaction {
    fileprivate class _Immutable {
        let version: Int32
        let vin: [Tx.In]
        let vout: [Tx.Out]
        let hasWitnesses: Bool
        let locktime: Tx.Locktime

        internal init(version: Int32, vin: [Tx.In], vout: [Tx.Out], hasWitnesses: Bool, locktime: Tx.Locktime) {
            self.version = version
            self.vin = vin
            self.vout = vout
            self.hasWitnesses = hasWitnesses
            self.locktime = locktime
        }
    }
}


fileprivate protocol TransactionImmutableDelegate: TransactionProtocol {
    var _immutable: Tx.Transaction._Immutable { get }
}

extension TransactionImmutableDelegate {
    public var version: Int32 {
        self._immutable.version
    }
    public var vin: [Tx.In] {
        self._immutable.vin
    }
    public var vout: [Tx.Out] {
        self._immutable.vout
    }
    public var locktime: Tx.Locktime {
        self._immutable.locktime
    }
    public var hasWitnesses: Bool {
        self._immutable.hasWitnesses
    }
}

extension Tx.Transaction: TransactionImmutableDelegate {
    fileprivate var _immutable: _Immutable {
        switch self.state {
        case .hashed(let state as TransactionImmutableDelegate),
             .validated(let state as TransactionImmutableDelegate),
             .initial(let state as TransactionImmutableDelegate):
            return state._immutable
        }
    }
}

// MARK: Hash Transition
extension Tx.Transaction {
    mutating func hash() throws {
        switch self.state {
        case .initial(let state):
            let hash = calculateHash(state, allocator: self.allocator)
            self.state = .hashed(HashedState(witnessState: state, txId: hash.txId, wtxId: hash.wtxId))
        case .validated, .hashed:
            throw TransactionError.invalidTransition(state: String(describing: self.state),
                                                     event: #function)
        }
    }
}

extension Tx.Transaction {
    fileprivate struct HashedState: TransactionImmutableDelegate {
        let _immutable: _Immutable
        let txId: Tx.TxId
        let wtxId: Tx.WtxId
        
        init(witnessState: InitialState, txId: Tx.TxId, wtxId: Tx.WtxId) {
            self._immutable = witnessState._immutable
            self.txId = txId
            self.wtxId = wtxId
        }
    }
}

fileprivate func calculateHash(_ state: Tx.Transaction.InitialState,
                               allocator: ByteBufferAllocator) -> (txId: Tx.TxId, wtxId: Tx.WtxId) {
    func legacyHash() -> Tx.TxId {
        var txId: ByteBuffer = allocator.buffer(capacity: 512)
        state.writeLegacyTransaction(to: &txId)
        return .makeHash(from: txId.readableBytesView)
    }
    
    func witnessHash() -> Tx.WtxId {
        var wtxId: ByteBuffer = allocator.buffer(capacity: 512)
        state.writeSegwitTransaction(to: &wtxId)
        return .makeHash(from: wtxId.readableBytesView)
    }
    
    switch (state.hasWitnesses, state.cachingTxId, state.cachingWtxId) {
    case (false, nil, nil):
        return (txId: legacyHash(), wtxId: legacyHash().asWtxId)
    case (false, .some(let cachingTxId), nil):
        let txId = cachingTxId()
        return (txId: txId, wtxId: txId.asWtxId)
    case (true, nil, nil):
        return (txId: legacyHash(), wtxId: witnessHash())
    case (true, nil, .some(let cachingWtxId)):
        let txIdHash: Tx.TxId = legacyHash()
        return (txId: txIdHash, wtxId: cachingWtxId())
    case (false, nil, _),
         (false, .some, .some),
         (true, _, nil),
         (true, .some, .some):
        preconditionFailure(
            "impossible state when calculating hash hasWitness(\(state.hasWitnesses))" +
            "txId(\(String(describing: state.cachingTxId))) wtxId(\(String(describing: state.cachingWtxId)))"
        )
    }
}

// MARK: Validate Transition
extension Tx.Transaction {
    mutating func validate() throws {
        switch self.state {
        case .hashed(let state):
            try doValidation(state)
            self.state = .validated(ValidState(hashedState: state))
        case .validated, .initial:
            throw TransactionError.invalidTransition(state: String(describing: state),
                                                     event: #function)
        }
    }
}

extension Tx.Transaction {
    fileprivate struct ValidState: TransactionImmutableDelegate {
        let _immutable: _Immutable
        let txId: Tx.TxId
        let wtxId: Tx.WtxId
        
        init(hashedState: HashedState) {
            self._immutable = hashedState._immutable
            self.txId = hashedState.txId
            self.wtxId = hashedState.wtxId
        }
    }
}

fileprivate func doValidation(_ state: Tx.Transaction.HashedState) throws {
    guard !state.vin.isEmpty else {
        logger.error("Transaction", #function, "- INVALID Transaction [\(state)] vin invalid/empty")
        throw Tx.Transaction.TransactionError.vinIsEmpty
    }

    guard !state.vout.isEmpty else {
        logger.error("Transaction", #function, "- INVALID Transaction [\(state)] vout invalid/empty")
        throw Tx.Transaction.TransactionError.voutIsEmpty
    }
 
    guard state.coreSize * Settings.PROTOCOL_SEGWIT_SCALE_FACTOR < Settings.PROTOCOL_SEGWIT_WEIGHT_LIMIT else {
        logger.error("Transaction", #function, "- INVALID Transaction over size limit")
        throw Tx.Transaction.TransactionError.overWeightLimit(weight: state.coreSize * Settings.PROTOCOL_SEGWIT_SCALE_FACTOR)

    }
    
    let moneySum = state.vout.map({ $0.value }).reduce(0, +)
    guard moneySum < Settings.PROTOCOL_MAX_MONEY * Settings.PROTOCOL_COIN else {
        logger.error("Transaction", #function, "- INVALID Transaction over MAX_MONEY limit")
        throw Tx.Transaction.TransactionError.overMaxMoneyLimit(sum: moneySum)
    }
    
    let outpoints = state.vin.map { $0.outpoint }
    let set: Set<Tx.Outpoint> = .init(outpoints)
    guard outpoints.count == set.count else {
        logger.error("Transaction", #function, "- INVALID Transaction [\(state)] with duplicate inputs")
        throw Tx.Transaction.TransactionError.duplicateInputs
    }
    
    if state.isCoinbase {
        let scriptSigSize = state.vin[0].scriptSig.count
        guard scriptSigSize >= 2 || scriptSigSize <= 100 else {
            logger.error("Transaction", #function, "- INVALID Coinbase Transaction [\(state)] with invalid scriptSig size [\(scriptSigSize)]")
            throw Tx.Transaction.TransactionError.coinbaseInvalidScriptSig(size: scriptSigSize)
        }
    } else {
        for vin in state.vin {
            guard !vin.outpoint.isNull else {
                logger.error("Transaction", #function, "- INVALID Transaction [\(state)] with zero input [\(vin)]")
                throw Tx.Transaction.TransactionError.nullInputOutpoint
            }
        }
    }
}
 
extension Tx.Transaction {
    func validatedView() throws -> ValidatedView {
        switch self.state {
        case .validated(let state):
            return ValidatedView(_validState: state)
        case .hashed, .initial:
            throw TransactionError.invalidTransition(state: String(describing: state),
                                                     event: #function)
        }
    }
}

extension Tx.Transaction {
    struct ValidatedView: TransactionProtocol {
        fileprivate let _validState: ValidState
        
        public var version: Int32 {
            self._validState.version
        }
        public var vin: [Tx.In] {
            self._validState.vin
        }
        public var vout: [Tx.Out] {
            self._validState.vout
        }
        public var hasWitnesses: Bool {
            self._validState.hasWitnesses
        }
        public var locktime: Tx.Locktime {
            self._validState.locktime
        }
        public var txId: Tx.TxId {
            self._validState.txId
        }
        public var wtxId: Tx.WtxId {
            self._validState.wtxId
        }
    }
}

// MARK: TxId / WtxId getters
public extension Tx.Transaction {
    func getTxId() throws -> Tx.TxId {
        switch self.state {
        case .hashed(let state):
            return state.txId
        case .validated(let state):
            return state.txId
        case .initial:
            throw TransactionError.invalidTransition(
                state: String(describing: self.state),
                event: #function)
        }
    }
    
    func getWtxId() throws -> Tx.WtxId {
        switch self.state {
        case .hashed(let state):
            return state.wtxId
        case .validated(let state):
            return state.wtxId
        case .initial:
            throw TransactionError.invalidTransition(
                state: String(describing: self.state),
                event: #function)
        }
    }
}

// MARK: BitcoinCommandEncodable & BitcoinCommandDecodable
extension Tx.Transaction: BitcoinCommandEncodable {
    public func write(to buffer: inout ByteBuffer) {
        switch self.state {
        case .hashed(let state as TransactionProtocol),
             .validated(let state as TransactionProtocol),
             .initial(let state as TransactionProtocol):
            if state.hasWitnesses {
                state.writeSegwitTransaction(to: &buffer)
            } else {
                state.writeLegacyTransaction(to: &buffer)
            }
        }
    }
}

extension Tx.Transaction: BitcoinCommandDecodable {
    public init?(fromBuffer: inout ByteBuffer) {
        self.init(fromBuffer: &fromBuffer, cacheBufferForHashing: true)
    }
    
    public init?(fromBuffer: inout ByteBuffer, cacheBufferForHashing: Bool) {
        let save = fromBuffer
        guard let version: Int32 = fromBuffer.readInteger(endianness: .little),
            let vinCount = fromBuffer.readVarInt() else {
                fromBuffer = save
                return nil
        }
        var enableLocktime: Bool = false
        var vin: [Tx.In] = []
        for _ in 0..<vinCount {
            guard let txIn = Tx.In(fromBuffer: &fromBuffer, enableLocktime: &enableLocktime, getWitness: nil) else {
                fromBuffer = save
                return nil
            }
            vin.append(txIn)
        }
        assert(Int(vinCount) == vin.count)
        guard let voutCount = fromBuffer.readVarInt() else {
            fromBuffer = save
            return nil
        }
        
        func readWitnessTransaction() -> Tx.Transaction? {
            guard let vinWitnessCount = fromBuffer.readVarInt() else {
                return nil
            }
            var witnesses: [Tx.Witness?] = []
            let vinWitness: [Tx.In] = (0..<Int(vinWitnessCount)).compactMap { i in
                Tx.In(fromBuffer: &fromBuffer, enableLocktime: &enableLocktime, getWitness: witnesses[i])
            }
            guard vinWitness.count == vinWitnessCount,
                let voutWitnessCount = fromBuffer.readVarInt() else {
                    return nil
            }

            var voutWitness: [Tx.Out] = []
            for _ in 0..<voutWitnessCount {
                guard let txOut = Tx.Out(fromBuffer: &fromBuffer) else {
                    return nil
                }
                voutWitness.append(txOut)
            }
            assert(Int(voutWitnessCount) == voutWitness.count)
            
            var encounteredWitness = false
            for _ in (0..<vinWitnessCount) {
                guard let witnessFieldCount = fromBuffer.readVarInt() else {
                    return nil
                }
                guard witnessFieldCount > 0 else {
                    witnesses.append(nil)
                    continue
                }
                var witnessField = [[UInt8]]()
                for _ in (0..<witnessFieldCount) {
                    guard let fieldLength = fromBuffer.readVarInt(),
                        let field = fromBuffer.readBytes(length: Int(fieldLength)) else {
                            return nil
                    }
                    witnessField.append(field)
                }
                encounteredWitness = true
                witnesses.append(Tx.Witness(witnessField: witnessField))
            }
            guard encounteredWitness else {
                return nil
            }
            
            guard let locktime: UInt32 = fromBuffer.readInteger(endianness: .little) else {
                return nil
            }
            
            if cacheBufferForHashing {
                return Tx.Transaction(.wireWitnessWithHash(
                    version: version,
                    vin: vinWitness,
                    vout: voutWitness,
                    locktime: enableLocktime
                        ? .enable(locktime)
                        : .disable(locktime),
                    cachingWtxId: { [endIndex = fromBuffer.readerIndex] in
                        let length = endIndex - save.readerIndex
                        let transactionSlice = save.getSlice(at: save.readerIndex, length: length)!
                        return .makeHash(from: transactionSlice.readableBytesView)
                }))
            } else {
                return Tx.Transaction(.wireWitness(
                    version: version,
                    vin: vinWitness,
                    vout: voutWitness,
                    locktime: enableLocktime
                        ? .enable(locktime)
                        : .disable(locktime)))
            }
        }

        func readLegacyTransaction() -> Tx.Transaction? {
            let vout: [Tx.Out] = (0..<voutCount).compactMap { _ in
                Tx.Out(fromBuffer: &fromBuffer)
            }
            guard vout.count == voutCount,
                let locktime: UInt32 = fromBuffer.readInteger(endianness: .little) else {
                    return nil
            }
            
            if cacheBufferForHashing {
                return Tx.Transaction(.wireLegacyWithHash(
                    version: version,
                    vin: vin,
                    vout: vout,
                    locktime: enableLocktime
                        ? .enable(locktime)
                        : .disable(locktime),
                    cachingTxId: { [endIndex = fromBuffer.readerIndex] in
                        let length = endIndex - save.readerIndex
                        let transactionSlice = save.getSlice(at: save.readerIndex, length: length)!
                        return .makeHash(from: transactionSlice.readableBytesView)
                }))
            } else {
                return Tx.Transaction(.wireLegacy(
                    version: version,
                    vin: vin,
                    vout: vout,
                    locktime: enableLocktime
                        ? .enable(locktime)
                        : .disable(locktime)))
            }
        }
                
        if vinCount == 0 && voutCount == 1 {
            guard let witness = readWitnessTransaction() else {
                fromBuffer = save
                return nil
            }
            self = witness
        } else {
            guard let legacy = readLegacyTransaction() else {
                fromBuffer = save
                return nil
            }
            self = legacy
        }
    }
}

extension Tx.Outpoint: BitcoinCommandDecodable & BitcoinCommandEncodable {}
extension Tx.In: BitcoinCommandDecodable & BitcoinCommandEncodable {}
extension Tx.Out: BitcoinCommandDecodable & BitcoinCommandEncodable {}

// MARK: Transaction Equatable and Hashable
extension Tx.Transaction: Equatable & Hashable {
    public static func == (lhs: Tx.Transaction, rhs: Tx.Transaction) -> Bool {
        lhs.state == rhs.state
    }
    
    public func hash(into hasher: inout Hasher) {
        hasher.combine(state)
    }
}

extension Tx.Transaction.State: Equatable & Hashable {}

extension Tx.Transaction.InitialState: Equatable & Hashable {
    static func == (lhs: Tx.Transaction.InitialState, rhs: Tx.Transaction.InitialState) -> Bool {
        lhs._immutable == rhs._immutable
    }
    
    func hash(into hasher: inout Hasher) {
        hasher.combine(self._immutable)
    }
}

extension Tx.Transaction._Immutable: Equatable & Hashable {
    static func == (lhs: Tx.Transaction._Immutable, rhs: Tx.Transaction._Immutable) -> Bool {
        lhs.version == rhs.version
            && lhs.vin == rhs.vin
            && lhs.vout == rhs.vout
            && lhs.locktime == rhs.locktime
    }
    
    func hash(into hasher: inout Hasher) {
        hasher.combine(self.version)
        hasher.combine(self.vin)
        hasher.combine(self.vout)
        hasher.combine(self.locktime)
    }
}

extension Tx.Transaction.HashedState: Equatable & Hashable {}
extension Tx.Transaction.ValidState: Equatable & Hashable {}
extension Tx.Transaction.ValidatedView: Equatable & Hashable {}

// MARK: Utility functions
extension Tx.Transaction {
    static func segwitCommitmentHashField(coinbase transaction: Tx.Transaction?) -> BlockChain.Hash<MerkleHash>? {
        guard let transaction = transaction, transaction.isCoinbase else {
            return nil
        }
        
        let prefix = Settings.PROTOCOL_SEGWIT_COINBASE_PREFIX
        let match = transaction.vout.filter {
            return $0.scriptPubKey.count >= 36
                && $0.scriptPubKey.prefix(prefix.count).elementsEqual(prefix)
        }
        
        guard let rootVout = match.last else {
            return nil
        }
        
        let result = rootVout.scriptPubKey.prefix(prefix.count + 32).suffix(from: prefix.count)
        assert(result.count == 32)
        
        return .init(.little(result))
    }
}

extension Tx.Transaction {
    @usableFromInline
    func findFunding(for pubKeys: Set<ScriptPubKey>) -> [FundingOutpoint] {
        return self.vout.enumerated()
        .compactMap { index, txOut in
            guard let firstIndex = pubKeys.firstIndex(of: ScriptPubKey(tag: .max,
                                                                       index: .max,
                                                                       opcodes: txOut.scriptPubKey))
            else { return nil }
            
            let first = pubKeys[firstIndex]
            return FundingOutpoint(
                outpoint: Tx.Outpoint(transactionId: try! self.getTxId(),
                                      index: UInt32(index)),
                amount: txOut.value,
                scriptPubKey: first
            )
        }
    }
}

extension TransactionProtocol {
    @usableFromInline
    func findSpent(for outpoints: Outpoints) -> Set<Tx.Outpoint> {
        let txOutpoints = self.vin.map(\.outpoint)
        let intersection = outpoints.intersection(txOutpoints)
        
        return intersection
    }
    
    @usableFromInline
    func findOutputs(in scriptPubKeys: Set<ScriptPubKey>) -> [SpentOutpoint.TransactionOutputs] {
        self.vout.enumerated().map { index, out in
            let scriptPubKey = ScriptPubKey(tag: .max, index: .max, opcodes: out.scriptPubKey)
            guard let index = scriptPubKeys.firstIndex(of: scriptPubKey)
            else { return .outgoing(out.value, out.scriptPubKey) }

            return .refund(out.value, scriptPubKeys[index])
        }
    }
}
