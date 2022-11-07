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
import protocol Foundation.LocalizedError
import HaByLo
import NIO

struct BlockCommand: BitcoinCommandValue {
    let description = "block"
    
    fileprivate enum State {
        case wire(WireState)
        case verified(VerifiedState)
    }
    
    private var state: State
    
    init(header: BlockHeader, transactions: [Tx.Transaction], hasWitness: Bool) {
        self.state = .wire(WireState(header: header,
                                     transactions: transactions,
                                     hasWitness: hasWitness))
    }
}

// MARK: States
extension BlockCommand {
    fileprivate struct WireState: BlockImmutableProperties {
        let header: BlockHeader
        let transactions: [Tx.Transaction]
        let hasWitness: Bool
    }
    
    fileprivate struct VerifiedState: BlockImmutableProperties {
        let header: BlockHeader
        let transactions: [Tx.Transaction]
        let hasWitness: Bool
        
        init(_ wireState: WireState, hashedHeader: BlockHeader? = nil, verifiedTransactions: [Tx.Transaction]) {
            self.header = hashedHeader ?? wireState.header
            self.transactions = verifiedTransactions
            self.hasWitness = wireState.hasWitness
        }
    }
}

// MARK: BlockError
extension BlockCommand {
    enum BlockError: Error, CustomStringConvertible, LocalizedError {
        case blockHeaderMismatch
        case identicalTransaction
        case internalError(description: String, event: StaticString)
        case notVerified
        case overWeightLimit
        case verificationFailed(merkleRootVerify: Bool, commitmentHashVerify: Bool?)

        var description: String {
            switch self {
            case .blockHeaderMismatch:
                return "blockHeaderMismatch error, expected/provided header or hash does not match downloaded data"
            case .identicalTransaction:
                return "identicalTransaction error, this should really NEVER happen CVE-2012-2459"
            case .internalError(let description, let event):
                return "internalError: \(description) for event \(event)"
            case .notVerified:
                return "notVerified error, trying to read properties from block prior to verification"
            case .overWeightLimit:
                return "overWeightLimit, block size over segwit weight or legacy size limit"
            case .verificationFailed(let merkleRootVerify, let commitmentHashVerify):
                if let commitmentHashVerify = commitmentHashVerify {
                    return "verificationFailed error merkleRootVerify [\(merkleRootVerify)] commitmentHashVerify [\(commitmentHashVerify)]"
                } else {
                    return "verificationFailed error merkleRootVerify [\(merkleRootVerify)]"
                }
            }
        }
        var errorDescription: String? { self.description }
    }
}

fileprivate protocol BlockImmutableProperties {
    var header: BlockHeader { get }
    var transactions: [Tx.Transaction] { get }
    var hasWitness: Bool { get }
}

// MARK: Verify
extension BlockCommand {
    mutating func verify(header: BlockHeader) throws {
        switch self.state {
        case .verified: return
        case .wire(let state):
            let verifiedState = try doVerification(state, header: header)
            self.state = .verified(verifiedState)
        }
    }
    
    mutating func verify(hash: BlockChain.Hash<BlockHeaderHash>) throws {
        switch self.state {
        case .verified: return
        case .wire(let state):
            let verifiedState = try doVerification(state, hash: hash)
            self.state = .verified(verifiedState)
        }
    }
}

fileprivate func transactionIds(_ transactions: [Tx.Transaction]) throws -> LazyCollection<[(txId: Tx.TxId, wtxId: Tx.WtxId)]> {
    try transactions.map { transaction -> (txId: Tx.TxId, wtxId: Tx.WtxId) in
        let txId = try transaction.getTxId()
        let wtxId = try transaction.getWtxId()
        return (txId: txId, wtxId: transaction.isCoinbase ? .zero : wtxId)
    }
    .lazy
}

fileprivate func verifyMerkleRootAndCommitmentHash(merkleRoot: BlockChain.Hash<MerkleHash>,
                                                   commitmentHash: BlockChain.Hash<MerkleHash>?,
                                                   transactions: [Tx.Transaction]) throws -> (merkleRoot: Bool, commitmentHash: Bool) {
    let ids = try transactionIds(transactions)
    let computeMerkleRoot = try BlockChain.merkleRoot(from: ids.map { $0.txId })
    let merkleRootValid = merkleRoot == computeMerkleRoot
    
    var commitmentHashValid = false
    if let commitmentHash = commitmentHash,
       let witnessReservedValue: [UInt8] = transactions.first?.vin.first?.witness?.witnessField.first {
        let witnessRoot: BlockChain.Hash<MerkleHash> = try BlockChain.merkleRoot(from: ids.map { $0.wtxId })
        let computeCommitmentHash: BlockChain.Hash<MerkleHash> = witnessRoot.appendHash(.little(witnessReservedValue))
        commitmentHashValid = commitmentHash == computeCommitmentHash
    }
    
    return (merkleRoot: merkleRootValid,
            commitmentHash: commitmentHashValid)
}

fileprivate func verifyBlock(hasWitness: Bool, merkleRoot: BlockChain.Hash<MerkleHash>, transactions: [Tx.Transaction]) throws {
    let commitmentHash = Tx.Transaction.segwitCommitmentHashField(coinbase: transactions.first)
    let (merkleRootVerify, commitmentHashVerify) = try verifyMerkleRootAndCommitmentHash(merkleRoot: merkleRoot,
                                                                                         commitmentHash: commitmentHash,
                                                                                         transactions: transactions)
    if hasWitness {
        guard merkleRootVerify && commitmentHashVerify else {
            logger.error("BlockCommand", #function, "- Validation failed merkleRootVerify[\(merkleRootVerify)] commitmentHashVerify[\(commitmentHashVerify)]")
            throw BlockCommand.BlockError.verificationFailed(merkleRootVerify: merkleRootVerify,
                                                                   commitmentHashVerify: commitmentHashVerify)
        }
        
        let scale = Settings.PROTOCOL_SEGWIT_SCALE_FACTOR
        let blockWeight = transactions.map({ $0.weight })
            .reduce(80 * scale, +)
            + (transactions.count.variableLengthCode.count * scale)
        guard blockWeight <= Settings.PROTOCOL_SEGWIT_WEIGHT_LIMIT else {
            throw BlockCommand.BlockError.overWeightLimit
        }
    } else {
        guard merkleRootVerify else {
            logger.error("BlockCommand", #function, "- Validation failed merkleRootValid[\(merkleRootVerify)]")
            throw BlockCommand.BlockError.verificationFailed(merkleRootVerify: merkleRootVerify,
                                                                   commitmentHashVerify: nil)
        }
        
        let blockSize = transactions.map({ $0.coreSize })
            .reduce(80, +)
            + transactions.count.variableLengthCode.count
        guard blockSize <= Settings.PROTOCOL_LEGACY_SIZE_LIMIT else {
            throw BlockCommand.BlockError.overWeightLimit
        }
    }
}

fileprivate func verifyTransactions(_ transactions: [Tx.Transaction]) throws -> [Tx.Transaction] {
    var verified: [Tx.Transaction] = []
    for var transaction in transactions {
        try transaction.hash()
        try transaction.validate()
        verified.append(transaction)
    }
    return verified
}

fileprivate func doVerification(_ wireState: BlockCommand.WireState,
                                header: BlockHeader) throws -> BlockCommand.VerifiedState {
    guard   wireState.header.version == header.version,
            wireState.header.merkleRoot == header.merkleRoot,
            wireState.header.timestamp == header.timestamp,
            wireState.header.difficulty == header.difficulty,
            wireState.header.nonce == header.nonce else {
                throw BlockCommand.BlockError.blockHeaderMismatch
    }
    
    let transactions = try verifyTransactions(wireState.transactions)
    
    try verifyBlock(hasWitness: wireState.hasWitness,
                    merkleRoot: wireState.header.merkleRoot,
                    transactions: transactions)

    return BlockCommand.VerifiedState(wireState, verifiedTransactions: transactions)
}

fileprivate func doVerification(_ wireState: BlockCommand.WireState,
                                hash: BlockChain.Hash<BlockHeaderHash>) throws -> BlockCommand.VerifiedState {
    var hashHeader = wireState.header
    try hashHeader.addBlockHash()
    guard try hashHeader.blockHash() == hash else {
        throw BlockCommand.BlockError.blockHeaderMismatch
    }

    let transactions = try verifyTransactions(wireState.transactions)

    try verifyBlock(hasWitness: wireState.hasWitness,
                    merkleRoot: wireState.header.merkleRoot,
                    transactions: transactions)

    return BlockCommand.VerifiedState(wireState,
                                      hashedHeader: hashHeader,
                                      verifiedTransactions: transactions)
}

// MARK: Getters/properties
extension BlockCommand {
    func getHeader() throws -> BlockHeader {
        switch self.state {
        case .verified(let state): return state.header
        case .wire: throw BlockCommand.BlockError.notVerified
        }
    }
    
    func getTransactions() throws -> [Tx.Transaction] {
        switch self.state {
        case .verified(let state): return state.transactions
        case .wire: throw BlockCommand.BlockError.notVerified
        }
    }
    
    func getHasWitness() throws -> Bool {
        switch self.state {
        case .verified(let state): return state.hasWitness
        case .wire: throw BlockCommand.BlockError.notVerified
        }
    }
    
    var isVerified: Bool {
        switch self.state {
        case .verified: return true
        case .wire: return false
        }
    }
}

// MARK: BitcoinCommandEncodable & BitcoinCommandDecodable
extension BlockCommand: BitcoinCommandEncodable {
    func write(to buffer: inout ByteBuffer) {
        switch self.state {
        case .verified(let state as BlockImmutableProperties),
             .wire(let state as BlockImmutableProperties):
            state.header.write(to: &buffer)
            assert(Int(state.header.transactions) == state.transactions.count)
            state.transactions.forEach {
                $0.write(to: &buffer)
            }
        }
    }
}

extension BlockCommand: BitcoinCommandDecodable {
    init?(fromBuffer: inout ByteBuffer) {
        let save = fromBuffer
        guard let header = BlockHeader(fromBuffer: &fromBuffer), header.transactions > 0 else {
            logger.error("BlockCommand", #function, "- Illegal blockheader data when loading block")
            fromBuffer = save
            return nil
        }

        var transactions: [Tx.Transaction] = []
        var hasWitness = false
        for _ in (0..<Int(header.transactions)) {
            if let transaction = Tx.Transaction(fromBuffer: &fromBuffer, cacheBufferForHashing: true) {
                if transaction.hasWitnesses {
                    hasWitness = true
                }
                transactions.append(transaction)
            } else {
                fromBuffer = save
                return nil
            }
        }
        
        assert(transactions.count == Int(header.transactions))
        
        self.state = .wire(
            WireState(header: header,
                      transactions: transactions,
                      hasWitness: hasWitness)
        )
    }
}

extension BlockCommand: CustomDebugStringConvertible {
    var debugDescription: String {
        switch self.state {
        case .verified(let state):
            return "BLOCK[V]\(state.hasWitness ? "[SegWit]" : "")  h[\(state.header)] tx#[\(state.transactions.count)] cb[\(state.transactions.first.map(String.init) ?? "none")]"
        case .wire(let state):
            return "BLOCK[U]\(state.hasWitness ? "[SegWit]" : "") h[\(state.header)] tx#[\(state.transactions.count)] cb[\(state.transactions.first.map(String.init) ?? "none")]"
        }
    }
}
