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
import struct Network.IPv6Address

protocol BitcoinCommandEncodable {
    func write(to buffer: inout ByteBuffer)
}

protocol BitcoinCommandDecodable {
    init?(fromBuffer: inout ByteBuffer)
}

protocol BitcoinCommandValue: CustomStringConvertible, BitcoinCommandEncodable & BitcoinCommandDecodable {
    var timeout: TimeAmount? { get }
    var writtenToNetworkPromise: Promise<Void>? { get }
}

extension BitcoinCommandValue {
    var timeout: TimeAmount? {
        return nil
    }
    
    var writtenToNetworkPromise: Promise<Void>? {
        return nil
    }

    func write(to buffer: inout ByteBuffer) {
        return
    }
    
    init?(fromBuffer: inout ByteBuffer) {
        return nil
    }
}

// MARK: BitcoinCommandBundle for BundleHandler
struct BitcoinCommandBundle<C: Collection>: BitcoinCommandValue where C.Element: BitcoinCommandValue {
    let description = "bundle of " + String(describing: C.Element.self)
    let value: C
    let notfound: NotfoundCommand?
}

extension Future where Value == BitcoinCommandValue {
    func type<T: BitcoinCommandValue>(of: T.Type = T.self) -> Future<T> {
        self.flatMapThrowing {
            guard let c = $0 as? T else {
                throw BitcoinNetworkError.castingFailed
            }
            return c
        }
    }
}

// MARK: Incoming command interpreter
extension String {
    func incomingCommand(buffer: inout ByteBuffer) -> FrameToCommandHandler.InboundOut? {
        switch self.lowercased() {
        case "addr":
            return AddrCommand(fromBuffer: &buffer)
            .flatMap {
                .unsolicited(.addr($0))
            }
        case "alert":
            return AlertCommand(fromBuffer: &buffer)
            .flatMap {
                .unsolicited(.alert($0))
            }
        case "block":
            return BlockCommand(fromBuffer: &buffer)
            .flatMap {
                .pending($0)
            }
        case "cfheaders":
            return CF.CfheadersCommand(fromBuffer: &buffer)
            .flatMap {
                .pending($0)
            }
        case "cfilter":
            return CF.CfilterCommand(fromBuffer: &buffer)
            .flatMap {
                .pending($0)
            }
        case "feefilter":
            return FeefilterCommand(fromBuffer: &buffer)
            .flatMap {
                .unsolicited(.feefilter($0))
            }
        case "getaddr":
            return .incoming(.getaddr(GetaddrCommand()))
        case "getcfcheckpt":
            return CF.GetcfcheckptCommand(fromBuffer: &buffer)
            .flatMap {
                .incoming(.getcfcheckpt($0))
            }
        case "getcfheaders":
            return CF.GetcfheadersCommand(fromBuffer: &buffer)
            .flatMap {
                .incoming(.getcfheaders($0))
            }
        case "getcfilters":
            return CF.GetcfiltersCommand(fromBuffer: &buffer)
            .flatMap {
                .incoming(.getcfilters($0))
            }
        case "getdata":
            return GetdataCommand(fromBuffer: &buffer)
            .flatMap {
                .incoming(.getdata($0))
            }
        case "getheaders":
            return GetheadersCommand(fromBuffer: &buffer)
            .flatMap {
                .incoming(.getheaders($0))
            }
        case "headers":
            return HeadersCommand(fromBuffer: &buffer)
            .flatMap {
                .pending($0)
            }
        case "inv":
            return InvCommand(fromBuffer: &buffer)
            .flatMap {
                .unsolicited(.inv($0))
            }
        case "notfound":
            return NotfoundCommand(fromBuffer: &buffer)
            .flatMap {
                .pending($0)
            }
        case "ping":
            return PingCommand(fromBuffer: &buffer)
            .flatMap {
                .incoming(.ping($0))
            }
        case "pong":
            return PongCommand(fromBuffer: &buffer)
            .flatMap {
                .pending($0)
            }
        case "reject":
            return RejectCommand(fromBuffer: &buffer)
            .flatMap {
                .unsolicited(.reject($0))
            }
        case "sendaddrv2":
            return .unsolicited(.sendaddrv2)
        case "sendcmpct":
            return SendcmpctCommand(fromBuffer: &buffer)
            .flatMap {
                .unsolicited(.sendcmpct($0))
            }
        case "sendheaders":
            return .unsolicited(.sendheaders(SendheadersCommand()))
        case "tx":
            return Tx.Transaction(fromBuffer: &buffer)
            .flatMap {
                .pending($0)
            }
        case "verack":
            return .pending(VerackCommand())
        case "version":
            return VersionCommand(fromBuffer: &buffer)
            .flatMap {
                .incoming(.version($0))
            }
        default:
            return .incoming(.unhandled(UnhandledCommand(description: self, buffer: buffer)))
        }
    }
}

// MARK: PeerAddress
enum IPAddressFormat {
    case v6(IPv6Address)
    case string(String)
    case data(Data)
}

extension IPv6Address: Codable {
    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(self.rawValue)
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        let data = try container.decode(Data.self)
        
        guard let address = IPv6Address.init(data) else {
            throw DecodingError.dataCorruptedError(in: container, debugDescription: "Cannot initialize IPv6Value from data \(data.hexEncodedString)")
        }
        self = address
    }
}

@usableFromInline
struct PeerAddress: Codable {
    public typealias IPPort = UInt16
    let time: Date
    let services: ServicesOptionSet
    let address: IPv6Address
    let port: IPPort
        
    init(time: UInt32? = nil, services: ServicesOptionSet, address: IPAddressFormat, port: IPPort) {
        var v6Address: IPv6Address
        switch address {
        case .v6(let a): v6Address = a
        case .string(let s):
            if let a = IPv6Address(s) {
                v6Address = a
            } else {
                assertionFailure("PeerAddress.\(#function) cannot init ipAddress from \(s) failing constructor with .any")
                logger.info("PeerAddress.\(#function) cannot init ipAddress from", s, "failing constructor with .any")
                v6Address = IPv6Address.any
            }
        case .data(let d):
            if let a = IPv6Address(d) {
                v6Address = a
            } else {
                assertionFailure("PeerAddress.\(#function) cannot init ipAddress from \(d.debugDescription) failing constructor with .any")
                logger.info("PeerAddress.\(#function) cannot init ipAddress from", d, "failing constructor with .any")
                v6Address = IPv6Address.any
            }
        }
        
        if let time = time {
            self.time = Date(timeIntervalSince1970: TimeInterval(time))
        } else {
            self.time = Date()
        }
        self.services = services
        self.address = v6Address
        self.port = port
    }
}

extension PeerAddress: BitcoinCommandEncodable {
    func write(to buffer: inout ByteBuffer) {
        let time = UInt32(self.time.timeIntervalSince1970)
        buffer.writeInteger(time, endianness: .little, as: UInt32.self)
        buffer.writeInteger(self.services.rawValue, endianness: .little, as: UInt64.self)
        buffer.writeBytes(self.address.rawValue)
        buffer.writeInteger(self.port, endianness: .big, as: UInt16.self)
    }
}

extension PeerAddress: BitcoinCommandDecodable {
    init?(fromBuffer: inout ByteBuffer) {
        let save = fromBuffer
        guard let timestamp: UInt32 = fromBuffer.readInteger(endianness: .little),
            let services: UInt64 = fromBuffer.readInteger(endianness: .little),
            let address: Data = fromBuffer.readData(length: 16),
            let port: UInt16 = fromBuffer.readInteger(endianness: .big) else {
                fromBuffer = save
                return nil
        }
        self.init(time: timestamp,
                  services: ServicesOptionSet(rawValue: services),
                  address: .data(address),
                  port: port)
    }
}

extension Array: BitcoinCommandEncodable where Element == PeerAddress {
    func write(to buffer: inout ByteBuffer) {
        let varInt = self.count.variableLengthCode
        buffer.writeBytes(varInt)
        self.forEach {
            $0.write(to: &buffer)
        }
    }
}

extension Array: BitcoinCommandDecodable where Element == PeerAddress {
    init?(fromBuffer: inout ByteBuffer) {
        guard let varInt = fromBuffer.readVarInt() else {
            return nil
        }
        let array: [PeerAddress] = (0..<varInt).compactMap { _ in
            PeerAddress(fromBuffer: &fromBuffer)
        }
        guard array.count == varInt else {
            return nil
        }
        self = array
    }
}

extension PeerAddress: Equatable, Hashable {
    @usableFromInline
    static func == (lhs: PeerAddress, rhs: PeerAddress) -> Bool {
        lhs.address == rhs.address
            && lhs.port == rhs.port
    }
    
    @usableFromInline
    func hash(into hasher: inout Hasher) {
        self.address.hash(into: &hasher)
        self.port.hash(into: &hasher)
    }
}

extension PeerAddress: CustomStringConvertible {
    @usableFromInline
    var description: String {
        let v4 = address.asIPv4?.debugDescription
        return String("PeerAddress ip(\(v4 ?? address.debugDescription)) port(\(port)) time(\(time)) services(\(services.rawValue))")
    }
}

// MARK: Addr
struct AddrCommand: BitcoinCommandValue {
    let description = "addr"
    let addresses: [PeerAddress]
}

extension AddrCommand: BitcoinCommandEncodable {
    func write(to buffer: inout ByteBuffer) {
        self.addresses.write(to: &buffer)
    }
}

extension AddrCommand: BitcoinCommandDecodable {
    init?(fromBuffer: inout ByteBuffer) {
        let save = fromBuffer
        let array: [PeerAddress]? = Array(fromBuffer: &fromBuffer)
        guard let notNil = array else {
            fromBuffer = save
            return nil
        }
        self.addresses = notNil
    }
}

// MARK: AlertCommand
struct AlertCommand: BitcoinCommandValue {
    let description = "alert"
}

// MARK: BlockCommand
/*
 The block message is sent in response to a getdata message which requests transaction information from a block hash.

 Field Size    Description    Data type    Comments
 4    version    int32_t    Block version information (note, this is signed)
 32    prev_block    char[32]    The hash value of the previous block this particular block references
 32    merkle_root    char[32]    The reference to a Merkle tree collection which is a hash of all transactions related to this block
 4    timestamp    uint32_t    A Unix timestamp recording when this block was created (Currently limited to dates before the year 2106!)
 4    bits    uint32_t    The calculated difficulty target being used for this block
 4    nonce    uint32_t    The nonce used to generate this block… to allow variations of the header and compute different hashes
 1+    txn_count    var_int    Number of transaction entries
  ?    txns    tx[]    Block transactions, in format of "tx" command
 The SHA256 hash that identifies each block (and which must have a run of 0 bits) is calculated from the first 6 fields of this structure (version, prev_block, merkle_root, timestamp, bits, nonce, and standard SHA256 padding, making two 64-byte chunks in all) and not from the complete block. To calculate the hash, only two chunks need to be processed by the SHA256 algorithm. Since the nonce field is in the second chunk, the first chunk stays constant during mining and therefore only the second chunk needs to be processed. However, a Bitcoin hash is the hash of the hash, so two SHA256 rounds are needed for each mining iteration. See Block hashing algorithm for details and an example.

 
 
 tx describes a bitcoin transaction, in reply to getdata. When a bloom filter is applied tx objects are sent automatically for matching transactions following the merkleblock.


 Field Size    Description    Data type    Comments
 4    version    int32_t    Transaction data format version (note, this is signed)
 0 or 2    flag    optional uint8_t[2]    If present, always 0001, and indicates the presence of witness data
 1+    tx_in count    var_int    Number of Transaction inputs (never zero)
 41+    tx_in    tx_in[]    A list of 1 or more transaction inputs or sources for coins
 1+    tx_out count    var_int    Number of Transaction outputs
 9+    tx_out    tx_out[]    A list of 1 or more transaction outputs or destinations for coins
 0+    tx_witnesses    tx_witness[]    A list of witnesses, one for each input; omitted if flag is omitted above
 4    lock_time    uint32_t    The block number or timestamp at which this transaction is unlocked:
 Value    Description
 0    Not locked
 < 500000000    Block number at which this transaction is unlocked
 >= 500000000    UNIX timestamp at which this transaction is unlocked
 If all TxIn inputs have final (0xffffffff) sequence numbers then lock_time is irrelevant. Otherwise, the transaction may not be added to a block until after lock_time (see NLockTime).

 TxIn consists of the following fields:

 Field Size    Description    Data type    Comments
 36    previous_output    outpoint    The previous output transaction reference, as an OutPoint structure
 1+    script length    var_int    The length of the signature script
  ?    signature script    uchar[]    Computational Script for confirming transaction authorization
 4    sequence    uint32_t    Transaction version as defined by the sender. Intended for "replacement" of transactions when information is updated before inclusion into a block.
 The OutPoint structure consists of the following fields:

 Field Size    Description    Data type    Comments
 32    hash    char[32]    The hash of the referenced transaction.
 4    index    uint32_t    The index of the specific output in the transaction. The first output is 0, etc.
 The Script structure consists of a series of pieces of information and operations related to the value of the transaction.

 (Structure to be expanded in the future… see script.h and script.cpp and Script for more information)

 The TxOut structure consists of the following fields:

 Field Size    Description    Data type    Comments
 8    value    int64_t    Transaction Value
 1+    pk_script length    var_int    Length of the pk_script
  ?    pk_script    uchar[]    Usually contains the public key as a Bitcoin script setting up conditions to claim this output.
 The TxWitness structure consists of a var_int count of witness data components, followed by (for each witness data component) a var_int length of the component and the raw component data itself.
 */

// MARK: FeefilterCommand
struct FeefilterCommand: BitcoinCommandValue {
    let description = "feefilter"
    let feerate: UInt64
}

extension FeefilterCommand: BitcoinCommandEncodable {
    func write(to buffer: inout ByteBuffer) {
        buffer.writeInteger(self.feerate, endianness: .little)
    }
}

extension FeefilterCommand: BitcoinCommandDecodable {
    init?(fromBuffer: inout ByteBuffer) {
        let save = fromBuffer
        guard let feerate: UInt64 = fromBuffer.readInteger(endianness: .little) else {
            fromBuffer = save
            return nil
        }
        self.feerate = feerate
    }
}

// MARK: GetAddr
struct GetaddrCommand: BitcoinCommandValue {
    let description = "getaddr"
}

// Outgoing getaddr command
extension Client {
    func outgoingGetaddr() -> Future<Void> {
        self.immediate(GetaddrCommand())
    }
}

// MARK: GetblocksCommand
// NOTE: Starts with block following the first blocklocator hash
// Getting 500 blocks (maximum) is seen as an error/mismatch, hence maximum locator range
// for this command is 499.
struct GetblocksCommand: BitcoinCommandValue {
    let description = "getblocks"
    let blockLocator: BlockLocator
    
    let timeout: TimeAmount?
    
    init(blockLocator: BlockLocator, timeout: TimeAmount? = nil) {
        self.blockLocator = blockLocator
        self.timeout = timeout
    }
}

extension GetblocksCommand {
    func filter(_ inventory: [Inventory]) -> Result<[Inventory], InventoryBundleCommand.Skip> {
        let matching = inventory.filter {
            if case .block = $0 {
                return true
            }
            return false
        }

        guard !matching.isEmpty else {
            return .failure(InventoryBundleCommand.Skip())
        }
        
        guard matching.count < Settings.PROTOCOL_MAX_GETBLOCKS else {
            return .success([])
        }
        
        return .success(matching)
    }
}

extension GetblocksCommand: BitcoinCommandEncodable {
    func write(to buffer: inout ByteBuffer) {
        self.blockLocator.write(to: &buffer)
    }
}

// Outgoing
extension Client {
    func outgoingGetblocks(locator: BlockLocator,
                           timeout: TimeAmount? = nil) -> Future<[BlockCommand]> {
        let getBlocks = GetblocksCommand(blockLocator: locator, timeout: timeout)
        let bundle: InventoryBundleCommand = .block(getBlocks)
        
        return self.pending(bundle)
        .type(of: BitcoinCommandBundle<[BlockCommand]>.self)
        .map(\.value)
        .flatMapErrorThrowing {
            switch $0 {
            case BitcoinNetworkError.emptyCommandReceived:
                return []
            default:
                throw $0
            }
        }
    }
}

// MARK: GetdataCommand
struct GetdataCommand: BitcoinCommandValue {
    let description = "getdata"
    let inventory: [Inventory]
    let timeout: Optional<TimeAmount>
    let writtenToNetworkPromise: Optional<EventLoopPromise<Void>>

    init(inventory: [Inventory],
         timeout: TimeAmount? = nil,
         writtenToNetworkPromise: Promise<Void>? = nil) {
        self.inventory = inventory
        self.timeout = timeout
        self.writtenToNetworkPromise = writtenToNetworkPromise
    }
}

extension GetdataCommand: BitcoinCommandEncodable {
    func write(to buffer: inout ByteBuffer) {
        buffer.writeBytes(self.inventory.count.variableLengthCode)
        self.inventory.forEach {
            $0.write(to: &buffer)
        }
    }
}

extension GetdataCommand: BitcoinCommandDecodable {
    init?(fromBuffer: inout ByteBuffer) {
        let save = fromBuffer
        guard let invCount = fromBuffer.readVarInt() else {
            fromBuffer = save
            return nil
        }
        var invs: [Inventory] = []
        for _ in (0..<invCount) {
            guard let inv = Inventory(fromBuffer: &fromBuffer) else {
                fromBuffer = save
                return nil
            }
            invs.append(inv)
        }
        assert(Int(invCount) == invs.count)
        self.init(inventory: invs)
    }
}

// Outgoing getdata
extension Client {
    fileprivate func outgoingGetdata<T: BitcoinCommandValue>(_ inventoryType: Inventory,
                                                             timeout: TimeAmount? = nil)
    -> (write: Future<Void>, result: Future<T>) {
        let writePromise = self.eventLoop.makePromise(of: Void.self)
        
        let result: Future<T> = self.pending(
            GetdataCommand(inventory: [ inventoryType ],
                           timeout: timeout,
                           writtenToNetworkPromise: writePromise)
        )
        .flatMapThrowing {
            if case let notFound as NotfoundCommand = $0 {
                logger.error("Client", #function, "- Notfound error when requesting inventory", notFound.inventory)
                throw BitcoinNetworkError.inventoryNotFound
            } else {
                return $0
            }
        }
        .type(of: T.self)
        
        return (writePromise.futureResult, result)
    }
    
    func outgoingGetdataBlock(_ hash: BlockChain.Hash<BlockHeaderHash>,
                              timeout: TimeAmount? = nil) -> Future<BlockCommand> {
        self.outgoingGetdata(.witnessBlock(hash),
                             timeout: timeout).result
    }
    
    func outgoingGetdataBlockWithWriteFuture(_ hash: BlockChain.Hash<BlockHeaderHash>,
                                             timeout: TimeAmount? = nil)
    -> (write: Future<Void>, result: Future<BlockCommand>) {
        self.outgoingGetdata(.witnessBlock(hash),
                             timeout: timeout)
    }
    
    func outgoingGetdataNoWitnessesBlock(_ hash: BlockChain.Hash<BlockHeaderHash>,
                                         timeout: TimeAmount? = nil) -> Future<BlockCommand> {
        self.outgoingGetdata(.block(hash),
                             timeout: timeout).result
    }

    func outgoingGetdataNoWitnessesBlockWithWriteFuture(_ hash: BlockChain.Hash<BlockHeaderHash>,
                                                        timeout: TimeAmount? = nil)
    -> (write: Future<Void>, result: Future<BlockCommand>) {
        self.outgoingGetdata(.block(hash),
                             timeout: timeout)
    }

    func outgoingGetdataTx(_ hash: BlockChain.Hash<TransactionLegacyHash>) -> Future<Tx.Transaction> {
        self.outgoingGetdata(.witnessTx(hash)).result
    }
}

// MARK: CompactFilters
extension Client {
// TODO: Evaluate promise based command for better cancellation handling
//    func outgoingGetcfHeaders(startHeight: Int, stop: BlockChain.Hash<BlockHeaderHash>,
//                              with timeout: TimeAmount? = nil,
//                              promise: EventLoopPromise<CF.CfheadersCommand>) {
//        self.pending(CF.GetcfheadersCommand(filterType: .basic,
//                                            startHeight: UInt32(startHeight),
//                                            stop: stop,
//                                            timeout: timeout),
//                     promise: promise)
//    }
    
    func outgoingGetcfheaders(startHeight: Int, stop: BlockChain.Hash<BlockHeaderHash>,
                              with timeout: TimeAmount? = nil) -> Future<CF.CfheadersCommand> {
        self.pending(CF.GetcfheadersCommand(filterType: .basic,
                                            startHeight: UInt32(startHeight),
                                            stop: stop,
                                            timeout: timeout))
        .type(of: CF.CfheadersCommand.self)
    }
    
    func outgoingGetcfilter(serial header: BlockHeader,
                            with timeout: TimeAmount? = nil) -> Future<CF.CfilterCommand> {
        self.pending(CF.GetcfiltersCommand(filterType: .basic,
                                           startHeight: UInt32(header.id),
                                           stopHash: try! header.blockHash(),
                                           timeout: timeout))
        .type(of: CF.CfilterCommand.self)
    }
    
    func outgoingGetcfilterWithWriteFuture(
        serial header: BlockHeader,
        with timeout: TimeAmount? = nil
    ) -> (write: Future<Void>, result: Future<CF.CfilterCommand>) {
        let writePromise = self.eventLoop.makePromise(of: Void.self)
        let cfilterFuture = self.pending(CF.GetcfiltersCommand(filterType: .basic,
                                                               startHeight: UInt32(header.id),
                                                               stopHash: try! header.blockHash(),
                                                               timeout: timeout,
                                                               writtenToNetworkPromise: writePromise))
        .type(of: CF.CfilterCommand.self)
        
        return (writePromise.futureResult, cfilterFuture)
    }

    func outgoingGetCfBundle(startHeight start: Int,
                             stopHash hash: BlockChain.Hash<BlockHeaderHash>,
                             count: Int,
                             with timeout: TimeAmount? = nil) -> Future<[CF.CfilterCommand]> {
        let getCommand: CF.GetcfiltersCommand = .init(filterType: .basic,
                                                      startHeight: UInt32(start),
                                                      stopHash: hash,
                                                      timeout: timeout)
        return self.outgoingGetCfBundle(getCommand, count: count)
    }

    
    func outgoingGetCfBundle(_ command: CF.GetcfiltersCommand,
                             count: Int) -> Future<[CF.CfilterCommand]> {
        self.pending(CFBundleCommand.init(command: command, count: count))
        .type(of: BitcoinCommandBundle<[CF.CfilterCommand]>.self)
        .map(\.value)
    }
    
    func outgoingGetCfBundleWithWriteFuture(
        startHeight start: Int,
        stopHash hash: BlockChain.Hash<BlockHeaderHash>,
        count: Int,
        with timeout: TimeAmount? = nil
    ) -> (write: Future<Void>, result: Future<[CF.CfilterCommand]>) {
        let writePromise = self.eventLoop.makePromise(of: Void.self)
        let getCommand: CF.GetcfiltersCommand = .init(filterType: .basic,
                                                      startHeight: UInt32(start),
                                                      stopHash: hash,
                                                      timeout: timeout,
                                                      writtenToNetworkPromise: writePromise)
        let filters = self.outgoingGetCfBundle(getCommand, count: count)
        
        return (writePromise.futureResult, filters)
    }
}

// MARK: GetheadersCommand
struct GetheadersCommand: BitcoinCommandValue {
    let description = "getheaders"
    let blockLocator: BlockLocator
    
    let timeout: TimeAmount?
    
    init(blockLocator: BlockLocator, timeout: TimeAmount? = nil) {
        self.blockLocator = blockLocator
        self.timeout = timeout
    }
}

extension GetheadersCommand: BitcoinCommandEncodable {
    func write(to buffer: inout ByteBuffer) {
        self.blockLocator.write(to: &buffer)
    }
}

extension GetheadersCommand: BitcoinCommandDecodable {
    init?(fromBuffer: inout ByteBuffer) {
        let save = fromBuffer
        guard let blockLocator = BlockLocator(fromBuffer: &fromBuffer) else {
            fromBuffer = save
            return nil
        }
        
        self.init(blockLocator: blockLocator)
    }
}

// Outgoing headers command
extension Client {
    func outgoingGetheaders(locator: BlockLocator,
                            with timeout: TimeAmount? = nil) -> Future<[BlockHeader]> {
        self.pending(GetheadersCommand(blockLocator: locator, timeout: timeout))
        .type(of: HeadersCommand.self)
        .map(\.headers)
    }
}

// MARK: BlockLocator
struct BlockLocator {
    @Setting(\.PROTOCOL_VERSION) var version: Int32
    public let hashes: [BlockChain.Hash<BlockHeaderHash>]
    let stop: BlockChain.Hash<BlockHeaderHash>
    
    init(version: Int32, hashes: [BlockChain.Hash<BlockHeaderHash>], stop: BlockChain.Hash<BlockHeaderHash>) {
        self.hashes = hashes
        self.stop = stop
        self.version = version
    }
    
    init(hashes: [BlockChain.Hash<BlockHeaderHash>], stop: BlockChain.Hash<BlockHeaderHash>) {
        self.hashes = hashes
        self.stop = stop
    }
}

extension BlockLocator: BitcoinCommandEncodable {
    func write(to buffer: inout ByteBuffer) {
        buffer.writeInteger(self.version, endianness: .little)
        buffer.writeBytes(self.hashes.count.variableLengthCode)
        self.hashes.forEach {
            buffer.writeBytes($0.littleEndian)
        }
        buffer.writeBytes(self.stop.littleEndian)
    }
}

extension BlockLocator: BitcoinCommandDecodable {
    init?(fromBuffer: inout ByteBuffer) {
        let save = fromBuffer
        guard let version: Int32 = fromBuffer.readInteger(endianness: .little),
            let varInt = fromBuffer.readVarInt(),
            var slice = fromBuffer.readSlice(length: Int(varInt + 1) * 32) else {
                fromBuffer = save
                return nil
        }
        let hashes: [BlockChain.Hash<BlockHeaderHash>] = (0..<varInt).map { _ in
            .little(slice.readBytes(length: 32)!)
        }
        self.init(version: version, hashes: hashes, stop: .little(slice.readBytes(length: 32)!))
    }
}

extension BlockLocator: CustomStringConvertible {
    public var description: String {
        "BlockLocator.hashes[\(self.hashes.map { String(describing: $0) }.joined(separator: ", "))]"
            + (self.stop == .zero ? "" : " stop[\(self.stop)]")
    }
}

// MARK: HeadersCommand
// See BlockHeader.swift

struct HeadersCommand: BitcoinCommandValue {
    let description = "headers"
    let headers: [BlockHeader]
}

extension HeadersCommand: BitcoinCommandEncodable {
    func write(to buffer: inout ByteBuffer) {
        buffer.writeBytes(self.headers.count.variableLengthCode)
        self.headers.forEach {
            $0.write(to: &buffer)
        }
    }
}

extension HeadersCommand: BitcoinCommandDecodable {
    init?(fromBuffer: inout ByteBuffer) {
        let save = fromBuffer
        guard let headerCount = fromBuffer.readVarInt() else {
            fromBuffer = save
            return nil
        }
        var headers: [BlockHeader] = []
        for _ in (0..<headerCount) {
            guard let header = BlockHeader(fromBuffer: &fromBuffer) else {
                logger.error("HeadersCommand", #function, "- Failed reading header, returning nil")
                fromBuffer = save
                return nil
            }
            headers.append(header)
        }
        assert(headers.count == headerCount)
        self.headers = headers
    }
}

// MARK: InvCommand
struct InvCommand: BitcoinCommandValue {
    let description = "inv"
    let inventory: [Inventory]
}

extension InvCommand: BitcoinCommandEncodable {
    func write(to buffer: inout ByteBuffer) {
        buffer.writeBytes(self.inventory.count.variableLengthCode)
        self.inventory.forEach {
            $0.write(to: &buffer)
        }
    }
}

extension InvCommand: BitcoinCommandDecodable {
    init?(fromBuffer: inout ByteBuffer) {
        let save = fromBuffer
        guard let headerCount = fromBuffer.readVarInt() else {
            fromBuffer = save
            return nil
        }
        var inventories: [Inventory] = []
        for _ in (0..<headerCount) {
            guard let inventory = Inventory(fromBuffer: &fromBuffer) else {
                fromBuffer = save
                return nil
            }
            inventories.append(inventory)
        }
        assert(inventories.count == Int(headerCount))
        self.inventory = inventories
    }
}

enum Inventory: Equatable {
    case error
    case tx(BlockChain.Hash<TransactionLegacyHash>)
    case block(BlockChain.Hash<BlockHeaderHash>)
    case filteredBlock(BlockChain.Hash<BlockHeaderHash>)
    case compactBlock(BlockChain.Hash<BlockHeaderHash>)
    case witnessTx(BlockChain.Hash<TransactionLegacyHash>)
    case witnessBlock(BlockChain.Hash<BlockHeaderHash>)
    
    var vectorType: UInt32 {
        switch self {
        case .error: return 0
        case .tx: return 1
        case .block: return 2
        case .filteredBlock: return 3
        case .compactBlock: return 4
        case .witnessTx: return 0x40000001
        case .witnessBlock: return 0x40000002
        }
    }
    
    static func from(vector type: UInt32, hash: ArraySlice<UInt8>) -> Self? {
        switch type {
        case 0: return .error
        case 1: return .tx(.little(hash))
        case 2: return .block(.little(hash))
        case 3: return .filteredBlock(.little(hash))
        case 4: return .compactBlock(.little(hash))
        case 0x40000001: return .witnessTx(.little(hash))
        case 0x40000002: return .witnessBlock(.little(hash))
        default: return nil
        }
    }
    
    var littleEndian: AnyBidirectionalCollection<UInt8> {
        switch self {
        case .error: return BlockChain.Hash<BlockHeaderHash>.zero.littleEndian
        case .tx(let hash): return hash.littleEndian
        case .block(let hash): return hash.littleEndian
        case .filteredBlock(let hash): return hash.littleEndian
        case .compactBlock(let hash): return hash.littleEndian
        case .witnessTx(let hash): return hash.littleEndian
        case .witnessBlock(let hash): return hash.littleEndian
        }
    }
}

extension Inventory: BitcoinCommandEncodable {
    func write(to buffer: inout ByteBuffer) {
        buffer.writeInteger(self.vectorType, endianness: .little)
        buffer.writeBytes(self.littleEndian)
    }
}

extension Inventory: BitcoinCommandDecodable {
    init?(fromBuffer: inout ByteBuffer) {
        let save = fromBuffer
        guard let vectorTypeInt: UInt32 = fromBuffer.readInteger(endianness: .little),
            let hash = fromBuffer.readBytes(length: 32),
            let inventoryType: Inventory = .from(vector: vectorTypeInt, hash: hash[...]) else {
                fromBuffer = save
                return nil
        }

        self = inventoryType
    }
}

// MARK: Inventory Bundler for getblocks and mempool
struct InventoryBundleCommand: BitcoinCommandValue {
    enum CommandType {
        case block(GetblocksCommand)
        case tx(MempoolCommand)
        
        var description: String {
            switch self {
            case .block(let command as BitcoinCommandValue),
                 .tx(let command as BitcoinCommandValue):
                return command.description
            }
        }
        
        var filter: FilterType {
            switch self {
            case .block(let getBlocks):
                return getBlocks.filter(_:)
            case .tx(let mempool):
                return mempool.filter(_:)
            }
        }
        
        var timeout: TimeAmount? {
            switch self {
            case .block(let command as BitcoinCommandValue),
                 .tx(let command as BitcoinCommandValue):
               return command.timeout
           }
        }
    }

    struct Skip: Swift.Error {}
    typealias FilterType = ([Inventory]) -> Result<[Inventory], Skip>

    let command: CommandType
    
    static func block(_ getBlocks: GetblocksCommand) -> Self {
        .init(command: .block(getBlocks))
    }
    
    static func tx(_ mempool: MempoolCommand) -> Self {
        .init(command: .tx(mempool))
    }
    
    var description: String {
        self.command.description
    }

    var filter: FilterType {
        self.command.filter
    }

    var timeout: TimeAmount? {
        self.command.timeout
    }
    
    func write(to buffer: inout ByteBuffer) {
        switch self.command {
        case .block(let command as BitcoinCommandValue),
             .tx(let command as BitcoinCommandValue):
            command.write(to: &buffer)
        }
    }
}

// MARK: MempoolCommand
struct MempoolCommand: BitcoinCommandValue {
    let description = "mempool"
    let transactionHashes: [BlockChain.Hash<TransactionLegacyHash>]?
    
    let timeout: TimeAmount?
    
    func filter(_ inventory: [Inventory]) -> Result<[Inventory], InventoryBundleCommand.Skip> {
        var result: [Inventory] = []
        for inv in inventory {
            switch inv {
            case .tx(let hash):
                let remove: Bool = (self.transactionHashes?.contains(hash)) ?? false
                if !remove {
                    result.append(inv)
                }
            case .block, .compactBlock, .error, .filteredBlock, .witnessBlock, .witnessTx:
                return .failure(InventoryBundleCommand.Skip())
            }
        }
        
        return .success(result)
    }
}

// Outgoing command
extension Client {
    func outgoingMempool(knownHashes: [BlockChain.Hash<TransactionLegacyHash>]?,
                         timeout: TimeAmount? = nil) -> Future<[Tx.Transaction]> {
        let mempoolCommand = MempoolCommand(transactionHashes: knownHashes, timeout: timeout)
        let bundle = InventoryBundleCommand.tx(mempoolCommand)
        
        return self.pending(bundle)
        .type(of: BitcoinCommandBundle<[Tx.Transaction]>.self)
        .map(\.value)
        .flatMapErrorThrowing {
            switch $0 {
            case BitcoinNetworkError.emptyCommandReceived:
                return []
            default:
                throw $0
            }
        }
    }
}

// MARK: NotfoundCommand
struct NotfoundCommand: BitcoinCommandValue & Equatable {
    let description = "notfound"
    let inventory: [Inventory]
}

extension NotfoundCommand: BitcoinCommandEncodable {
    func write(to buffer: inout ByteBuffer) {
        buffer.writeBytes(self.inventory.count.variableLengthCode)
        self.inventory.forEach {
            $0.write(to: &buffer)
        }
    }
}

extension NotfoundCommand: BitcoinCommandDecodable {
    init?(fromBuffer: inout ByteBuffer) {
        let save = fromBuffer
        guard let invCount = fromBuffer.readVarInt() else {
            fromBuffer = save
            return nil
        }
        var invs: [Inventory] = []
        for _ in 0..<invCount {
            guard let inv = Inventory(fromBuffer: &fromBuffer) else {
                fromBuffer = save
                return nil
            }
            invs.append(inv)
        }
        self.inventory = invs
    }
}

// MARK: PingCommand
struct PingCommand: BitcoinCommandValue & Equatable {
    let description = "ping"
    let nonce: UInt64
    let timeout: TimeAmount?
    
    init(nonce: UInt64, timeout: TimeAmount? = nil) {
        self.nonce = nonce
        self.timeout = timeout
    }
    
    init() {
        self.init(nonce: .random(in: 0 ... .max))
    }
}

extension PingCommand: BitcoinCommandEncodable {
    func write(to buffer: inout ByteBuffer) {
        buffer.writeInteger(self.nonce, endianness: .little)
    }
}

extension PingCommand: BitcoinCommandDecodable {
    init?(fromBuffer: inout ByteBuffer) {
        let save = fromBuffer
        guard let nonce: UInt64 = fromBuffer.readInteger(endianness: .little) else {
            fromBuffer = save
            return nil
        }
        
        self.init(nonce: nonce)
    }
}

extension Client {
    func outgoingPing() -> Future<PongCommand> {
        self.pending(PingCommand())
        .type(of: PongCommand.self)
    }
}

// MARK: PongCommand
struct PongCommand: BitcoinCommandValue & BitcoinLatencyTimerProperty {
    let description = "pong"
    let nonce: UInt64
    let _latencyTimer: BitcoinCommandTimer = .init()
}

extension PongCommand: BitcoinCommandEncodable {
    func write(to buffer: inout ByteBuffer) {
        buffer.writeInteger(self.nonce, endianness: .little)
    }
}

extension PongCommand: BitcoinCommandDecodable {
    init?(fromBuffer: inout ByteBuffer) {
        let save = fromBuffer
        guard let nonce: UInt64 = fromBuffer.readInteger(endianness: .little) else {
            fromBuffer = save
            return nil
        }
        self.nonce = nonce
    }
}

// MARK: RejectCommand
struct RejectCommand: BitcoinCommandValue {
    let description = "reject"
    let message: String
    let ccode: CCode
    let reason: String
    let hash: BlockChain.Hash<Unknown>?
}

extension RejectCommand {
    enum Unknown {}
    
    enum CCode: UInt8 {
        case malformed = 0x01
        case invalid = 0x10
        case obsolete = 0x11
        case duplicate = 0x12
        case nonstandard = 0x40
        case dust = 0x41
        case insufficientFee = 0x42
        case checkpoint = 0x43
    }
}

extension RejectCommand: BitcoinCommandEncodable {
    func write(to buffer: inout ByteBuffer) {
        buffer.writeBytes(self.message.count.variableLengthCode)
        buffer.writeBytes(self.message.ascii)
        buffer.writeInteger(self.ccode.rawValue)
        buffer.writeBytes(self.reason.count.variableLengthCode)
        buffer.writeBytes(self.reason.ascii)
        if let hash = self.hash {
            buffer.writeBytes(hash.littleEndian)
        }
    }
}

extension RejectCommand: BitcoinCommandDecodable {
    init?(fromBuffer buffer: inout ByteBuffer) {
        let save = buffer
        guard let messageSize = buffer.readVarInt(),
              let message = buffer.readString(length: Int(messageSize), encoding: .ascii),
              let ccode = buffer.readInteger(as: UInt8.self)
                .flatMap(RejectCommand.CCode.init(rawValue:)),
              let reasonSize = buffer.readVarInt(),
              let reason = buffer.readString(length: Int(reasonSize), encoding: .ascii)
        else {
            buffer = save
            return nil
        }
        
        let hash: BlockChain.Hash<Unknown>? = {
            guard let rawHash = buffer.readBytes(length: 32),
                  buffer.readableBytes == 0
            else { return nil }
            
            return .little(rawHash)
        }()
        
        self.message = message
        self.ccode = ccode
        self.reason = reason
        self.hash = hash
    }
}

// MARK: Sendaddrv2Command
struct Sendaddrv2Command: BitcoinCommandValue {
    let description = "sendaddrv2"
}

// MARK: SendcmpctCommand
struct SendcmpctCommand: BitcoinCommandValue {
    let description = "sendcmpct"
    let includingNewBlocks: Bool
    let version: UInt64
}

extension SendcmpctCommand: BitcoinCommandEncodable {
    func write(to buffer: inout ByteBuffer) {
        buffer.writeInteger(self.includingNewBlocks ? UInt8(1) : UInt8(0))
        buffer.writeInteger(self.version, endianness: .little)
    }
}

extension SendcmpctCommand: BitcoinCommandDecodable {
    init?(fromBuffer: inout ByteBuffer) {
        let save = fromBuffer
        guard let includingNewBlocks: Bool = fromBuffer.readInteger(as: UInt8.self).flatMap({
            switch $0 {
            case 0: return false
            case 1: return true
            default: return nil
            }
        }),
            let version: UInt64 = fromBuffer.readInteger(endianness: .little) else {
                fromBuffer = save
                return nil
        }
        self.includingNewBlocks = includingNewBlocks
        self.version = version
    }
}

// MARK: SendheadersCommand
struct SendheadersCommand: BitcoinCommandValue {
    let description = "sendheaders"
}

// MARK: TxCommand
// See Tx.Transaction in Transaction.swift

// MARK: VerackCommand
struct VerackCommand: BitcoinCommandValue {
    let description = "verack"
}

// MARK: VersionCommand
struct VersionCommand: BitcoinCommandValue {
    let description = "version"
    let versionNumber: Int32
    let services: ServicesOptionSet
    let timestamp: Date
    let recipientAddress: PeerAddress
    let senderAddress: PeerAddress
    let nonce: UInt64
    let userAgent: String
    let startHeight: UInt32
    let relay: Bool
}

extension VersionCommand: BitcoinCommandEncodable {
    func write(to buffer: inout ByteBuffer) {
        buffer.writeInteger(self.versionNumber, endianness: .little)
        buffer.writeInteger(self.services.rawValue, endianness: .little)
        buffer.writeInteger(Int64(self.timestamp.timeIntervalSince1970), endianness: .little)
        buffer.writeInteger(self.recipientAddress.services.rawValue, endianness: .little)
        buffer.writeBytes(self.recipientAddress.address.rawValue)
        buffer.writeInteger(self.recipientAddress.port, endianness: .big)
        buffer.writeInteger(self.senderAddress.services.rawValue, endianness: .little)
        buffer.writeBytes(self.senderAddress.address.rawValue)
        buffer.writeInteger(self.senderAddress.port, endianness: .big)
        buffer.writeInteger(self.nonce, endianness: .little)
        buffer.writeBytes(self.userAgent.count.variableLengthCode)
        buffer.writeBytes(self.userAgent.ascii)
        buffer.writeInteger(self.startHeight, endianness: .little)
        buffer.writeInteger(self.relay ? UInt8(1) : (UInt8(0)))
    }
}

extension VersionCommand: BitcoinCommandDecodable {
    init?(fromBuffer: inout ByteBuffer) {
        let save = fromBuffer
        guard let version: Int32 = fromBuffer.readInteger(endianness: .little),
            let services: UInt64 = fromBuffer.readInteger(endianness: .little),
            let timestamp: Int64 = fromBuffer.readInteger(endianness: .little),
            let recipientServices: UInt64 = fromBuffer.readInteger(endianness: .little),
            let recipientIP: Data = fromBuffer.readData(length: 16),
            let recipientPort: UInt16 = fromBuffer.readInteger(endianness: .big),
            let senderServices: UInt64 = fromBuffer.readInteger(endianness: .little),
            let senderIP: Data = fromBuffer.readData(length: 16),
            let senderPort: UInt16 = fromBuffer.readInteger(endianness: .big),
            let nonce: UInt64 = fromBuffer.readInteger(endianness: .little),
            let variableInteger = fromBuffer.readVarInt(),
            let userAgent: String = fromBuffer.readString(length: Int(variableInteger), encoding: .ascii),
            let startHeight: UInt32 = fromBuffer.readInteger(endianness: .little),
            let relay: Bool = fromBuffer.readInteger(endianness: .little, as: UInt8.self).flatMap({ $0 == 1 }) else {
                fromBuffer = save
                return nil
        }
        let recipientPeerAddress = PeerAddress(time: UInt32(truncatingIfNeeded: timestamp),
                                               services: ServicesOptionSet(rawValue: recipientServices),
                                               address: .data(senderIP),
                                               port: senderPort)
        let senderPeerAddress = PeerAddress(time: UInt32(truncatingIfNeeded: timestamp),
                                            services: ServicesOptionSet(rawValue: senderServices),
                                            address: .data(recipientIP),
                                            port: recipientPort)
        self.versionNumber = version
        self.services = ServicesOptionSet(rawValue: services)
        self.timestamp = Date(timeIntervalSince1970: TimeInterval(timestamp))
        self.recipientAddress = recipientPeerAddress
        self.senderAddress = senderPeerAddress
        self.nonce = nonce
        self.userAgent = userAgent
        self.startHeight = startHeight
        self.relay = relay
    }
}

// Outgoing version command
extension Client {
    func version(height: UInt32) -> Future<VerackCommand> {
        let recipientAddress = PeerAddress(time: 0,
                                           services: [],
                                           address: .v6(self.hostPort?.host ?? IPv6Address.loopback),
                                           port: self.hostPort?.port ?? 0)
        let senderAddress = PeerAddress(time: 0,
                                        services: self.protocolServiceOptions,
                                        address: .v6(.zero),
                                        port: 0)
        let versionCommand = VersionCommand(versionNumber: self.versionVersion,
                                            services: self.protocolServiceOptions,
                                            timestamp: Date(),
                                            recipientAddress: recipientAddress,
                                            senderAddress: senderAddress,
                                            nonce: .random(in: 0 ... .max),
                                            userAgent: self.userAgent,
                                            startHeight: height,
                                            relay: self.protocolRelay)
        return self.pending(versionCommand)
        .type(of: VerackCommand.self)
    }
}

//MARK: UnhandledCommand
struct UnhandledCommand: BitcoinCommandValue {
    let description: String
    let buffer: ByteBuffer
}
