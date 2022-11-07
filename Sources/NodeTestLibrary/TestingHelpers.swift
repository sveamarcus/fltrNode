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
@testable import fltrNode
import fltrECC
import FileRepo
import fltrTx
import fltrWAPI
import HaByLo
import Network
import NIO
import NIOTransportServices
import XCTest
import Darwin.C

public final class TestingHelpers {}

extension TestingHelpers {
    public static func diSetup(settings: ((inout NodeSettings) -> Void)? = nil) {
        var testing = NodeSettings.testTestnet
        settings?(&testing)
        
        Settings = testing
    }
    
    public final class DB: XCTestCase {
        public static func openFileHandle() throws -> NIOFileHandle {
            try NIOFileHandle(path: Settings.BITCOIN_BLOCKHEADER_FILEPATH,
                               mode: [ .read, .write, ],
                               flags: .allowFileCreation(posixMode: 0o600))
        }
        
        public static func getFileBlockHeaderRepo(offset: Int, threadPool: NIOThreadPool, eventLoop: EventLoop) -> FileBlockHeaderRepo {
            let nonBlockingFileIODependency: (NIOThreadPool) -> NonBlockingFileIOClient = Settings.NONBLOCKING_FILE_IO
            let nonBlockingFileIO = nonBlockingFileIODependency(threadPool)
            var openFileHandle: NIOFileHandle!
            XCTAssertNoThrow(
                openFileHandle = try Self.openFileHandle()
            )

            return FileBlockHeaderRepo(eventLoop: eventLoop,
                                          nonBlockingFileIO: nonBlockingFileIO,
                                          nioFileHandle: openFileHandle,
                                          offset: offset)
        }

        public static func getFileCFHeaderRepo(offset: Int, threadPool: NIOThreadPool, eventLoop: EventLoop) -> FileCFHeaderRepo {
            let nonBlockingFileIODependency: (NIOThreadPool) -> NonBlockingFileIOClient = Settings.NONBLOCKING_FILE_IO
            let nonBlockingFileIO = nonBlockingFileIODependency(threadPool)
            var openFileHandle: NIOFileHandle!
            XCTAssertNoThrow(
                openFileHandle = try Self.openFileHandle()
            )

            return FileCFHeaderRepo(
                eventLoop: eventLoop,
                nonBlockingFileIO: nonBlockingFileIO,
                nioFileHandle: openFileHandle,
                offset: offset
            )
        }
        
        @discardableResult
        public static func writeFakeBlockHeaders(count: Int, eventLoop: EventLoop) -> [BlockHeader] {
            var testData: [BlockHeader]!
            XCTAssertNoThrow(
                testData = try BlockHeaderMock.TestChain.createInvalid(length: count).map {
                    var copy = $0
                    try copy.serial()
                    return copy
                }
            )

            let threadPool = NIOThreadPool(numberOfThreads: 2)
            threadPool.start()
            let repo = Self.getFileBlockHeaderRepo(offset: 0,
                                                   threadPool: threadPool,
                                                   eventLoop: eventLoop)
            
            XCTAssertNoThrow(
                try testData.forEach {
                    try repo.write($0).wait()
                }
            )
            
            XCTAssertNoThrow(try repo.close().wait())
            XCTAssertNoThrow(try threadPool.syncShutdownGracefully())
            
            return testData
        }
        
        @discardableResult
        public static func writeFakeCompactFilters(count: Int, processed: Int, eventLoop: EventLoop) -> ([CF.Header], [CF.SerialHeader]) {
            let wireFilterHeaders = Self.makeWireFilterHeaders(count: count)
            let serialFilterHeaders = wireFilterHeaders.enumerated().map(Self.wireToSerial(processed: processed))
            
            let threadPool = NIOThreadPool(numberOfThreads: 2)
            threadPool.start()
            let repo = Self.getFileCFHeaderRepo(offset: 0,
                                                threadPool: threadPool,
                                                eventLoop: eventLoop)
            
            XCTAssertNoThrow(
                try serialFilterHeaders.forEach {
                    try repo.write($0).wait()
                }
            )
            
            XCTAssertNoThrow(try repo.close().wait())
            XCTAssertNoThrow(try threadPool.syncShutdownGracefully())
            
            return (wireFilterHeaders, serialFilterHeaders)
        }
        
        public static func makeWireFilterHeaders(count: Int) -> [CF.Header] {
            var previousHeaderHash = BlockChain.Hash<CompactFilterHeaderHash>.zero
            return (0..<count).map { _ in
                let currentHash: BlockChain.Hash<CompactFilterHeaderHash> = .makeHash(from:
                    [BlockChain.Hash<CompactFilterHeaderHash>.zero.littleEndian, previousHeaderHash.littleEndian].joined()
                )
                let cfHeader = CF.Header(filterHash: .zero,
                                         previousHeaderHash: previousHeaderHash,
                                         headerHash: currentHash)
                previousHeaderHash = currentHash
                return cfHeader
            }
        }
        
        public static func makeEmptyFilterAndHeader(previousHeaderHash: BlockChain.Hash<CompactFilterHeaderHash>,
                                                    blockHeader: BlockHeader)
        -> (wire: CF.Header, serial: CF.SerialHeader, cf: CF.CfilterCommand) {
            let dataHash: BlockChain.Hash<CompactFilterHash> = .makeHash(from: [ 0x00, ])
            let headerHash: BlockChain.Hash<CompactFilterHeaderHash> = dataHash.appendHash(previousHeaderHash)

            let cfHeader: CF.Header = .init(filterHash: dataHash, previousHeaderHash: previousHeaderHash, headerHash: headerHash)
            let cFilterCommand: CF.CfilterCommand = .init(
                .wire(filterType: .basic,
                      blockHash: try! blockHeader.blockHash(),
                      filterHash: dataHash,
                      n: 0,
                      compressed: [])
            )
            
            let serialHeader: CF.SerialHeader = .init(recordFlag: .hasFilterHeader,
                                                      id: try! blockHeader.height(),
                                                      filterHash: cfHeader.filterHash,
                                                      headerHash: cfHeader.headerHash)

            return (cfHeader, serialHeader, cFilterCommand)
        }
        
        public static func makeEmptyFilters(previousHeaderHash: BlockChain.Hash<CompactFilterHeaderHash>,
                                            blockHeaders: ArraySlice<BlockHeader>) -> [(CF.SerialHeader, CF.CfilterCommand)] {
            var previous = previousHeaderHash
        
            return blockHeaders.map {
                let (wire, serial, cf) = makeEmptyFilterAndHeader(previousHeaderHash: previous, blockHeader: $0)
                previous = wire.headerHash
                return (serial, cf)
            }
        }
        
        
        public static func wireToSerial(_ wire: (offset: Int, element: CF.Header)) -> CF.SerialHeader {
            CF.SerialHeader(recordFlag: [.hasFilterHeader],
                            id: wire.offset,
                            filterHash: wire.element.filterHash,
                            headerHash: wire.element.headerHash)
        }

        public static func wireToSerial(processed count: Int = 0) -> (Int, CF.Header) -> CF.SerialHeader {
            { offset, element in
                CF.SerialHeader(recordFlag: offset < count ? [.hasFilterHeader, .nonMatchingFilter, .processed, ] : [.hasFilterHeader, ],
                                id: offset,
                                filterHash: element.filterHash,
                                headerHash: element.headerHash)
            }
        }
        
        public static func printStats(offset: Int = 0, eventLoop: EventLoop) {
            let threadPool = NIOThreadPool(numberOfThreads: 2)
            threadPool.start()
            
            let bhRepo = Self.getFileBlockHeaderRepo(offset: offset,
                                                     threadPool: threadPool,
                                                     eventLoop: eventLoop)
            let cfHeaderRepo = Self.getFileCFHeaderRepo(offset: offset,
                                                        threadPool: threadPool,
                                                        eventLoop: eventLoop)
            
            print("---- REPO STATS ----")
            XCTAssertNoThrow(print("** bhRepo\t\t\tcount: \(try bhRepo.count().wait())\t\theights: \(try bhRepo.heights().wait())"))
            XCTAssertNoThrow(print("** cfHeaderRepo\t\tprocessed: \(try cfHeaderRepo.processedRecords(processed: 0).wait())\t\tcfHeaderTip: \(try cfHeaderRepo.cfHeadersTip(processed: 0).wait())"))
            print("--------------------")
            
            XCTAssertNoThrow(try bhRepo.close().wait())
            XCTAssertNoThrow(try cfHeaderRepo.close().wait())
        }
        
        public static func createBlockHeaderFile() {
            let fileName = Settings.BITCOIN_BLOCKHEADER_FILEPATH
            XCTAssertTrue(
                FileManager.default
                .createFile(atPath: fileName, contents: nil)
            )
        }
        
        public static func deleteBlockHeaderFile() throws {
            let fileName = Settings.BITCOIN_BLOCKHEADER_FILEPATH
            try FileManager.default
            .removeItem(atPath: fileName)
        }
        
        public static func resetBlockHeaderFile() {
            try? Self.deleteBlockHeaderFile()
            Self.createBlockHeaderFile()
        }
        
        public func writeTestingBlockHeadersFile(repo: FileBlockHeaderRepo, count: Int) {
            var testData: [BlockHeader]!
            XCTAssertNoThrow(
                testData = try BlockHeaderMock.TestChain.createInvalid(length: count).map {
                    var copy = $0
                    try copy.serial()
                    return copy
                }
            )
            
            testData.forEach {
                try! repo.write($0).wait()
            }
        }

        public static func withDupFuture<T>(fileHandle: NIOFileHandle,
                                            _ body: (NIOFileHandle) throws -> T) throws -> T {
            let copy = try fileHandle.duplicate()
            return try body(copy)
        }
        
        public static func withFileBlockHeaderRepo<T>(fileHandle: NIOFileHandle,
                                                      nonBlockingFileIO: NonBlockingFileIOClient,
                                                      eventLoop: EventLoop,
                                                      offset: Int = 0,
                                                      _ body: (FileBlockHeaderRepo) throws -> T) throws -> T {
            try withDupFuture(fileHandle: fileHandle) { fileHandle in
                let repo = FileBlockHeaderRepo(eventLoop: eventLoop,
                                                  nonBlockingFileIO: nonBlockingFileIO,
                                                  nioFileHandle: fileHandle,
                                                  offset: offset)
                
                do {
                    let result = try body(repo)
                    try repo.close().wait()
                    return result
                } catch {
                    logger.error(#function, "- Error \(error.localizedDescription)")
                    try? repo.close().wait()
                    throw error
                }
            }
        }
        
        public static func withFileCFHeaderRepo<T>(fileHandle: NIOFileHandle,
                                                   nonBlockingFileIO: NonBlockingFileIOClient,
                                                   eventLoop: EventLoop,
                                                   offset: Int = 0,
                                                   _ body: (FileCFHeaderRepo) throws -> T) throws -> T {
            try Self.withDupFuture(fileHandle: fileHandle) { fileHandle in
                let repo = FileCFHeaderRepo(eventLoop: eventLoop,
                                            nonBlockingFileIO: nonBlockingFileIO,
                                            nioFileHandle: fileHandle,
                                            offset: offset)
                
                do {
                    let result = try body(repo)
                    try repo.close().wait()
                    return result
                } catch {
                    logger.error(#function, "- Error \(error.localizedDescription)")
                    try? repo.close().wait()
                    throw error
                }
            }
        }
        
        public static func writeFlags(_ flags: File.RecordFlag,
                                      fileHandle: NIOFileHandle,
                                      nonBlockingFileIO: NonBlockingFileIOClient,
                                      start: Int = 0,
                                      repeating: Int,
                                      recordSize: Int,
                                      eventLoop: EventLoop) -> EventLoopFuture<Void> {
            var buffer = ByteBufferAllocator().buffer(capacity: 2)
            buffer.writeInteger(flags.rawValue)

            let futures: [EventLoopFuture<Void>] = (0..<repeating).map {
                let copy = buffer
                
                return nonBlockingFileIO.write(fileHandle: fileHandle,
                                               toOffset: Int64((recordSize * (start + $0)) + 80),
                                               buffer: copy,
                                               eventLoop: eventLoop)
            }
            
            return Future.andAllSucceed(futures, on: eventLoop)
        }
    }


    
    public class Server: XCTestCase {
        public static func setupServer(_ userDefaults: UserDefaults? = nil) -> TestServer {
            let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
            let testServer = TestServer(group: group)
            
            let peerAddress = PeerAddress(time: UInt32(0),
                                          services: [ .nodeNetwork, .nodeWitness ],
                                          address: .string("::ffff:7f00:1"),
                                          port: PeerAddress.IPPort(testServer.serverPort))

            XCTAssertNoThrow(
                try userDefaults?.set(JSONEncoder().encode([ peerAddress ]), forKey: "remoteNodes")
            )
            
            return testServer
        }
    }
    
    public final class Node: XCTestCase {
        public static func createNetworkState(offset: Int = 0,
                                              muxer: Muxer,
                                              cfMatcher: CF.MatcherClient,
                                              eventLoop: EventLoop,
                                              threadPool: NIOThreadPool,
                                              nioFileHandle: NIOFileHandle) -> fltrNode.Node.NetworkState {
            let nonBlockingFileIODependency: (NIOThreadPool) -> NonBlockingFileIOClient = Settings.NONBLOCKING_FILE_IO
            let nonBlockingFileIO = nonBlockingFileIODependency(threadPool)
            
            let bhRepo = FileBlockHeaderRepo(eventLoop: eventLoop,
                                             nonBlockingFileIO: nonBlockingFileIO,
                                             nioFileHandle: try! nioFileHandle.duplicate(),
                                             offset: offset)
            let cfHeaderRepo = FileCFHeaderRepo(eventLoop: eventLoop,
                                                nonBlockingFileIO: nonBlockingFileIO,
                                                nioFileHandle: try! nioFileHandle.duplicate(),
                                                offset: offset)
            return .init(threadPool: threadPool,
                         nonBlockingFileIO: nonBlockingFileIO,
                         bhRepo: bhRepo,
                         cfHeaderRepo: cfHeaderRepo,
                         openFile: nioFileHandle,
                         muxer: muxer,
                         cfMatcher: cfMatcher)
        }
        
        public static func closeNetworkState(_ networkState: fltrNode.Node.NetworkState, eventLoop: EmbeddedEventLoopLocked) {
            let bhClose = networkState.bhRepo.close()
            let cfClose = networkState.cfHeaderRepo.close()
            eventLoop.run()
            XCTAssertNoThrow(try bhClose.wait())
            XCTAssertNoThrow(try cfClose.wait())
        }
    }
}

extension BlockCommand {
    public init(previousBlockHash: BlockChain.Hash<BlockHeaderHash> = .zero,
                merkleRoot: BlockChain.Hash<MerkleHash> = .zero) {
        let header = BlockHeader(
            .wire(version: 1,
                  previousBlockHash: previousBlockHash,
                  merkleRoot: merkleRoot,
                  timestamp: 0,
                  difficulty: 0,
                  nonce: 0,
                  transactions: 0)
        )

        let transaction = Tx.Transaction(
            .wireLegacy(version: 1, vin: [], vout: [], locktime: .disable(0))
        )
        self.init(header: header, transactions: [transaction], hasWitness: false)
    }
 
    public static func make(height: Int = 0,
                            prevBlockHash: BlockChain.Hash<BlockHeaderHash> = .zero,
                            outpoint: Tx.Outpoint = .init(transactionId: .makeHash(from: [ 1, 2, 3 ]), index: 0),
                            opCodes: [UInt8] = []) -> (BlockHeader,
                                                       CF.SerialHeader,
                                                       BlockCommand,
                                                       Tx.AnyIdentifiableTransaction) {
        let txx = Tx.makeSegwitTx(for: outpoint, scriptPubKey: opCodes)
        var buffer = ByteBufferAllocator().buffer(capacity: 1000)
        txx.write(to: &buffer)
        let transaction = Tx.Transaction(fromBuffer: &buffer)!
        var tCopy = transaction
        try! tCopy.hash()
        let merkleRoot = try! BlockChain.merkleRoot(from: [tCopy.getTxId()])
        
        let blockHeader = BlockHeader(.wire(version: 1,
                                            previousBlockHash: prevBlockHash,
                                            merkleRoot: merkleRoot,
                                            timestamp: 1,
                                            difficulty: 1,
                                            nonce: 1,
                                            transactions: 1))
        var copy = blockHeader
        try! copy.addBlockHash()
        try! copy.addHeight(height)
        var fullBlock: BlockCommand = .init(header: blockHeader,
                                            transactions: [ transaction ],
                                            hasWitness: false)
        
        try! copy.serial()
        try! fullBlock.verify(header: copy)
        
        let cfHeader = CF.SerialHeader(recordFlag: [.hasFilterHeader, .matchingFilter],
                                       id: height,
                                       filterHash: .zero,
                                       headerHash: .zero)
        
        return (copy, cfHeader, fullBlock, Tx.AnyIdentifiableTransaction(txx))
    }
}

extension VersionCommand {
    public init(testing: Bool? = nil) {
        self = VersionCommand(versionNumber: 1,
                              services: [ .nodeNetwork, .nodeWitness, ], 
                              timestamp: Date(),
                              recipientAddress: .init(services: [ .nodeNetwork ],
                                                      address: .v6(.zero),
                                                      port: 0),
                              senderAddress: .init(services: [ .nodeNetwork ],
                                                   address: .v6(.zero),
                                                   port: 0),
                              nonce: .random(in: 0 ... .max),
                              userAgent: "test",
                              startHeight: 1,
                              relay: false)
    }
}

extension Tx.Transaction {
    public init(testing: Bool? = nil) {
        self = .init(.wireWitness(
            version: 2,
            vin: [], vout: [], locktime: .disable(0)
            )
        )
    }
}

fileprivate let __allocator = ByteBufferAllocator()
public extension Array where Element == UInt8 {
    var buffer: ByteBuffer {
        var b = __allocator.buffer(capacity: self.count)
        b.writeBytes(self)
        return b
    }
}

extension Array: ExpressibleByUnicodeScalarLiteral where Element == UInt8 {
    public init(unicodeScalarLiteral value: UnicodeScalar) {
        self = String(value).hex2Bytes
    }
}

extension Array: ExpressibleByExtendedGraphemeClusterLiteral where Element == UInt8 {
    public init(extendedGraphemeClusterLiteral value: Character) {
        self = String(value).hex2Bytes
    }
}

extension Array: ExpressibleByStringLiteral where Element == UInt8 {
    public init(stringLiteral value: String) {
        self = value.hex2Bytes
    }
}

public extension PublicKeyHash {
    init(_ scalar: Scalar) {
        let point = DSA.PublicKey(Point(scalar))
        self.init(point)
    }
}

public func withTemporaryFile<T>(content: String? = nil, _ body: (NIO.NIOFileHandle, String) throws -> T) rethrows -> T {
    let (fd, path) = openTemporaryFile()
    let fileHandle = NIOFileHandle(descriptor: fd)
    defer {
        try? fileHandle.close()
        XCTAssertEqual(0, unlink(path))
    }
    if let content = content {
        Array(content.utf8).withUnsafeBufferPointer { ptr in
            let toWrite = ptr.count
            let start = ptr.baseAddress!
            let res = Darwin.write(fd, start, toWrite)
            XCTAssertNotEqual(res, -1)
            XCTAssertEqual(0, lseek(fd, 0, SEEK_SET))
        }
    }
    return try body(fileHandle, path)
}

var temporaryDirectory: String {
    get {
        #if targetEnvironment(simulator)
        // Simulator temp directories are so long (and contain the user name) that they're not usable
        // for UNIX Domain Socket paths (which are limited to 103 bytes).
        return "/tmp"
        #else
        #if os(Android)
        return "/data/local/tmp"
        #elseif os(Linux)
        return "/tmp"
        #else
        if #available(macOS 10.12, iOS 10, tvOS 10, watchOS 3, *) {
            return FileManager.default.temporaryDirectory.path
        } else {
            return "/tmp"
        }
        #endif // os
        #endif // targetEnvironment
    }
}

func createTemporaryDirectory() -> String {
    let template = "\(temporaryDirectory)/.NIOTests-temp-dir_XXXXXX"
    
    var templateBytes = template.utf8 + [0]
    let templateBytesCount = templateBytes.count
    templateBytes.withUnsafeMutableBufferPointer { ptr in
        ptr.baseAddress!.withMemoryRebound(to: Int8.self, capacity: templateBytesCount) { (ptr: UnsafeMutablePointer<Int8>) in
            let ret = mkdtemp(ptr)
            XCTAssertNotNil(ret)
        }
    }
    templateBytes.removeLast()
    return String(decoding: templateBytes, as: Unicode.UTF8.self)
}

func openTemporaryFile() -> (CInt, String) {
    let template = "\(temporaryDirectory)/nio_XXXXXX"
    var templateBytes = template.utf8 + [0]
    let templateBytesCount = templateBytes.count
    let fd = templateBytes.withUnsafeMutableBufferPointer { ptr in
        ptr.baseAddress!.withMemoryRebound(to: Int8.self, capacity: templateBytesCount) { (ptr: UnsafeMutablePointer<Int8>) in
            return mkstemp(ptr)
        }
    }
    templateBytes.removeLast()
    return (fd, String(decoding: templateBytes, as: Unicode.UTF8.self))
}

public func versionPreamble(_ server: TestServer) throws {
    let version = try server.channelIn(timeout: 2)
    guard case .incoming(let isVersion) = version, case .version = isVersion else {
        XCTFail()
        return
    }
    
    let verack = try server.channelIn(timeout: 2)
    guard case .pending(let isVerack) = verack, isVerack.description == "verack" else {
        XCTFail()
        return
    }
}
