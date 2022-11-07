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
import struct Foundation.Date
import HaByLo
import NIO
import NIOFoundationCompat

public struct BlockHeaderMock {}

extension BlockHeaderMock {
    public static let rawRealHeaders: [ByteBuffer] = [
        .init(.init(
            "0100000000000000000000000000000000000000000000000000000000000000000000003ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4a29ab5f49ffff001d1dac2b7c00"
            .hex2Bytes)),
        .init(.init(
            "010000006fe28c0ab6f1b372c1a6a246ae63f74f931e8365e15a089c68d6190000000000982051fd1e4ba744bbbe680e1fee14677ba1a3c3540bf7b1cdb606e857233e0e61bc6649ffff001d01e3629900"
                .hex2Bytes)),
        .init(.init(
            "010000004860eb18bf1b1620e37e9490fc8a427514416fd75159ab86688e9a8300000000d5fdcc541e25de1c7a5addedf24858b8bb665c9f36ef744ee42c316022c90f9bb0bc6649ffff001d08d2bd6100"
                .hex2Bytes)),
        .init(.init(
            "01000000bddd99ccfda39da1b108ce1a5d70038d0a967bacb68b6b63065f626a0000000044f672226090d85db9a9f2fbfe5f0f9609b387af7be5b7fbb7a1767c831c9e995dbe6649ffff001d05e0ed6d00"
                .hex2Bytes)),
    ]
    
    public static var parsedRealHeaders: [BlockHeader] = {
        rawRealHeaders.map { raw in
            parseRawHeader(raw)
        }
    }();

    public static var hashedRealHeaders: [BlockHeader] = {
        rawRealHeaders.map { raw in
            var h = parseRawHeader(raw)
            try! h.addBlockHash()
            return h
        }
    }();
    
    public static func parseRawHeader(_ header: ByteBuffer) -> BlockHeader {
        var copy = header
        return BlockHeader(fromBuffer: &copy)!
    }
        
    public struct TestChain {
        static var randomMerkleRoot: BlockChain.Hash<MerkleHash> {
            .little(
                (1...32).map { _ in
                    .random(in: 0 ... .max)
                }
            )
        }
        
        public static func createInvalid(length: Int, offset: Int = 0) throws -> [BlockHeader] {
            func initializeHeader(height: Int, previous: BlockChain.Hash<BlockHeaderHash>) throws -> BlockHeader {
                var b = BlockHeader.init(
                    .wire(version: 2,
                          previousBlockHash: previous,
                          merkleRoot: TestChain.randomMerkleRoot,
                          timestamp: UInt32(Date(timeIntervalSince1970: Double(3600 * 24 * (height + 1))).timeIntervalSince1970),
                          difficulty: 1,
                          nonce: 0,
                          transactions: 0)
                )
                try b.addHeight(height)
                try b.addBlockHash()
                return b
            }
            let first = try initializeHeader(height: offset, previous: .zero)
            let result: [BlockHeader] = [ first ]
            return try (1..<length).reduce(into: result) { acc, next in
                let previousBlockHash = try acc.last!.blockHash()
                let new = try initializeHeader(height: .init(next + offset), previous: previousBlockHash)
                acc.append(new)
            }
        }
    }
}
