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
import HaByLo
import XCTest
import NodeTestLibrary

final class BlockHeaderFSMTests: XCTestCase {
    var testVersion: Int32!
    var testPreviousBlockHash: BlockChain.Hash<BlockHeaderHash>!
    var testMerkleRoot: BlockChain.Hash<MerkleHash>!
    var testTimestamp: UInt32!
    var testDifficulty: UInt32!
    var testNonce: UInt32!
    var testHeight: Int!
    var testWire: BlockHeader!
    
    override func setUp() {
        testVersion = 1
        testPreviousBlockHash = .zero
        testMerkleRoot = .zero
        testTimestamp = 4
        testDifficulty = 5
        testNonce = 6
        testHeight = 7
        
        testWire = .init(.wire(version: testVersion, previousBlockHash: testPreviousBlockHash, merkleRoot: testMerkleRoot, timestamp: testTimestamp, difficulty: testDifficulty, nonce: testNonce, transactions: 0))
    }
    
    func testCreateWireView() {
        var wireView: BlockHeaderWireView!
        XCTAssertNoThrow(wireView = try testWire.wireView())
        XCTAssertEqual(wireView.version, testVersion)
        XCTAssertEqual(wireView.previousBlockHash, testPreviousBlockHash)
        XCTAssertEqual(wireView.merkleRoot, testMerkleRoot)
        XCTAssertEqual(wireView.timestamp, testTimestamp)
        XCTAssertEqual(wireView.difficulty, testDifficulty)
        XCTAssertEqual(wireView.nonce, testNonce)
    }
    
    func testWireToUnhashedToSerialTransition() {
        XCTAssertNoThrow(try testWire.addHeight(testHeight))
        XCTAssertThrowsError(try testWire.addHeight(testHeight))
        XCTAssertNoThrow(try testWire.addBlockHash())
        XCTAssertThrowsError(try testWire.addHeight(testHeight))
        XCTAssertThrowsError(try testWire.addBlockHash())
    }
    
    func testEquality() {
        var b = testWire!
        XCTAssertEqual(testWire, b)
        XCTAssertNoThrow(try testWire.addBlockHash())
        XCTAssertNotEqual(testWire, b)
        XCTAssertNoThrow(try b.addBlockHash())
        XCTAssertEqual(testWire, b)
        XCTAssertNoThrow(try b.addHeight(testHeight))
        XCTAssertNotEqual(testWire, b)
        XCTAssertNoThrow(try testWire.addHeight(testHeight))
        XCTAssertEqual(testWire, b)
    
        XCTAssertNoThrow(try testWire.serial())
        
        var testSerial: BlockHeader!
        XCTAssertNoThrow(testSerial = BlockHeader(.serial(version: testVersion, merkleRoot: testMerkleRoot, timestamp: testTimestamp, difficulty: testDifficulty, nonce: testNonce, id: testHeight, hash: try testWire.blockHash())))
        XCTAssertNotNil(testSerial)
        XCTAssertNoThrow(try b.serial())
        XCTAssertEqual(b, testSerial)
        
        guard var testWireB: BlockHeader = testSerial else {
            XCTFail()
            return
        }
        XCTAssertNoThrow(try testWireB.addPreviousBlockHash(self.testPreviousBlockHash))
        XCTAssertNoThrow(try testWireB.wire())
        XCTAssertNotEqual(testWire, testWireB)
        XCTAssertNoThrow(try testWire.addPreviousBlockHash(self.testPreviousBlockHash))
        XCTAssertNoThrow(try testWire.wire())
        XCTAssertEqual(testWire, testWireB)
    }
    
    func testGetHash() {
        XCTAssertThrowsError(try testWire.blockHash())
        XCTAssertNoThrow(try testWire.addBlockHash())
        var testHash: BlockChain.Hash<BlockHeaderHash>!
        XCTAssertNoThrow(testHash = try testWire.blockHash())
        XCTAssertNotNil(testHash)
        XCTAssertNoThrow(try testWire.addHeight(1))
        testHash = nil
        XCTAssertNoThrow(testHash = try testWire.blockHash())
        XCTAssertNotNil(testHash)
        XCTAssertNoThrow(try testWire.serial())
        testHash = nil
        XCTAssertNoThrow(testHash = try testWire.blockHash())
        XCTAssertNotNil(testHash)
        XCTAssertNoThrow(try testWire.addPreviousBlockHash(self.testPreviousBlockHash))
        XCTAssertNoThrow(try testWire.wire())
        XCTAssertThrowsError(try testWire.blockHash())
    }
}
