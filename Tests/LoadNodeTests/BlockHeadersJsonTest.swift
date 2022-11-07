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
import LoadNode
import XCTest

final class BlockHeadersJsonTest: XCTestCase {
    var blockHeadersJson: BlockHeadersJson!
    
    override func setUp() {
        self.blockHeadersJson = .init(network: .test,
                                      offset: 1,
                                      blockHeaders: [ testHeader(),
                                      testHeader(),
                                      testHeader() ],
                                      filter0: testCfHeader(),
                                      filter1: testCfHeader())
    }
    
    override func tearDown() {
        self.blockHeadersJson = nil
    }
    
    func testEncodeDecode() {
        let jsonEncoder = JSONEncoder()
        jsonEncoder.outputFormatting = [ .prettyPrinted, .sortedKeys, .withoutEscapingSlashes, ]
        guard let encoded = try? jsonEncoder.encode(self.blockHeadersJson)
        else {
            XCTFail()
            return
        }

        let string = String(decoding: encoded, as: UTF8.self)
        let testVector: String =
"""
{
  "blockHeaders" : [
    "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+P0BBQkNERUZHSElKS0xNTk8=",
    "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+P0BBQkNERUZHSElKS0xNTk8=",
    "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+P0BBQkNERUZHSElKS0xNTk8="
  ],
  "filterHash0" : "3ddfa1ba2705e191459eb1d88729ab67ae68c5ee5e7922886144a30e93f2f935",
  "filterHash1" : "3ddfa1ba2705e191459eb1d88729ab67ae68c5ee5e7922886144a30e93f2f935",
  "filterHeaderHash0" : "fd5a30df66d0f06514b05f8eea9b1513e3a22d4991f39faaf5578e1ebe38d17c",
  "filterHeaderHash1" : "fd5a30df66d0f06514b05f8eea9b1513e3a22d4991f39faaf5578e1ebe38d17c",
  "network" : "test",
  "offset" : 1
}
"""
        XCTAssertEqual(string, testVector)
        
        let jsonDecoder = JSONDecoder()
        XCTAssertNoThrow(
            XCTAssertEqual(try jsonDecoder.decode(BlockHeadersJson.self, from: encoded),
                           self.blockHeadersJson)
        )
    }
}


func testHeader() -> [UInt8] {
    (0..<80).map(UInt8.init)
}

func testCfHeader() -> Load.CFHeader {
    let hashFilter: BlockChain.Hash<CompactFilterHash> = .makeHash(from: [ 0, 1, 2, ])
    let hashHeader: BlockChain.Hash<CompactFilterHeaderHash> = .makeHash(from: [ 3, 4, 5, ])
    
    return Load.CFHeader(filter: hashFilter, header: hashHeader)
}
