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
@testable import LoadNode
import fltrNode
import NIO
import XCTest

final class LoadNodeTests: XCTestCase {
    var allocator: ByteBufferAllocator!
    
    override func setUp() {
        self.allocator = ByteBufferAllocator()
    }
    
    override func tearDown() {
        self.allocator = nil
    }
    
    func testLoadHeaders() {
        let years = Load.years()
        XCTAssertGreaterThanOrEqual(years.count, 1)
        guard let headers = years.first?.load(network: .test,
                                              allocator: ByteBufferAllocator(),
                                              decoder: JSONDecoder())
        else { XCTFail(); return }
        XCTAssertGreaterThanOrEqual(headers.blockHeaders.count, 100)

        headers.blockHeaders.forEach {
            var copy = $0
            XCTAssertNotNil(BlockHeader(fromBuffer: &copy))
        }

        XCTAssertEqual(headers.filterHash0.littleEndian.count, 32)
        XCTAssertEqual(headers.filterHeaderHash0.littleEndian.count, 32)
        XCTAssertEqual(headers.filterHash1.littleEndian.count, 32)
        XCTAssertEqual(headers.filterHeaderHash1.littleEndian.count, 32)
        XCTAssertGreaterThanOrEqual(headers.offset, 0)
    }
}

func testData() -> Data {
    let url = Bundle.module.url(forResource: "test", withExtension: "json")!
    return try! Data(contentsOf: url)
}
