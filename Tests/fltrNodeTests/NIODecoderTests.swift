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
import Foundation
import NodeTestLibrary
import NIO
import NIOTestUtils
import XCTest

extension ByteToMessageDecoderVerifier.VerificationError: LocalizedError, CustomStringConvertible {
    public var errorDescription: String? {
        self.description
    }

    public var description: String {
        func outputInputErrorString(_ output: OutputType) -> String {
            "output (\(String(describing: output)) for inputs (\(String(describing: self.inputs))"
        }

        switch self.errorCode {
        case .wrongProduction(let actual, let expected): return "wrongProduction error: expected [\(String(describing: expected))] provides actual [\(String(describing: actual))] for inputs \(self.inputs)"
        case .overProduction(let output): return "overProduction error \(outputInputErrorString(output))"
        case .underProduction(let output): return "underProduction error \(outputInputErrorString(output))"
        case .leftOversOnDeconstructingChannel(inbound: let anyInbound, outbound: let anyOutbound, pendingOutbound: let pendingOutbound): return "leftOversOnDeconstructingChannel inbound: \(anyInbound) outbound: \(anyOutbound) pendingOutbound: \(pendingOutbound) for inputs: \(String(describing: self.inputs))"
        }
    }
}

final class NIODecoderTests: XCTestCase {
    var savedSettings: NodeSettings!
    let testnetGetblocks = "0b110907676574626c6f636b00000000450000002a0af9950100000001000000000000592589e55cda6e8a093998e8356ea770d4aaeb7c0f5439b147d7000000000000017a09017d52db538d7a9ddcc48311866d7e5fdbbbec7d0faad5".hex2Bytes
    let testnetGetheaders = "0b110907676574686561646572730000450000002a0af9950100000001000000000000592589e55cda6e8a093998e8356ea770d4aaeb7c0f5439b147d7000000000000017a09017d52db538d7a9ddcc48311866d7e5fdbbbec7d0faad5".hex2Bytes

    override func setUp() {
        self.savedSettings = Settings
        TestingHelpers.diSetup {
            $0.BITCOIN_NETWORK = .testnet
        }
    }

    override func tearDown() {
        Settings = self.savedSettings
        self.savedSettings = nil
    }

    func testDecode() {
        let bufferGB = self.testnetGetblocks.buffer
        let bufferGH = self.testnetGetheaders.buffer
        let pairs: [(ByteBuffer, [ByteBuffer])] = [ (bufferGB, [bufferGB]), (bufferGH, [bufferGH]) ]
        XCTAssertNoThrow(
            try ByteToMessageDecoderVerifier.verifyDecoder(inputOutputPairs: pairs, decoderFactory: FrameDecoder.init)
        )
    }
}
