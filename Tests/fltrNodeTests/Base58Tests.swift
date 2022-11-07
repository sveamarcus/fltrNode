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

final class Base58Tests: XCTestCase {
    func testEmpties() {
        let test1 = "".ascii
        let bytes = test1.base58Encode()
        XCTAssertEqual(bytes, "")
        XCTAssertNoThrow(
            XCTAssertEqual(try bytes.base58Decode(), "")
        )
    }
    
    func testStringsEncodeDecodeSuccess() {
        XCTAssertNoThrow(
            try self.testStrings.forEach { (testString, testBase58) in
                let ascii = testString.ascii
                let encoded = ascii.base58Encode()
                XCTAssertEqual(encoded, testBase58)
                let decoded = try encoded.base58Decode()
                let decodedString = String(bytes: decoded, encoding: .ascii)
                XCTAssertEqual(decodedString, testString)
            }
        )
    }
    
    func testInvalidStringsThrows() {
        for (invalidBase58, _) in self.invalidTestStrings {
            XCTAssertThrowsError(try invalidBase58.base58Decode()) { error in
                switch error {
                case let e as DecodeBase58Error where e == .illegalInput:
                    break
                default:
                    XCTFail()
                }
            }
        }
        
        let validWithSpaces = "  3yxU3u1igY8WkgtjK92fbJQCd4BZiiT1v25f  "
        XCTAssertNoThrow(try validWithSpaces.base58Decode())
        let interleavedSpaces1 = "  3yxU3u1igY8Wk gtjK92fbJQCd4BZiiT1v25f  "
        let interleavedSpaces2 =
        """
        3y
        xU3u1igY8WkgtjK92fbJQCd4BZiiT1v25f
        """
        XCTAssertThrowsError(try interleavedSpaces1.base58Decode()) { error in
            switch error {
            case let e as DecodeBase58Error where e == .interleavedWhitespace:
                break
            default:
                XCTFail()
            }
        }
        XCTAssertThrowsError(try interleavedSpaces2.base58Decode()) { error in
            switch error {
            case let e as DecodeBase58Error where e == .interleavedWhitespace:
                break
            default:
                XCTFail()
            }
        }
    }
    
    func testLargeDecodeEncode() {
        var result: [UInt8]!
        XCTAssertNoThrow(result = try self.largeDecode.base58Decode())
        XCTAssertEqual(result?.base58Encode(), self.largeDecode)
    }
    
    func testBase58CheckEncoding() {
        let inputData: [UInt8] = [
            6, 161, 159, 136, 34, 110, 33, 238, 14, 79, 14, 218, 133, 13, 109, 40, 194, 236, 153, 44, 61, 157, 254
        ]
        let expectedOutput = "tz1Y3qqTg9HdrzZGbEjiCPmwuZ7fWVxpPtRw"
        let actualOutput = inputData.base58CheckEncode()
        XCTAssertEqual(actualOutput, expectedOutput)
    }
    
    func testBase58CheckDecoding() {
        let input = "tz1Y3qqTg9HdrzZGbEjiCPmwuZ7fWVxpPtRw"
        let expectedOutput: [UInt8] = [
            6, 161, 159, 136, 34, 110, 33, 238, 14, 79, 14, 218, 133, 13, 109, 40, 194, 236, 153, 44, 61, 157, 254
        ]
        XCTAssertNoThrow(
            XCTAssertEqual(expectedOutput, Array(try input.base58CheckDecode()))
        )
        
        let index = input.index(input.startIndex, offsetBy: 10)
        let modifiedInput = input.replacingCharacters(in: (index...index), with: "1")
        XCTAssertThrowsError(try modifiedInput.base58CheckDecode()) { error in
            switch error {
            case let e as DecodeBase58Error where e == .checksumMismatch:
                break
            default:
                XCTFail()
            }
        }
    }

    let testStrings = [
        ("", ""),
        (" ", "Z"),
        ("-", "n"),
        ("0", "q"),
        ("1", "r"),
        ("-1", "4SU"),
        ("11", "4k8"),
        ("abc", "ZiCa"),
        ("1234598760", "3mJr7AoUXx2Wqd"),
        ("abcdefghijklmnopqrstuvwxyz", "3yxU3u1igY8WkgtjK92fbJQCd4BZiiT1v25f"),
        ("00000000000000000000000000000000000000000000000000000000000000", "3sN2THZeE9Eh9eYrwkvZqNstbHGvrxSAM7gXUXvyFQP8XvQLUqNCS27icwUeDT7ckHm4FUHM2mTVh1vbLmk7y")
    ]
    
    let invalidTestStrings = [
        ("0", ""),
        ("O", ""),
        ("I", ""),
        ("l", ""),
        ("3mJr0", ""),
        ("O3yxU", ""),
        ("3sNI", ""),
        ("4kl8", ""),
        ("0OIl", ""),
        ("!@#$%^&*()-_=+~`", "")
    ]
    
    let largeDecode = "3GimCffBLAHhXMCeNxX2nST6dBem9pbUi3KVKykW73LmewcFtMk9oh9eNPdNR2eSzNqp7Z3E21vrWUkGHzJ7w2yqDUDJ4LKo1w5D6aafZ4SUoNQyrSVxyVG3pwgoZkKXMZVixRyiPZVUpekrsTvZuUoW7mB6BQgDTXbDuMMSRoNR7yiUTKpgwTD61DLmhNZopNxfFjn4avpYPgzsTB94iWueq1yU3EoruWCUMvp6fc1CEbDrZY3pkx9oUbUaSMC37rruBKSSGHh1ZE3XK3kQXBCFraMmUQf8dagofMEg5aTnDiLAZjLyWJMdnQwW1FqKKztP8KAQS2JX8GCCfc68KB4VGf2CfEGXtaapnsNWFrHuWi7Wo5vqyuHd21zGm1u5rsiR6tKNCsFC4nzf3WUNxJNoZrDSdF9KERqhTWWmmcM4qdKRCtBWKTrs1DJD2oiK6BK9BgwoW2dfQdKuxojFyFvmxqPKDDAEZPPpJ51wHoFzBFMM1tUBBkN15cT2GpNwKzDcjHPKJAQ6FNRgppfQytzqpq76sSeZaWAB8hhULMJCQGU57ZUjvP7xYAQwtACBnYrjdxA91XwXFbq5AsQJwAmLw6euKVWNyv11BuHrejVmnNViWg5kuZBrtgL6NtzRWHtdxngHDMtuyky3brqGXaGQhUyXrkSpeknkkHL6NLThHH5NPnfFMVPwn2xf5UM5R51X2nTBzADSVcpi4cT7i44dT7o3yRKWtKfUzZiuNyTcSSrfH8KVdLap5ZKLmdPuXM65M2Z5wJVh3Uc4iv6iZKk44RKikM7zs1hqC4sBxRwLZjxhKvvMXDjDcYFkzyUkues4y7fjdCnVTxc4vTYUqcbY2k2WMssyj9SDseVc7dVrEvWCLQtYy79mJFoz1Hmsfk5ynE28ipznzQ3yTBugLHA6j6qW3S74eY4pJ6iynFEuXT4RqqkLGFcMh3goqS7CkphUMzP4wuJyGnzqa5tCno4U3dJ2jUL7Povg8voRqYAfiHyXC8zuhn225EdmRcSnu2pAuutQVV9hN3bkjfzAFUhUWKki8SwXtFSjy6NJyrYUiaze4p7ApsjHQBCgg2zAoBaGCwVN8991Jny31B5vPyYHy1oRSE4xTVZ7tTw9FyQ7w9p1NSEF4sziCxZHh5rFWZKAajc5c7KaMNDvHPNV6S62MTFGTyuKPQNbv9yHRGN4eH6SnZGW6snvEVdYCspWZ1U3Nbxo6vCmBK95UyYpcxHgg1CCGdU4s3edju2NDQkMifyPkJdkabzzHVDhJJbChAJc1ACQfNW74VXXwrBZmeZyA2R28MBctDyXuSuffiwueys2LVowLu9wiTHUox7KQjtHK2c9howk9czzx2mpnYzkVYH42CYsWa5514EM4CJEXPJSSbXSgJJ"
}
