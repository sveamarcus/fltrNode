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
import struct Foundation.Data
import Stream64
import HaByLo

public extension GolombCodedSetClient {
    static let optionalFail: Self = .init(decode: { _, _, _ in nil },
                                          encode: { _, _ in nil })
    
    static let emptyHash: BlockChain.Hash<CompactFilterHash> = {
        let data: [UInt8] = [ 0x00 ]
        return .makeHash(from: data)
    }()
}
