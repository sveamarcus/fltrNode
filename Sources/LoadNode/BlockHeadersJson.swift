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
import Foundation
import HaByLo

public struct BlockHeadersJson: Codable, Hashable {
    let network: Network
    let offset: Int
    let blockHeaders: [Data]
    let filterHash0: BlockChain.Hash<CompactFilterHash>
    let filterHeaderHash0: BlockChain.Hash<CompactFilterHeaderHash>
    let filterHash1: BlockChain.Hash<CompactFilterHash>
    let filterHeaderHash1: BlockChain.Hash<CompactFilterHeaderHash>
    
    internal init(network: Network,
                  offset: Int,
                  blockHeaders: [Data],
                  filterHash0: BlockChain.Hash<CompactFilterHash>,
                  filterHeaderHash0: BlockChain.Hash<CompactFilterHeaderHash>,
                  filterHash1: BlockChain.Hash<CompactFilterHash>,
                  filterHeaderHash1: BlockChain.Hash<CompactFilterHeaderHash>) {
        self.network = network
        self.offset = offset
        self.blockHeaders = blockHeaders
        self.filterHash0 = filterHash0
        self.filterHeaderHash0 = filterHeaderHash0
        self.filterHash1 = filterHash1
        self.filterHeaderHash1 = filterHeaderHash1
    }

    public init(network: Network,
                offset: Int,
                blockHeaders: [[UInt8]],
                filter0: Load.CFHeader,
                filter1: Load.CFHeader) {
        self.init(network: network,
                  offset: offset,
                  blockHeaders: blockHeaders.map({ Data($0) }),
                  filterHash0: filter0.filter,
                  filterHeaderHash0: filter0.header,
                  filterHash1: filter1.filter,
                  filterHeaderHash1: filter1.header)
    }
}
