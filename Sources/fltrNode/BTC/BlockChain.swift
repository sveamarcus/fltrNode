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
import struct Foundation.Date

// MARK: Blocklocator
extension BlockChain {
    static func strideGenerator(lowerHeight: Int, upperHeight: Int) -> [Int] {
        guard upperHeight > 0 else {
            return [0]
        }
        var acc: [Int] = []
        var h = upperHeight
        var stride: Int = 1
        repeat {
            acc.append(h)
            h -= stride
            stride *= 2
        } while h > lowerHeight
        assert(!acc.isEmpty)
        acc.append(lowerHeight)
        return acc
    }
    
    static func refreshBlockLocator(new headers: [BlockHeader],
                                    previous locator: BlockLocator) -> BlockLocator {
        guard headers.count > 1 else {
            let firstHash = try! headers.first?.blockHash()
            return BlockLocator(
                hashes: (firstHash.map({ [$0] }) ?? []) + locator.hashes,
                stop: .zero
            )
        }
        
        let hashes = BlockChain.strideGenerator(lowerHeight: headers.startIndex,
                                                upperHeight: headers.index(before: headers.endIndex)).map {
            try! headers[$0].blockHash()
        }
        
        let previousLocatorIndices = (1...5).reversed().compactMap {
            locator.hashes.index(locator.hashes.endIndex,
                                 offsetBy: -$0,
                                 limitedBy: locator.hashes.startIndex)
        }

        logger.trace("BlockChain", #function, "- Refreshed block locator to new height [",
                     headers.last?.id, "]")
        
        let hashesToAppend = previousLocatorIndices.compactMap { index -> BlockChain.Hash<BlockHeaderHash>? in
            guard !hashes.contains(locator.hashes[index]) else {
                return nil
            }
            
            return locator.hashes[index]
        }

        return .init(hashes: hashes + hashesToAppend,
                     stop: .zero)
    }
}

// MARK: DateOrHeight
extension BlockChain {
    enum DateOrHeight: Codable {
        case date(UInt32)
        case height(Int)
    }
}

extension BlockChain.DateOrHeight {
    func after(_ bh: BlockHeader) -> Bool {
        switch self {
        case .date(let timeStamp):
            return timeStamp > bh.timestamp
        case .height(let height):
            let fromBh = try! bh.height()
            return height > fromBh
        }
    }
    
    func before(_ bh: BlockHeader) -> Bool {
        switch self {
        case .date(let timeStamp):
            return timeStamp < bh.timestamp
        case .height(let height):
            let fromBh = try! bh.height()
            return height < fromBh
        }
    }
    
    var date: Date? {
        switch self {
        case .date(let timeStamp):
            return Date(timeIntervalSince1970: .init(timeStamp))
        case .height:
            return nil
        }
    }
}
