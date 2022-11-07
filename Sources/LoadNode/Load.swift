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
import NIO

public enum Load {}

public extension Load {
    typealias CFHeader = (filter: BlockChain.Hash<CompactFilterHash>,
                          header: BlockChain.Hash<CompactFilterHeaderHash>)

    struct Headers {
        public let offset: Int
        public let blockHeaders: [ByteBuffer]
        public let filterHash0: BlockChain.Hash<CompactFilterHash>
        public let filterHeaderHash0: BlockChain.Hash<CompactFilterHeaderHash>
        public let filterHash1: BlockChain.Hash<CompactFilterHash>
        public let filterHeaderHash1: BlockChain.Hash<CompactFilterHeaderHash>
        
        init(offset: Int, blockHeaders: [ByteBuffer], filterHash0: BlockChain.Hash<CompactFilterHash>, filterHeaderHash0: BlockChain.Hash<CompactFilterHeaderHash>, filterHash1: BlockChain.Hash<CompactFilterHash>, filterHeaderHash1: BlockChain.Hash<CompactFilterHeaderHash>) {
            self.offset = offset
            self.blockHeaders = blockHeaders
            self.filterHash0 = filterHash0
            self.filterHeaderHash0 = filterHeaderHash0
            self.filterHash1 = filterHash1
            self.filterHeaderHash1 = filterHeaderHash1
        }
    }
    
    struct ChainYear: Equatable, Identifiable {
        public let year: Int
        public let url: URL
        public var id: Int { self.year }
        
        public init(year: Int, url: URL) {
            self.year = year
            self.url = url
        }
        
        public func load(network: Network,
                         allocator: ByteBufferAllocator,
                         decoder: JSONDecoder) -> Headers {
            let data = try! Data(contentsOf: self.url)
            let blockHeaders = try! decoder.decode(BlockHeadersJson.self, from: data)
            
            switch (network, blockHeaders.network) {
            case (.main, .main),
                 (.test, .test):
                break
            case (.main, .test),
                 (.test, .main):
                preconditionFailure()
            }
            
            let buffers = blockHeaders.blockHeaders
            .map { header in
                allocator.buffer(bytes: header)
            }
            
            return .init(offset: blockHeaders.offset,
                         blockHeaders: buffers,
                         filterHash0: blockHeaders.filterHash0,
                         filterHeaderHash0: blockHeaders.filterHeaderHash0,
                         filterHash1: blockHeaders.filterHash1,
                         filterHeaderHash1: blockHeaders.filterHeaderHash1)
        }
        
        static let dateFormatter: DateFormatter = {
            let dateFormatter = DateFormatter()
            dateFormatter.dateFormat = "yyyy"
            return dateFormatter
        }()
        
        public func date() -> Date {
            Self.dateFormatter.date(from: String(self.year))!
        }
    }
}

public extension Load {
    static func years(base name: String = "blockheaders") -> [ChainYear] {
        let startYear = 2009
        var result = [ChainYear]()
        
        func url(for year: Int) -> URL? {
            for b in [ Bundle.main, Bundle.module ] {
                let baseName = name + "_" + String(year)
                if let url = b.url(forResource: baseName, withExtension: "json") {
                    return url
                }
            }
            return nil
        }
        
        var year: Int = {
            for y in (startYear...2050) {
                if let _ = url(for: y) {
                    return y
                }
            }
            preconditionFailure()
        }()
        
        
        while let u = url(for: year) {
            defer { year += 1 }
            result.append(ChainYear(year: year, url: u))
        }
        
        precondition(result.count > 0)
        return result
    }
}
