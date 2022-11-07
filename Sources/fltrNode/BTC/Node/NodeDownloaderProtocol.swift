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
import NIO

protocol NodeDownloaderProtocol {
    func cancel()
    func start() -> EventLoopFuture<NodeDownloaderOutcome>
    var almostComplete: Bool { get }
}

enum NodeDownloaderOutcome: Equatable {
    case drop
    case processed
    case quit
    case rollback(Int)
    case synch(BlockHeader, Client.Key)
    indirect case first(NodeDownloaderOutcome, followedBy: NodeDownloaderOutcome)
}

extension NodeDownloaderOutcome {
    var isTerminal: Bool {
        switch self {
        case .drop, .rollback:
            return true
        case .quit, .first, .synch, .processed:
            return false
        }
    }
}

extension Sequence where Element == NodeDownloaderOutcome {
    var synch: (BlockHeader, Client.Key)? {
        self.first {
            switch $0 {
            case .synch:
                return true
            case .drop, .processed,
                    .quit, .rollback, .first:
                return false
                
            }
        }
        .map {
            guard case .synch(let bh, let key) = $0
            else { preconditionFailure() }
            
            return (bh, key)
        }
    }
    
    var drop: Bool {
        self.first {
            switch $0 {
            case .drop:
                return true
            case .processed, .quit, .rollback,
                    .synch, .first:
                return false
            }
        }
        .map { _ in
            true
        }
        ?? false
    }
    
    var processed: Bool {
        self.allSatisfy {
            switch $0 {
            case .processed:
                return true
            case .drop, .quit, .rollback,
                    .synch, .first:
                return false
            }
        }
    }
    
    var quit: Bool {
        self.first {
            switch $0 {
            case .quit:
                return true
            case .processed, .drop, .rollback,
                    .synch, .first:
                return false
            }
        }
        .map { _ in
            true
        }
        ?? false
    }
    
    var rollback: Int? {
        self.compactMap {
            switch $0 {
            case .rollback(let height), .first(.rollback(let height), _):
                return height
            case .drop, .processed, .quit, .synch, .first:
                return nil
            }
        }
        .reduce(Int?.none) {
            Swift.min($0 ?? .max, $1)
        }
    }
}
