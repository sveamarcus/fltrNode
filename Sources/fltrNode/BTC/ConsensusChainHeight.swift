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
import var HaByLo.logger

public struct ConsensusChainHeight {
    public enum CurrentHeight: Equatable {
        case guessed(Int)
        case synced(Int, BlockHeader)
        case unknown
        
        public var value: Int {
            switch self {
            case .guessed(let value),
                 .synced(let value, _):
                return value
            case .unknown:
                return 0
            }
        }
    }
    
    private enum InternalHeight {
        case header(BlockHeader, votes: [ObjectIdentifier])
        case number(Int)
    }
    
    private var state: Optional<InternalHeight> = .none
    @Setting(\.BITCOIN_NUMBER_OF_CLIENTS) private var clients: Int
    private let denominator = 3
    private var minimumVotes: Int {
        (self.clients + self.denominator - 1) / self.denominator
    }
    
    private let newValue: (CurrentHeight) -> Void
    
    init(_ newValue: @escaping (CurrentHeight) -> Void) {
        self.newValue = newValue
    }
    
    mutating private func setState(_ newState: InternalHeight?) {
        let oldHeight = self.current
        self.state = newState
        
        let newHeight = self.currentHeight(for: newState)
        guard oldHeight != newHeight else {
            return
        }
        
        self.newValue(newHeight)
    }
    
    private func currentHeight(for internalHeight: InternalHeight?) -> CurrentHeight {
        switch internalHeight {
        case .none:
            return .unknown
        case .some(.header(let header, let votes)) where votes.count < self.minimumVotes:
            return .guessed(try! header.height())
        case .some(.header(let header, _)):
            return .synced(try! header.height(), header)
        case .some(.number(let height)):
            return .guessed(height)
        }
    }
    
    var current: CurrentHeight {
        self.currentHeight(for: self.state)
    }
    
    @discardableResult
    mutating func guessing(new height: Int) -> CurrentHeight {
        switch self.state {
        case .some(.header(let currentHeader, let votes)) where try! height > currentHeader.height()
                && votes.count < self.minimumVotes:
            self.setState(.number(height))
        case .some(.number(let currentHeight)) where height > currentHeight:
            self.setState(.number(height))
        case .none:
            self.setState(.number(height))
        case .some(.number),
             .some(.header):
            break
        }
        
        return self.current
    }
    
    @discardableResult
    mutating func sync(to header: BlockHeader, from client: Client.Key) -> CurrentHeight {
        let newHeight = try! header.height()
        
        switch self.state {
        case .some(.header(let currentHeader, _)) where try! newHeight > currentHeader.height():
            if Settings.BITCOIN_NUMBER_OF_CLIENTS > 1 {
                self.setState(.header(header, votes: [client.value]))
            } else {
                self.setState(.number(newHeight))
            }
        case .some(.header(let currentHeader, _)) where try! newHeight < currentHeader.height():
            break
        case .some(.header(let currentHeader, var votes)) where currentHeader == header:
            guard !votes.contains(client.value) else {
                break
            }
            votes.append(client.value)
            
            self.setState(.header(currentHeader, votes: votes))
        case .some(.header):
            logger.info("Node.Reactor", #function, "- Error header mismatch for same height when establishing sync height, replacing header to latest")
            self.setState(.header(header, votes: [client.value]))
        case .none,
             .some(.number):
            self.setState(.header(header, votes: [client.value]))
        }
        
        return self.current
    }
    
    @discardableResult
    mutating func rollback(_ height: Int) -> CurrentHeight {
        self.state = .number(height)
        
        return self.current
    }
    
    @discardableResult
    mutating func newHeader() -> CurrentHeight {
        switch self.state {
        case .some(.number(let oldHeight)):
            self.setState(.number(oldHeight + 1))
        case .some(.header(let oldHeader, _)):
            let oldHeight = try! oldHeader.height()
            self.setState(.number(oldHeight))
        case .none:
            break
        }
        
        return self.current
    }
    
    mutating func reset() {
        switch self.state {
        case .some(.header(let header, _)):
            self.state = .number(try! header.height())
        case .some(.number), .none:
            break
        }
    }
}

