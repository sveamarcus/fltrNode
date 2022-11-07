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
struct ServicesOptionSet: OptionSet, Codable, Hashable {
    let rawValue: UInt64
    static let nodeNetwork = ServicesOptionSet(rawValue: 1)
    static let nodeGetUtxo = ServicesOptionSet(rawValue: 1 << 1)
    static let nodeBloom = ServicesOptionSet(rawValue: 1 << 2)
    static let nodeWitness = ServicesOptionSet(rawValue: 1 << 3)
    static let nodeFilters = ServicesOptionSet(rawValue: 1 << 6)
    static let nodeNetworkLimited = ServicesOptionSet(rawValue: 1 << 10)
    
    var isFullNode: Bool {
        self.contains([.nodeNetwork, .nodeWitness])
    }
    
    var isFilterNode: Bool {
        self.contains([.nodeFilters, .nodeWitness])
    }
}

extension ServicesOptionSet: CustomDebugStringConvertible {
    var debugDescription: String {
        var out: [String] = Array()
        var copy = self
        
        if copy.contains(.nodeNetwork) {
            copy.subtract(.nodeNetwork)
            out.append("NW✅")
        } else {
            out.append("NW❎")
        }
        if copy.contains(.nodeGetUtxo) {
            copy.subtract(.nodeGetUtxo)
            out.append("UTXO✅")
        } else {
            out.append("UTXO❎")
        }
        if copy.contains(.nodeBloom) {
            copy.subtract(.nodeBloom)
            out.append("BLOOM✅")
        } else {
            out.append("BLOOM❎")
        }
        if copy.contains(.nodeWitness) {
            copy.subtract(.nodeWitness)
            out.append("SEGWIT✅")
        } else {
            out.append("SEGWIT❎")
        }
        if copy.contains(.nodeFilters) {
            copy.subtract(.nodeFilters)
            out.append("FILTER✅")
        } else {
            out.append("FILTER❎")
        }
        if copy.contains(.nodeNetworkLimited){
            copy.subtract(.nodeNetworkLimited)
            out.append("NWLIM✅")
        } else {
            out.append("NWLIM❎")
        }
        if copy.rawValue > 0 {
            out.append("❌REMAINING (rawValue:\(copy.rawValue))")
        }
        return out.joined(separator: " ⎸")
    }
}

// MARK: BitcoinError
public enum BitcoinNetworkError: Error {
    case alreadyConnecting
    case cannotDecodeIncomingCommand
    case castingFailed
    case channelClosed
    case channelUnexpectedlyClosed
    case clientQueueAllDisconnected
    case decoderError
    case decoderErrorIllegalNetworkReceived
    case disconnected
    case emptyCommandReceived
    case encoderError
    case expectedChannelNotNil
    case expectedDisconnectedState
    case expectedVoidStateResult
    case expectedNonceResult
    case expectedArrayofPeerAddressResult
    case expectedPendingState
    case expectedSelfNotNil
    case illegalAddrCommand
    case illegalBlockHeaderCommand
    case illegalCommandReceived
    case illegalHeadersCommand
    case illegalInvCommand
    case illegalStateDuringConnectionSetup
    case illegalStateExpectedVersionCommand
    case illegalStateInventoryCommandUnexpectedTypeBlocks
    case illegalStateInventoryCommandUnexpectedTypeTxs
    case illegalStateInventoryNotFoundReceived
    case illegalStateInventoryRemainingNegative
    case illegalStateInventoryTupleSet
    case illegalStatePendingPromisesNil
    case illegalStatePendingPromisesNotNil
    case illegalTransactionEmptySequence
    case illegalTransactionFormatForCommitmentHash
    case illegalVersion
    case illegalVersionCommand
    case illegalPeerAddress
    case illegalPingCommand
    case inventoryNotFound
    case pendingCommandTimeout
    case pendingCommandStallingTimeout
}
