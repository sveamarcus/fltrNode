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
import fltrTx
import fltrWAPI

public typealias Outpoints = Set<Tx.Outpoint>

public enum CompactFilterEvent: Hashable {
    case download(Int)
    case matching(Int)
    case nonMatching(Int)
    case failure(Int)
    case blockDownload(Int)
    case blockMatching(Int)
    case blockNonMatching(Int)
    case blockFailed(Int)
}

public enum TransactionEvent: Hashable {
    case spent(SpentOutpoint)
    case new(FundingOutpoint)
}

public enum SyncEvent: UInt8, Hashable {
    case tracking
    case synched
}

public protocol NodeDelegate {
    func newTip(height: Int, commit: @escaping () -> Void)
    func rollback(height: Int, commit: @escaping () -> Void)
    func transactions(height: Int,
                      events: [TransactionEvent],
                      commit: @escaping (TransactionEventCommitOutcome) -> Void)
    func unconfirmedTx(height: Int, events: [TransactionEvent])
    func estimatedHeight(_: ConsensusChainHeight.CurrentHeight)
    func filterEvent(_: CompactFilterEvent)
    func syncEvent(_: SyncEvent)
}
