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
import UserDefaultsClientTest

extension NodeProperties {
    static let test: NodeProperties = {
        let offsetUserDefaults = UserDefaultsProperty<Int>.inMemory()
        let verifyDownloaderCheckpoint = UserDefaultsProperty<Int>.inMemory()
        let processedIndex = UserDefaultsProperty<Int>.inMemory()
        let remoteNodesUserDefaults = UserDefaultsProperty<Set<PeerAddress>>.inMemory()
        let walletCreation = UserDefaultsProperty<WalletCreation>.inMemory()

        return NodeProperties(offset: offsetUserDefaults,
                              verifyDownloaderCheckpoint: verifyDownloaderCheckpoint,
                              processedIndex: processedIndex,
                              remoteNodes: remoteNodesUserDefaults,
                              walletCreation: walletCreation)
    }()
}
