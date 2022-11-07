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
import class Foundation.UserDefaults
import class Foundation.JSONDecoder
import class Foundation.JSONEncoder
import HaByLo
import UserDefaultsClientLive

struct NodeProperties {
    init(offset: UserDefaultsProperty<Int>,
         verifyDownloaderCheckpoint: UserDefaultsProperty<Int>,
         processedIndex: UserDefaultsProperty<Int>,
         remoteNodes: UserDefaultsProperty<Set<PeerAddress>>,
         walletCreation: UserDefaultsProperty<WalletCreation>) {
        self._offset = offset
        self._verifyDownloaderCheckpoint = verifyDownloaderCheckpoint
        self._processedIndex = processedIndex
        self._remoteNodes = remoteNodes
        self._walletCreation = walletCreation
    }
    
    private let _offset: UserDefaultsProperty<Int>
    private let _verifyDownloaderCheckpoint: UserDefaultsProperty<Int>
    private let _processedIndex: UserDefaultsProperty<Int>
    private let _remoteNodes: UserDefaultsProperty<Set<PeerAddress>>
    private let _walletCreation: UserDefaultsProperty<WalletCreation>
    
    var offset: Int {
        get {
            self._offset.get()!
        }
        nonmutating set {
            self._offset.put(newValue)
        }
    }
    
    var peers: Set<PeerAddress> {
        get {
            self._remoteNodes.get()!
        }
        nonmutating set {
            self._remoteNodes.put(newValue)
        }
    }
    
    var verifyCheckpoint: Int {
        get {
            self._verifyDownloaderCheckpoint.get()!
        }
        nonmutating set {
            self._verifyDownloaderCheckpoint.put(newValue)
        }
    }
    
    var processedIndex: Int {
        get {
            return self._processedIndex.get()!
        }
        nonmutating set {
            self._processedIndex.put(newValue)
        }
    }
    
    var walletCreation: WalletCreation {
        get {
            self._walletCreation.get()!
        }
        nonmutating set {
            self._walletCreation.put(newValue)
        }
    }
    
    func reset() {
        self._offset.delete()
        self._remoteNodes.delete()
        self._processedIndex.delete()
        self._verifyDownloaderCheckpoint.delete()
        self._walletCreation.delete()
    }
}

extension NodeProperties {
    enum WalletCreation: Codable, CustomDebugStringConvertible {
        case set(BlockChain.DateOrHeight)
        case recovered
        
        func skip(_ bh: BlockHeader) -> Bool {
            switch self {
            case .set(let date):
                return date.after(bh)
            case .recovered:
                return false
            }
        }
        
        var debugDescription: String {
            var result: [ String ] = [ "WalletCreation("]
            switch self {
            case .set(let dateOrHeight):
                switch dateOrHeight {
                case .date:
                    result.append("date: \(dateOrHeight.date!))")
                case .height(let height):
                    result.append("height: \(height))")
                }
            case .recovered:
                result.append(".recovered)")
            }
            
            return result.joined()
        }
    }
}

extension NodeProperties {
    static func live(_ userDefaults: UserDefaults) -> Self {
        let decoder = JSONDecoder()
        let encoder = JSONEncoder()
        
        return self.init(offset: .live(key: "offset",
                                       defaults: userDefaults,
                                       encoder: encoder,
                                       decoder: decoder),
                         verifyDownloaderCheckpoint: .live(key: "verifyDownloader",
                                                           defaults: userDefaults,
                                                           encoder: encoder,
                                                           decoder: decoder),
                         processedIndex: .live(key: "processedIndex",
                                               defaults: userDefaults,
                                               encoder: encoder,
                                               decoder: decoder),
                         remoteNodes: .live(key: "remoteNodes",
                                            defaults: userDefaults,
                                            encoder: encoder,
                                            decoder: decoder),
                         walletCreation: .live(key: "walletCreation",
                                               defaults: userDefaults,
                                               encoder: encoder,
                                               decoder: decoder))
    }
}
