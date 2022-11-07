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
import bech32
import FileRepo
import fltrWAPI
import Foundation
import NIO
import class NIOTransportServices.NIOTSEventLoopGroup
import Stream64

#if DEBUG
public var Settings: NodeSettings = .mainProdMultiConnection
#else
public let Settings: NodeSettings = .mainProdMultiConnection
#endif

public struct NodeSettings {
    var BITCOIN_BIP32_MASTER_SEED_PHRASE: String
    var BITCOIN_BLOCKHEADER_FILEPATH: String
    var BITCOIN_BLOCKHEADER_MAX_DOWNLOAD_COUNT: Int
    var BITCOIN_CF_BUNDLE_SIZE: Int
    var BITCOIN_CF_CONCURRENT_INVOCATIONS: Int
    var BITCOIN_CF_DOWNLOAD_FILTER_COUNT: Int
    var BITCOIN_CF_GOLOMB_CODED_SET_CLIENT: () -> GolombCodedSetClient
    var BITCOIN_CF_HEADERS_MAX_STOPS: Int
    var BITCOIN_CF_TIMEOUT: TimeAmount
    var BITCOIN_COMMAND_FULLBLOCK_TIMEOUT: TimeAmount
    var BITCOIN_COMMAND_THROTTLE_TIMEOUT: TimeAmount
    var BITCOIN_COMMAND_TIMEOUT: TimeAmount
    var BITCOIN_FULL_BLOCK_DOWNLOADER_BATCH_SIZE: Int
    var BITCOIN_HEADERS_MIN_NUMBER: Int
    var BITCOIN_HEADERS_RESTRUCTURE_FACTOR: Int
    var BITCOIN_NETWORK: NetworkParameters
    var BITCOIN_MAX_HEADERS_OVERWRITE: Int
    var BITCOIN_NUMBER_OF_CLIENTS: Int
    var BITCOIN_UNSOLICITED_FILTER_CACHE: Int
    var BITCOIN_VERIFY_MIN_TRANSACTIONS: Int
    var BITCOIN_WALLET_DELEGATE_COMMIT_TIMEOUT: TimeAmount
    var BYTEBUFFER_ALLOCATOR: ByteBufferAllocator
    var CF_MATCHER_FACTORY: () -> CF.MatcherClient
    var CLIENT_FACTORY: (NIOTSEventLoopGroup, NIOThreadPool, @escaping () -> Int) -> ClientFactory
    var EVENT_LOOP_GROUPS: Int
    var EVENT_LOOP_DEFAULT_QOS: DispatchQoS
    var MUXER_CLIENT_SELECTION: PeerAddressSelection
    var MUXER_RECONNECT_TIMER: DispatchTimeInterval
    var NODE_PROPERTIES: NodeProperties
    var NODE_REPEATING_TASK_DELAY: TimeAmount
    var NODE_ROLLBACK_DELAY: TimeAmount
    var NODE_SLEEP_DELAY: TimeAmount
    var NODE_VERIFY_RANDOM_STEP_RANGE: Range<Int>
    var NONBLOCKING_FILE_IO: (NIOThreadPool) -> NonBlockingFileIOClient
    var NUMBER_THREAD_POOL_THREADS: Int
    var VERSION_VERSION: Int32
    var PROTOCOL_COIN: Int
    var PROTOCOL_LEGACY_SIZE_LIMIT: Int
    var PROTOCOL_MAX_GETBLOCKS: Int
    var PROTOCOL_MAX_GETHEADERS: Int
    var PROTOCOL_MAX_MONEY: Int
    var PROTOCOL_USER_AGENT: String
    var PROTOCOL_VERSION: Int32
    var PROTOCOL_SERVICE_OPTIONS: ServicesOptionSet
    var PROTOCOL_RELAY: Bool
    var PROTOCOL_SEGWIT_COINBASE_PREFIX: [UInt8]
    var PROTOCOL_SEGWIT_SCALE_FACTOR: Int
    var PROTOCOL_SEGWIT_WEIGHT_LIMIT: Int
    
    public var network: NetworkParameters { self.BITCOIN_NETWORK }
    public var numberOfThreads: Int { self.NUMBER_THREAD_POOL_THREADS }
}


public extension NodeSettings {
    static var mainProdMultiConnection: NodeSettings = .init(
        BITCOIN_BIP32_MASTER_SEED_PHRASE: "Bitcoin seed",
        BITCOIN_BLOCKHEADER_FILEPATH: {
            let url = FileManager.default.urls(for: .documentDirectory,
                                               in: .userDomainMask)
                .first!
                .path
            return url + "/headers.db"
        }(),
        BITCOIN_BLOCKHEADER_MAX_DOWNLOAD_COUNT: 100,
        BITCOIN_CF_BUNDLE_SIZE: 15,
        BITCOIN_CF_CONCURRENT_INVOCATIONS: 8,
        BITCOIN_CF_DOWNLOAD_FILTER_COUNT: 10_000,
        BITCOIN_CF_GOLOMB_CODED_SET_CLIENT: { GolombCodedSetClient.stream64 },
        BITCOIN_CF_HEADERS_MAX_STOPS: 6,
        BITCOIN_CF_TIMEOUT: .seconds(11),
        BITCOIN_COMMAND_FULLBLOCK_TIMEOUT: .seconds(60),
        BITCOIN_COMMAND_THROTTLE_TIMEOUT: .seconds(4),
        BITCOIN_COMMAND_TIMEOUT: .seconds(6),
        BITCOIN_FULL_BLOCK_DOWNLOADER_BATCH_SIZE: 7,
        BITCOIN_HEADERS_MIN_NUMBER: 100,
        BITCOIN_HEADERS_RESTRUCTURE_FACTOR: 10,
        BITCOIN_NETWORK: .main,
        BITCOIN_MAX_HEADERS_OVERWRITE: 60,
        BITCOIN_NUMBER_OF_CLIENTS: 8,
        BITCOIN_UNSOLICITED_FILTER_CACHE: 10000,
        BITCOIN_VERIFY_MIN_TRANSACTIONS: 500,
        BITCOIN_WALLET_DELEGATE_COMMIT_TIMEOUT: .seconds(20),
        BYTEBUFFER_ALLOCATOR: ByteBufferAllocator(),
        CF_MATCHER_FACTORY: CF.MatcherClient.reference,
        CLIENT_FACTORY: { .dnsWithFallback(eventLoopGroup: $0, threadPool: $1, fetch: $2) },
        EVENT_LOOP_GROUPS: max(1, Self.coreCount - 1),
        EVENT_LOOP_DEFAULT_QOS: .userInitiated,
        MUXER_CLIENT_SELECTION: .live,
        MUXER_RECONNECT_TIMER: .milliseconds(300),
        NODE_PROPERTIES: .live(UserDefaults.standard),
        NODE_REPEATING_TASK_DELAY: .nanoseconds(1),
        NODE_ROLLBACK_DELAY: .seconds(5),
        NODE_SLEEP_DELAY: .milliseconds(300),
        NODE_VERIFY_RANDOM_STEP_RANGE: 1..<6001,
        NONBLOCKING_FILE_IO: NonBlockingFileIOClient.live(_:),
        NUMBER_THREAD_POOL_THREADS: max(2, Self.coreCount - 2),
        VERSION_VERSION: 70015,
        PROTOCOL_COIN: 100_000_000,
        PROTOCOL_LEGACY_SIZE_LIMIT: 1_000_000,
        PROTOCOL_MAX_GETBLOCKS: 500,
        PROTOCOL_MAX_GETHEADERS: 2000,
        PROTOCOL_MAX_MONEY: 21_000_000,
        PROTOCOL_USER_AGENT: "/70015 client/",
        PROTOCOL_VERSION: 2,
        PROTOCOL_SERVICE_OPTIONS: [ .nodeWitness, .nodeNetworkLimited ],
        PROTOCOL_RELAY: true,
        PROTOCOL_SEGWIT_COINBASE_PREFIX: [0x6a, 0x24, 0xaa, 0x21, 0xa9, 0xed],
        PROTOCOL_SEGWIT_SCALE_FACTOR: 4,
        PROTOCOL_SEGWIT_WEIGHT_LIMIT: 4_000_000)
}

private extension NodeSettings {
    static var coreCount: Int {
        var ncpu = UInt(0)
        var size = MemoryLayout<UInt>.size
        
        sysctlbyname("hw.ncpu", &ncpu, &size, nil, 0)
        precondition(ncpu > 0)
        
        return Int(ncpu)
    }
}

@propertyWrapper
struct Setting<Value> {
    private var value: Value
    init(_ keyPath: KeyPath<NodeSettings, Value>) {
        self.value = Settings[keyPath: keyPath]
    }
    var wrappedValue: Value {
        get {
            self.value
        }
        set {
            self.value = newValue
        }
    }
}

