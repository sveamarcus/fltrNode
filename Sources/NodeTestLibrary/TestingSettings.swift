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
import FileRepo
import Foundation
import struct Network.IPv6Address
import NIO
import NIOTransportServices
import Stream64

extension NodeSettings {
    public static var testTestnet: NodeSettings = .init(
        BITCOIN_BIP32_MASTER_SEED_PHRASE: "Bitcoin seed",
        BITCOIN_BLOCKHEADER_FILEPATH: "/Users/marcus/blockheader.db",
        BITCOIN_BLOCKHEADER_MAX_DOWNLOAD_COUNT: 100,
        BITCOIN_CF_BUNDLE_SIZE: 3,
        BITCOIN_CF_CONCURRENT_INVOCATIONS: 200,
        BITCOIN_CF_DOWNLOAD_FILTER_COUNT: 2000,
        BITCOIN_CF_GOLOMB_CODED_SET_CLIENT: { .stream64 },
        BITCOIN_CF_HEADERS_MAX_STOPS: 100,
        BITCOIN_CF_TIMEOUT: .seconds(100),
        BITCOIN_COMMAND_FULLBLOCK_TIMEOUT: .seconds(60),
        BITCOIN_COMMAND_THROTTLE_TIMEOUT: .milliseconds(25_000),
        BITCOIN_COMMAND_TIMEOUT: .milliseconds(8_000),
        BITCOIN_FULL_BLOCK_DOWNLOADER_BATCH_SIZE: 1,
        BITCOIN_HEADERS_MIN_NUMBER: 600,
        BITCOIN_HEADERS_RESTRUCTURE_FACTOR: 10,
        BITCOIN_NETWORK: .testnet,
        BITCOIN_MAX_HEADERS_OVERWRITE: 10,
        BITCOIN_NUMBER_OF_CLIENTS: 8,
        BITCOIN_UNSOLICITED_FILTER_CACHE: 10,
        BITCOIN_VERIFY_MIN_TRANSACTIONS: 2,
        BITCOIN_WALLET_DELEGATE_COMMIT_TIMEOUT: .seconds(2),
        BYTEBUFFER_ALLOCATOR: ByteBufferAllocator(),
        CF_MATCHER_FACTORY: CF.MatcherClient.reference,
        CLIENT_FACTORY: { elg, _, fetch in .iterated(eventLoopGroup: elg, fetch: fetch) },
        EVENT_LOOP_GROUPS: 2,
        EVENT_LOOP_DEFAULT_QOS: .userInitiated,
        MUXER_CLIENT_SELECTION: .iterator,
        MUXER_RECONNECT_TIMER: .milliseconds(300),
        NODE_PROPERTIES: NodeProperties.test,
        NODE_REPEATING_TASK_DELAY: .milliseconds(50),
        NODE_ROLLBACK_DELAY: .milliseconds(300),
        NODE_SLEEP_DELAY: .milliseconds(300),
        NODE_VERIFY_RANDOM_STEP_RANGE: 1..<2,
        NONBLOCKING_FILE_IO: {
            NonBlockingFileIOClient.live($0)
        },
        NUMBER_THREAD_POOL_THREADS: 2,
        VERSION_VERSION: 70015,
        PROTOCOL_COIN: 100_000_000,
        PROTOCOL_LEGACY_SIZE_LIMIT: 1_000_000,
        PROTOCOL_MAX_GETBLOCKS: 500,
        PROTOCOL_MAX_GETHEADERS: 2_000,
        PROTOCOL_MAX_MONEY: 21_000_000,
        PROTOCOL_USER_AGENT: "/70015 client/",
        PROTOCOL_VERSION: 2,
        PROTOCOL_SERVICE_OPTIONS: [],
        PROTOCOL_RELAY: true,
        PROTOCOL_SEGWIT_COINBASE_PREFIX: [0x6a, 0x24, 0xaa, 0x21, 0xa9, 0xed],
        PROTOCOL_SEGWIT_SCALE_FACTOR: 4,
        PROTOCOL_SEGWIT_WEIGHT_LIMIT: 4_000_000
    )
}

// MARK: Integration testing
/*
 testnet-seed.bitcoin.jonasschnelli.ch. 2778 IN A 47.107.50.83
 testnet-seed.bitcoin.jonasschnelli.ch. 2778 IN A 66.206.13.51
 testnet-seed.bitcoin.jonasschnelli.ch. 2778 IN A 18.218.226.83
 testnet-seed.bitcoin.jonasschnelli.ch. 2778 IN A 3.114.7.254
 testnet-seed.bitcoin.jonasschnelli.ch. 2778 IN A 3.221.56.249
 testnet-seed.bitcoin.jonasschnelli.ch. 2778 IN A 144.202.110.14
 testnet-seed.bitcoin.jonasschnelli.ch. 2778 IN A 45.204.3.103
 testnet-seed.bitcoin.jonasschnelli.ch. 2778 IN A 159.65.68.208
 testnet-seed.bitcoin.jonasschnelli.ch. 2778 IN A 52.49.13.57
 testnet-seed.bitcoin.jonasschnelli.ch. 2778 IN A 18.212.130.132
 testnet-seed.bitcoin.jonasschnelli.ch. 2778 IN A 175.207.13.27
 testnet-seed.bitcoin.jonasschnelli.ch. 2778 IN A 18.219.193.255
 testnet-seed.bitcoin.jonasschnelli.ch. 2778 IN A 141.223.85.141
 testnet-seed.bitcoin.jonasschnelli.ch. 2778 IN A 94.75.250.71
 testnet-seed.bitcoin.jonasschnelli.ch. 2778 IN A 5.9.158.123
 testnet-seed.bitcoin.jonasschnelli.ch. 2778 IN A 165.22.117.244
 testnet-seed.bitcoin.jonasschnelli.ch. 2778 IN A 207.180.243.110
 testnet-seed.bitcoin.jonasschnelli.ch. 2778 IN A 193.70.46.134
 testnet-seed.bitcoin.jonasschnelli.ch. 2778 IN A 13.58.129.155
 testnet-seed.bitcoin.jonasschnelli.ch. 2778 IN A 2.238.108.41
 testnet-seed.bitcoin.jonasschnelli.ch. 2778 IN A 54.89.114.195
 testnet-seed.bitcoin.jonasschnelli.ch. 2778 IN A 95.216.36.239
 testnet-seed.bitcoin.jonasschnelli.ch. 2778 IN A 39.108.222.178

 */
//public let BITCOIN_INTEGRATION_TEST_HOST = IPv6Address(fromIpV4: "69.59.18.23")!
// btcd
//public let BITCOIN_INTEGRATION_TEST_HOST = IPv6Address(fromIpV4: "52.141.21.174")!
// fast satoshi 18.1
//public let BITCOIN_INTEGRATION_TEST_HOST = IPv6Address(fromIpV4: "119.3.162.199")!
// fast satoshi 19.99
//public let BITCOIN_INTEGRATION_TEST_HOST = IPv6Address(fromIpV4: "95.216.36.239")!
//public let BITCOIN_INTEGRATION_TEST_HOST = IPv6Address(fromIpV4: "5.9.158.123")!
//public let BITCOIN_INTEGRATION_TEST_PORT: UInt16 = 18333
public let BITCOIN_INTEGRATION_TEST_HOST = IPv6Address(fromIpV4: "127.0.0.1")!
public let BITCOIN_INTEGRATION_TEST_PORT: UInt16 = 8333
