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
import FileRepo
import fltrTx
import fltrWAPI
import Foundation
import HaByLo
import LoadNode
import Network
import NIO
import NIOTransportServices

public var RemoteTestnetNodes: [String] = [
    "63.141.242.58",
    "44.237.38.26",
    "3.231.207.106",
    "195.201.95.119",
    "88.198.91.246",
    "104.131.26.124",
    "82.119.233.36",
    "52.170.156.19",
    "18.206.205.116",
    "44.241.182.5",
    "44.238.120.81",
    "66.183.0.205",
    "159.203.125.125",
    "162.55.181.95",
]

public var RemoteMainNodes: [String] = [
    "78.88.167.1",
    "94.231.253.18",
    "89.233.207.67",
    "76.14.26.217",
    "47.221.205.227",
    "90.164.252.107",
    "109.28.162.4",
    "83.162.2.12",
    "165.169.116.199",
    "68.2.190.15",
    "66.23.238.220",
    "189.39.6.82",
    "212.252.22.133",
    "86.88.27.123",
    "174.60.234.54",
    "47.63.223.239",
    "95.214.53.160",
    "88.84.222.252",
    "136.25.131.81",
    "99.132.141.86",
    "217.105.86.87",
    "163.158.173.243",
    "90.230.106.40",
    "188.32.79.170",
    "37.200.59.67",
    "195.201.95.119",
]


public struct AppStarter {
    public static func deleteBlockHeaderFile() throws {
        let fileName = Settings.BITCOIN_BLOCKHEADER_FILEPATH
        try FileManager.default
        .removeItem(atPath: fileName)
    }
    
    public static func openFileHandle() throws -> NIOFileHandle {
        try NIOFileHandle(path: Settings.BITCOIN_BLOCKHEADER_FILEPATH,
                           mode: [ .read, .write, ],
                           flags: .allowFileCreation(posixMode: 0o600))
    }

    static func getFileBlockHeaderRepo(offset: Int, threadPool: NIOThreadPool, eventLoop: EventLoop) -> FileBlockHeaderRepo {
        let nonBlockingFileIODependency: (NIOThreadPool) -> NonBlockingFileIOClient = Settings.NONBLOCKING_FILE_IO
        let nonBlockingFileIO = nonBlockingFileIODependency(threadPool)
        let openFileHandle = try! Self.openFileHandle()

        return FileBlockHeaderRepo(eventLoop: eventLoop,
                                   nonBlockingFileIO: nonBlockingFileIO,
                                   nioFileHandle: openFileHandle,
                                   offset: offset)
    }
    
    static func getFileCFHeaderRepo(offset: Int, threadPool: NIOThreadPool, eventLoop: EventLoop) -> FileCFHeaderRepo {
        let nonBlockingFileIODependency: (NIOThreadPool) -> NonBlockingFileIOClient = Settings.NONBLOCKING_FILE_IO
        let nonBlockingFileIO = nonBlockingFileIODependency(threadPool)
        let openFileHandle = try! Self.openFileHandle()

        return FileCFHeaderRepo(
            eventLoop: eventLoop,
            nonBlockingFileIO: nonBlockingFileIO,
            nioFileHandle: openFileHandle,
            offset: offset
        )
    }
    
    static func loaderData(year: Load.ChainYear) -> (block: [BlockHeader],
                                                     filter: [CF.SerialHeader]) {
        let network: LoadNode.Network = {
            switch Settings.BITCOIN_NETWORK {
            case .main: return .main
            case .testnet: return .test
            default: preconditionFailure()
            }
        }()

        let data = year.load(network: network,
                               allocator: Settings.BYTEBUFFER_ALLOCATOR,
                               decoder: JSONDecoder())
        
        let block: [BlockHeader] = data.blockHeaders
        .enumerated()
        .map { (index, header) -> BlockHeader in
            var copy = header
            var serial = BlockHeader(fromBuffer: &copy)!
            try! serial.addBlockHash()
            try! serial.addHeight(data.offset + index)
            try! serial.serial()
            return serial
        }
        
        let filter0: CF.SerialHeader = .init(recordFlag: [ .hasFilterHeader, .nonMatchingFilter, .processed ],
                                             id: data.offset,
                                             filterHash: data.filterHash0,
                                             headerHash: data.filterHeaderHash0)
        let filter1: CF.SerialHeader = .init(recordFlag: [ .hasFilterHeader, .nonMatchingFilter, .processed ],
                                             id: data.offset + 1,
                                             filterHash: data.filterHash1,
                                             headerHash: data.filterHeaderHash1)
        
        return (block, [ filter0, filter1, ])
    }
    
    static func initializeRepos(year: Load.ChainYear) throws -> Int {
        let niots = NIOTransportServices.NIOTSEventLoopGroup(loopCount: 1, defaultQoS: .default)
        try? Self.deleteBlockHeaderFile()

        let headers = Self.loaderData(year: year)
        precondition(headers.block.count > 0)
        precondition(headers.filter.count > 0)
        let offset = headers.block[0].id

        let threadPool = NIOThreadPool(numberOfThreads: 1)
        threadPool.start()
        let bhRepo = Self.getFileBlockHeaderRepo(offset: offset,
                                                 threadPool: threadPool,
                                                 eventLoop: niots.next())
        let cfHeaderRepo = Self.getFileCFHeaderRepo(offset: offset,
                                                    threadPool: threadPool,
                                                    eventLoop: niots.next())
        
        for bh in headers.block {
            try bhRepo.write(bh).wait()
        }
        
        for cfHeader in headers.filter {
            try cfHeaderRepo.write(cfHeader).wait()
        }

        try bhRepo.close().wait()
        try cfHeaderRepo.close().wait()
        try threadPool.syncShutdownGracefully()
        try niots.syncShutdownGracefully()
        
        return offset
    }
    
    static func preloadRemoteNodes() {
        var peers: Set<PeerAddress> = .init()

        func ipV6(from strings: [String]) -> [IPv6Address] {
            strings.map { string in
                IPv6Address(string) ?? IPv6Address(fromIpV4: string)!
            }
        }
        
        func peer(from ip: IPv6Address, port: PeerAddress.IPPort) -> PeerAddress {
            PeerAddress(time: UInt32(Date().timeIntervalSince1970),
                        services: [ .nodeNetwork, .nodeWitness, .nodeFilters ],
                        address: .v6(ip),
                        port: port)
        }

        let staticPeers: [PeerAddress] = {
            if Settings.BITCOIN_NETWORK == NetworkParameters.testnet {
                let ips = ipV6(from: RemoteTestnetNodes)
                return ips.map { peer(from: $0, port: 18333) }
            } else {
                let ips = ipV6(from: RemoteMainNodes)
                return ips.map { peer(from: $0, port: 8333) }
            }
        }()
        peers.formUnion(staticPeers)
        
        Settings.NODE_PROPERTIES.peers = peers
    }
    
    public static func prepare(new: Bool, year: Load.ChainYear) throws {
        let offset = try Self.initializeRepos(year: year)
        
        Settings.NODE_PROPERTIES.reset()
        
        Self.offset(offset)
        Self.processedIndex(0)
        Self.verifyDownloaderCheckpoint(offset + 1)
        Self.creationDate(new ? .set(.date(UInt32(Date().timeIntervalSince1970))) : .recovered)
        Self.preloadRemoteNodes()
    }

    static private func setHost(_ address: IPv6Address) {
        let peerAddress: PeerAddress = {
            var isTestnet: Bool = false
            if Settings.BITCOIN_NETWORK == NetworkParameters.testnet {
                isTestnet = true
            }
            
            return PeerAddress(time: UInt32(Date().timeIntervalSince1970),
                                      services: [ .nodeNetwork, .nodeWitness, .nodeFilters ],
                                      address: .v6(address),
                                      port: isTestnet
                                        ? PeerAddress.IPPort(18333)
                                        : PeerAddress.IPPort(8333))
        }()
        
        var localPeerAddresses: Set<PeerAddress> = .init()
        localPeerAddresses.insert(peerAddress)
        Settings.NODE_PROPERTIES.peers = localPeerAddresses
    }
    
    static func setLocalhost() {
        Self.setHost(IPv6Address(fromIpV4: "127.0.0.1")!)
    }
    
    static func offset(_ value: Int) {
        Settings.NODE_PROPERTIES.offset = value
    }
    
    static func verifyDownloaderCheckpoint(_ value: Int) {
        Settings.NODE_PROPERTIES.verifyCheckpoint = value
    }
    
    static func creationDate(_ creation: NodeProperties.WalletCreation) {
        Settings.NODE_PROPERTIES.walletCreation = creation
    }

    static func processedIndex(_ value: Int) {
        Settings.NODE_PROPERTIES.processedIndex = value
    }
    
    public static func createNode(threadPool: NIOThreadPool,
                                  walletDelegate: NodeDelegate) -> Node {
        let niots = NIOTSEventLoopGroup(loopCount: Settings.EVENT_LOOP_GROUPS, defaultQoS: Settings.EVENT_LOOP_DEFAULT_QOS)
        let el = niots.next()
        
        return Node(eventLoopGroup: niots,
                    nodeEventLoop: el,
                    threadPool: threadPool,
                    delegate: walletDelegate)
    }
}
