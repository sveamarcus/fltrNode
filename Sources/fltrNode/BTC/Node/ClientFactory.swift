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
import fltrWAPI
import HaByLo
import NameResolverPosix
import Network
import NIO
import NIOConcurrencyHelpers
import NIOTransportServices

struct ClientFactory {
    @usableFromInline
    var _next: (ClientUnsolicitedDelegate, Int) -> Future<Client>
    
    @usableFromInline
    var _height: () -> Int
    
    @usableFromInline
    func callAsFunction(delegate: ClientUnsolicitedDelegate) -> Future<Client> {
        let height = self._height()
        return self._next(delegate, height)
    }
    
    @usableFromInline
    init(next: @escaping (ClientUnsolicitedDelegate, Int) -> Future<Client>,
         height: @escaping () -> Int) {
        self._next = next
        self._height = height
    }
}

extension ClientFactory {
    static func dns(eventLoopGroup: NIOTSEventLoopGroup,
                    threadPool: NIOThreadPool,
                    fetch _height: @escaping () -> Int) -> Self {
        let host = "fltr.app"
        let nameResolver = NameResolver.posix(threadPool: threadPool)
        
        return .init(
            next: ({ delegate, height in
                let eventLoop = eventLoopGroup.next()
                
                return nameResolver.lookup(address: host,
                                           eventLoop: eventLoop)
                .map { addresses in
                    // addresses non empty per definition (else Future in error state)
                    let ipAddress = addresses.randomElement()!.asIPv6
                    
                    return Client.connect(eventLoop: eventLoop,
                                          host: ipAddress,
                                          port: Settings.BITCOIN_NETWORK == NetworkParameters.main
                                            ? 8333
                                            : 18333,
                                          height: height,
                                          delegate: delegate)
                }
            }),
            height: _height
        )
    }
    
    static func combine(eventLoopGroup: NIOTSEventLoopGroup,
                        threadPool: NIOThreadPool,
                        fetch _height: @escaping () -> Int,
                        factories: [() -> ClientFactory]) -> Self {
        return .init(
            next: { delegate, _ in
                let random = factories.randomElement()!
                let factory = random()

                return factory(delegate: delegate)
            },
            height: _height)
    }
    
    static func dnsWithFallback(eventLoopGroup: NIOTSEventLoopGroup,
                                threadPool: NIOThreadPool,
                                fetch _height: @escaping () -> Int) -> Self {
        let dns = Self.dns(eventLoopGroup: eventLoopGroup,
                           threadPool: threadPool,
                           fetch: _height)
        let shuffled = Self.shuffled(eventLoopGroup: eventLoopGroup,
                                     fetch: _height)
        
        return ClientFactory { delegate, _ in
            dns(delegate: delegate)
            .flatMapError {
                logger.error("ClientFactory \(#function) - DNS service failed with error \($0) "
                    + "falling back to UserDefaults")
                return shuffled(delegate: delegate)
            }
        }
        height: { _height() }
    }

    private static func _userDefaults<T>(elg: NIOTSEventLoopGroup,
                                         peers: @escaping () -> T,
                                         fetch: @escaping () -> Int) -> Self
    where T: Collection, T.Element == PeerAddress {
        let lock: NIOConcurrencyHelpers.NIOLock = .init()
        
        var i: T.Iterator? = lock.withLock { nil }
        return .init(
            next: { delegate, height in
                let host: PeerAddress = {
                    if let next = lock.withLock({ i?.next() }) {
                        return next
                    } else {
                        let next: PeerAddress = lock.withLock {
                            i = peers().makeIterator()
                            return i!.next()!
                        }
                        return next
                    }
                }()
                let eventLoop = elg.next()
                
                return eventLoop.makeSucceededFuture(Client.connect(eventLoop: eventLoop,
                                                                    host: host.address,
                                                                    port: host.port,
                                                                    height: height,
                                                                    delegate: delegate))
            },
            height: fetch
        )
    }

    
    static func iterated(eventLoopGroup: NIOTSEventLoopGroup,
                        fetch _height: @escaping () -> Int) -> Self {
        Self._userDefaults(elg: eventLoopGroup,
                           peers: { Settings.NODE_PROPERTIES.peers },
                           fetch: _height)
    }
    
    static func shuffled(eventLoopGroup: NIOTSEventLoopGroup,
                         fetch _height: @escaping () -> Int) -> Self {
        Self._userDefaults(elg: eventLoopGroup,
                           peers: { Settings.NODE_PROPERTIES.peers.shuffled() },
                           fetch: _height)
    }
}

