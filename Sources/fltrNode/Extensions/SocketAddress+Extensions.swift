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
import Foundation
import NIO
import Network

extension SocketAddress {
    var ipAddress: Network.IPv6Address {
        var ipAddress: Network.IPv6Address? = .zero
        switch (self) {
        case .v4(let address):
            var int = in_addr_t(address.address.sin_addr.s_addr)
            var bytes = withUnsafePointer(to: &int) {
                $0.withMemoryRebound(to: UInt8.self, capacity: MemoryLayout<in_addr_t>.size) {
                    [UInt8](UnsafeBufferPointer(start: $0, count: MemoryLayout<in_addr_t>.size))
                }
            }
            bytes.insert(0xFF, at: 0)
            bytes.insert(0xFF, at: 0)
            for _ in 0..<10 {
                bytes.insert(0, at: 0)
            }

            ipAddress = Network.IPv6Address(Data(bytes))
        case .v6(let address):
            var addr = address.address.sin6_addr
            let typeCast: [UInt8] = withUnsafeBytes(of: &addr) {
                [UInt8]($0)
            }
            ipAddress = Network.IPv6Address(Data(typeCast))
        default:
            break
        }
        guard let ip = ipAddress else {
            return .zero
        }
        return ip
    }
    
    var ipPort: PeerAddress.IPPort {
        let port: in_port_t
        switch (self) {
        case .v4(let address):
            port = in_port_t(bigEndian: address.address.sin_port)
        case .v6(let address):
            port = in_port_t(bigEndian: address.address.sin6_port)
        default:
            port = in_port_t(0)
        }
        return port

    }
}
