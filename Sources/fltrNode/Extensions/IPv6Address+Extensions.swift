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
import Network

extension IPv6Address {
    var asString: String {
        let ip = rawValue.reduce(("", false)) { i, c in
            var builder = String()
            builder.append(i.0)
            builder.append(String(format: "%02hhx", c))
            builder.append(i.1 ? ":" : "")
            return (builder, !i.1)
        }.0.dropLast()
        return String(ip)
    }

    static var zero: IPv6Address {
        return IPv6Address("0000:0000:0000:0000:0000:0000:0000:0000")!
    }
        
    init?(fromIpV4 address: String) {
        if let ipv4 = IPv4Address(address) {
            self.init(fromIpV4: ipv4)
        } else {
            return nil
        }
    }
    
    init(fromIpV4 address: IPv4Address) {
        let bytes = address.rawValue

        let v6Bytes = [ [0x00, 0x00, 0x00, 0x00],
                        [0x00, 0x00, 0x00, 0x00],
                        [0x00, 0x00, 0xff, 0xff],
                        Array(bytes) ].joined()
        self.init(Data(v6Bytes))!
    }
}
