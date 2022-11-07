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

public extension SingleSelection where Item == PeerAddress {
    static var iterator: Self = {
        var iterator: AnyIterator<PeerAddress> = AnyIterator({ nil })
        
        return Self.init(select: {
            if let next = iterator.next() {
                return next
            } else {
                iterator = $0.makeIterator()
                return iterator.next()
            }
        })
    }()
}
