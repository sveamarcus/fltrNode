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
@usableFromInline
typealias PeerAddressSelection = SingleSelection<PeerAddress>

extension SingleSelection where Item == PeerAddress {
    @usableFromInline
    static var live: Self {
        .init(select: { $0.randomElement() })
    }
}
