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
public struct SingleSelection<Item> {
    @usableFromInline
    let select0: (AnyCollection<Item>) -> Item?
    
    @inlinable
    public func callAsFunction<C: Collection>(_ items: C) -> Item?
    where C.Element == Item {
        self.select0(AnyCollection(items))
    }
    
    @inlinable
    public func callAsFunction(_ items: AnyCollection<Item>) -> Item? {
        self.select0(items)
    }
    
    @inlinable
    public init(select: @escaping (AnyCollection<Item>) -> Item?) {
        self.select0 = select
    }
}
