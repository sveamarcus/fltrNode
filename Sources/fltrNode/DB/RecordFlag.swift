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
import NIO

public extension File {
    struct RecordFlag: OptionSet {
        public let rawValue: UInt8
        
        public init(rawValue: UInt8) {
            self.rawValue = rawValue
        }
    }
}

public extension File.RecordFlag {
    static let hasFilterHeader = File.RecordFlag(rawValue: 1)
    static let nonMatchingFilter = File.RecordFlag(rawValue: 1 << 1)
    static let matchingFilter = File.RecordFlag(rawValue: 1 << 2)
    static let processed = File.RecordFlag(rawValue: 1 << 7)
}

extension File.RecordFlag: CustomStringConvertible {
    public var description: String {
        var flags: [String] = []
        if self.contains(.hasFilterHeader) {
            flags.append(".hasFilterHeader")
        }
        if self.contains(.nonMatchingFilter) {
            flags.append(".nonMatchingFilter")
        }
        if self.contains(.matchingFilter) {
            flags.append(".matchingFilter")
        }
        if self.contains(.processed) {
            flags.append(".processed")
        }
        if self.rawValue & ~(0b1000_0111) > 0 {
            flags.append("unknown flags (rawValue: \(self.rawValue))")
        }
        
        return "File.RecordFlag[" + flags.joined(separator: ", ") + "]"
    }
}
