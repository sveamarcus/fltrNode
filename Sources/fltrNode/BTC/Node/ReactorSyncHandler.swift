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
extension Node.Reactor {
    struct SyncHandler {
        private let syncNotification: (SyncEvent) -> Void
        private var state: Optional<SyncEvent>
        
        init(syncNotification: @escaping (SyncEvent) -> Void) {
            self.state = nil
            self.syncNotification = syncNotification
        }
        
        mutating func sync() {
            switch self.state {
            case .tracking, nil:
                self.state = .synched
                self.syncNotification(.synched)
            case .synched:
                break
            }
        }
        
        mutating func tracking() {
            switch self.state {
            case .synched, nil:
                self.state = .tracking
                self.syncNotification(.tracking)
            case .tracking:
                break
            }
        }
    }
}
