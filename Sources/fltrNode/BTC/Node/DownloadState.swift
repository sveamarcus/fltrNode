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
import var HaByLo.logger

extension Node {
    struct DownloadState: NetworkStateProperties {
        let threadPool: NIOThreadPool
        let nonBlockingFileIO: NonBlockingFileIOClient
        let bhRepo: FileBlockHeaderRepo
        let cfHeaderRepo: FileCFHeaderRepo
        let openFile: NIOFileHandle
        let muxer: Muxer
        let cfMatcher: CF.MatcherClient
        let backlog: () -> [BlockHeader]
        let almostComplete: () -> Bool
        let canceller: () -> Void
        let outcomes: Future<[NodeDownloaderOutcome]>
        
        init(networkState: NetworkState,
             backlog: @escaping () -> [BlockHeader],
             almostComplete: @escaping () -> Bool,
             canceller: @escaping () -> Void,
             outcomes: EventLoopFuture<[NodeDownloaderOutcome]>) {
            self.threadPool = networkState.threadPool
            self.nonBlockingFileIO = networkState.nonBlockingFileIO
            self.bhRepo = networkState.bhRepo
            self.cfHeaderRepo = networkState.cfHeaderRepo
            self.openFile = networkState.openFile
            self.muxer = networkState.muxer
            self.cfMatcher = networkState.cfMatcher
            self.backlog = backlog
            self.almostComplete = almostComplete
            self.canceller = canceller
            self.outcomes = outcomes
        }
    }
}

import Foundation
extension Node.DownloadState {
    func download(eventLoop: EventLoop) -> Future<[NodeDownloaderOutcome]?> {
        if self.almostComplete() {
            defer { self.canceller() }
            logger.trace("Node.DownloadState - almostComplete 𝐓𝐑𝐔𝐄 awaiting result")
            
            return self.outcomes.always {
                logger.trace("Node.DownloadState - Result available:\n\($0)")
            }
            .map { $0 as [NodeDownloaderOutcome]? }
        } else {
            return eventLoop.makeSucceededFuture(nil)
        }
    }
}
