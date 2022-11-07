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
import NIO
import XCTest

final class LatencyDecoratorTests: XCTestCase {
    var channel: EmbeddedChannel!
    var eventLoop: EmbeddedEventLoop!
    
    override func setUp() {
        self.channel = EmbeddedChannel()
        let handler = LatencyDecoratorHandler()
        XCTAssertNoThrow(try self.channel.pipeline.addHandler(handler).wait())
        let el = self.channel.eventLoop as! EmbeddedEventLoop
        self.eventLoop = el
    }
    
    override func tearDown() {
        XCTAssertNoThrow(try self.channel.finish(acceptAlreadyClosed: true))
        self.eventLoop = nil
        self.channel = nil
    }
    
    func testPingLatencyTimer() throws {
        let ping = PingCommand()
        let pendingPromise = self.channel.eventLoop.makePromise(of: BitcoinCommandValue.self)
        let pendingCommand = PendingBitcoinCommand(promise: pendingPromise, bitcoinCommandValue: ping)
        
        let outboundIn: QueueHandler.OutboundIn = .pending(pendingCommand)
        try channel.writeOutbound(outboundIn)

        let _: QueueHandler.OutboundIn = try channel.readOutbound()!
        RunLoop.current.run(until: Date(timeIntervalSinceNow: 0.1))
        let pong = PongCommand(nonce: ping.nonce)
        pendingPromise.succeed(pong)
        XCTAssertGreaterThan(pong.latencyTimer, 0.1)
    }
}
