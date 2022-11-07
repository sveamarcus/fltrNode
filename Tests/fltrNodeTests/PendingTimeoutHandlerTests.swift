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
import NodeTestLibrary
import NIO
import XCTest

final class PendingTimeoutHandlerTests: XCTestCase {
    var eventLoop: EmbeddedEventLoop!
    var channel: EmbeddedChannel!
    var savedSettings: NodeSettings!
    
    override func setUp() {
        self.savedSettings = Settings
        TestingHelpers.diSetup {
            $0.BITCOIN_COMMAND_TIMEOUT = .hours(1)
        }

        self.channel = EmbeddedChannel()
        XCTAssertNoThrow(try self.channel.pipeline.addHandler(PendingTimeoutHandler()).wait())
        XCTAssertNoThrow(try self.channel.pipeline.addHandler(InboundTestHandler()).wait())
        self.eventLoop = (self.channel.eventLoop as! EmbeddedEventLoop)
    }
    
    override func tearDown() {
        XCTAssertNoThrow(try self.channel.finish(acceptAlreadyClosed: true))
        XCTAssertNoThrow(try self.eventLoop.syncShutdownGracefully())
        
        self.eventLoop = nil
        self.channel = nil
        
        Settings = self.savedSettings
        self.savedSettings = nil
    }
    
    final class InboundTestHandler: ChannelInboundHandler {
        typealias InboundIn = ByteBuffer
        var isCalled = false
        
        func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
            switch event {
            case is PendingTimeoutHandler.PendingTimeout:
                self.isCalled = true
            default:
                XCTFail()
            }
        }
    }
    
    func testDefaultTimeout() {
        let pendingPromise: Promise<BitcoinCommandValue> = self.eventLoop.makePromise()
        let pending = PendingBitcoinCommand(promise: pendingPromise, bitcoinCommandValue: PingCommand(nonce: 1))
        let outbound = QueueHandler.OutboundIn.pending(pending)
        let _ = self.channel.write(NIOAny(outbound))
        let handler = try? self.channel.pipeline.handler(type: InboundTestHandler.self).wait()

        XCTAssertEqual(handler?.isCalled, false)
        self.channel.flush()
        self.eventLoop.advanceTime(by: .hours(1))
        XCTAssertEqual(handler?.isCalled, true)
    }
    
    func testOverrideTimeout() {
        let pendingPromise: Promise<BitcoinCommandValue> = self.eventLoop.makePromise()
        let pending = PendingBitcoinCommand(promise: pendingPromise, bitcoinCommandValue: PingCommand(nonce: 1, timeout: .milliseconds(100)))
        let outbound = QueueHandler.OutboundIn.pending(pending)
        let _ = self.channel.write(NIOAny(outbound))
        let handler = try? self.channel.pipeline.handler(type: InboundTestHandler.self).wait()

        XCTAssertEqual(handler?.isCalled, false)
        self.channel.flush()
        self.eventLoop.advanceTime(by: .milliseconds(100))
        XCTAssertEqual(handler?.isCalled, true)
    }
}
