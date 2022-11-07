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

final class TestClientServerTests: XCTestCase {
    var savedSettings: NodeSettings!
    
    override func setUp() {
        self.savedSettings = Settings
        TestingHelpers.diSetup()
    }
    
    override func tearDown() {
        Settings = self.savedSettings
        self.savedSettings = nil
    }
    
    func testTestServer() {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let testServer = TestServer(group: group)
        let helpers = TestServerHelpers()
        let pendingPromise = group.next().makePromise(of: BitcoinCommandValue.self)
        let pendingCommand = PendingBitcoinCommand(promise: pendingPromise, bitcoinCommandValue: PingCommand())
        XCTAssertNoThrow(
            try helpers.sendCommand(.pending(pendingCommand), port: testServer.serverPort)
        )
        var bitcoinCommand: FrameToCommandHandler.InboundOut! = nil
        XCTAssertNoThrow(bitcoinCommand = try testServer.channelIn(timeout: 2))
        XCTAssertNotNil(bitcoinCommand)

        if case .incoming(.ping(let pingCommand)) = bitcoinCommand {
            XCTAssertEqual(pingCommand.description, "ping")
        XCTAssertNoThrow(try testServer.stop())
        } else {
            XCTFail()
        }
    }
}
