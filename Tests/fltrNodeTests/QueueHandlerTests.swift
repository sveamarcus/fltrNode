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

final class QueueHandlerTests: XCTestCase {
    var queueingChannel: EmbeddedChannel!
    var queueingEventLoop: EmbeddedEventLoop!
    var queueingBitcoinHandler: QueueHandler!
    
    override func setUp() {
        self.queueingChannel = .init()
        let qLoop = self.queueingChannel.eventLoop as! EmbeddedEventLoop
        self.queueingEventLoop = qLoop
        self.queueingBitcoinHandler = QueueHandler()
        XCTAssertNoThrow(try self.queueingChannel.pipeline.addHandler(self.queueingBitcoinHandler).wait())
        XCTAssertNoThrow(try self.queueingChannel.connect(to: .init(ipAddress: "1.1.1.1", port: 1)).wait())
    }
    
    override func tearDown() {
        XCTAssertNoThrow(
            try self.queueingChannel.finish(acceptAlreadyClosed: true)
        )
        self.queueingEventLoop = nil
        self.queueingChannel = nil
        self.queueingBitcoinHandler = nil
    }
    
    func generatePingPendingCommand() -> PendingBitcoinCommand {
        let ping = PingCommand()
        let pingPromise = self.queueingEventLoop.makePromise(of: BitcoinCommandValue.self)
        let pendingCommand = PendingBitcoinCommand(promise: pingPromise,
                                                   bitcoinCommandValue: ping)
        return pendingCommand
    }
    
    func readOutboundQueue(channel: EmbeddedChannel, count: Int) throws -> [PendingBitcoinCommand] {
        struct IllegalCase: Error {}
        
        return try (1 ... count).map { _ in
            let out: QueueHandler.OutboundIn = try channel.readOutbound()!
            switch out {
            case .immediate: XCTFail(); throw IllegalCase()
            case .pending(let p): return p
            }
        }
    }
    
    
    func testQueueDequeue() {
        let testCount = 100
        
        let pendingCommands = (1...testCount).map { _ in
            self.generatePingPendingCommand()
        }
        
        
        pendingCommands.forEach {
            let writePromise = self.queueingEventLoop.makePromise(of: Void.self)
            
            writePromise.futureResult.whenFailure {
                XCTFail(String(describing: $0))
            }
            
            self.queueingChannel.write(
                NIOAny(QueueHandler.OutboundIn.pending($0)),
                promise: writePromise
            )
            //            try self.queueingChannel.writeOutbound(QueuingBitcoinHandler.CommandClassIn.pending($0))
        }
        
        (1...testCount).forEach { _ in
            self.queueingChannel.pipeline.fireUserInboundEventTriggered(PendingCommandHandler.PendingState.dequeue)
        }
        
        var outboundOuts: [PendingBitcoinCommand]!
        XCTAssertNoThrow(
            outboundOuts = try self.readOutboundQueue(channel: self.queueingChannel, count: testCount)
        )
        
        zip(pendingCommands, outboundOuts).forEach {
            let (a, b) = $0
            XCTAssertEqual(a.bitcoinCommandValue as! PingCommand,
                           b.bitcoinCommandValue as! PingCommand)
        }
        
        pendingCommands.forEach {
            $0.promise.fail(ChannelError.inputClosed)
        }
    }
    
    func testQueueingHandleCleanup() throws {
        let testCount = 100
        
        var pings = (1...testCount).map { _ in
            self.generatePingPendingCommand()
        }

        var counter = 0
        pings.forEach {
            $0.promise.futureResult.whenFailure { _ in
                counter += 1
            }
        }

        // First command goes through the Handler
        let firstPing = pings.removeFirst()
        XCTAssertNoThrow(
            try self.queueingChannel.writeOutbound(
                QueueHandler.OutboundIn.pending(firstPing)
            )
        )
        pings.forEach {
            let writePromise = self.queueingEventLoop.makePromise(of: Void.self)
            
            writePromise.futureResult.whenSuccess { XCTFail() }
            writePromise.futureResult.cascadeFailure(to: $0.promise)
            
            self.queueingChannel.write(
                NIOAny(QueueHandler.OutboundIn.pending($0)),
                promise: writePromise
            )
        }
        
        let outboundOut = try self.readOutboundQueue(channel: self.queueingChannel, count: 1)
        
        XCTAssertEqual(firstPing.bitcoinCommandValue as! PingCommand,
                       outboundOut.first!.bitcoinCommandValue as! PingCommand)
                
        struct TestError: Error {}

        firstPing.promise.fail(TestError())
        XCTAssertNoThrow(
            try self.queueingChannel.pipeline.triggerUserOutboundEvent(
                DelegatingHandler.ChannelStateEvent.cleanup(TestError())
            ).wait()
        )
        XCTAssertNoThrow(try self.queueingChannel.close().wait())
        XCTAssertEqual(counter, 100)
    }
    
    func testQueuingChannelInactiveThrows() {
        self.queueingChannel.pipeline.fireChannelInactive()
        do {
            try self.queueingChannel.throwIfErrorCaught()
        } catch(let e as BitcoinNetworkError) {
            if e != .channelUnexpectedlyClosed {
                XCTFail()
            }
        } catch {
            XCTFail()
        }
    }
    
    func testQueuingChannelInactiveAfterClose() {
        XCTAssertNoThrow(try self.queueingChannel.close().wait())
        self.queueingChannel.pipeline.fireChannelInactive()
        XCTAssertNoThrow(try self.queueingChannel.throwIfErrorCaught())
    }
}
