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

final class PendingCommandHandlerTests: XCTestCase {
    var pendingChannel: EmbeddedChannel!
    var pendingEventLoop: EmbeddedEventLoop!
    var pendingBitcoinHandler: PendingCommandHandler!
    
    override func setUp() {
        self.pendingChannel = .init()
        let pLoop = self.pendingChannel.eventLoop as! EmbeddedEventLoop
        self.pendingEventLoop = pLoop
        self.pendingBitcoinHandler = PendingCommandHandler()
        XCTAssertNoThrow(try self.pendingChannel.pipeline.addHandler(self.pendingBitcoinHandler).wait())
        XCTAssertNoThrow(try self.pendingChannel.connect(to: .init(ipAddress: "1.1.1.1", port: 1)).wait())
    }
    
    override func tearDown() {
        XCTAssertNoThrow(try self.pendingChannel.finish(acceptAlreadyClosed: true))
        self.pendingChannel = nil
        self.pendingEventLoop = nil
    }
    
    func generatePingPendingCommand() -> PendingBitcoinCommand {
        let ping = PingCommand()
        let pingPromise = self.pendingEventLoop.makePromise(of: BitcoinCommandValue.self)
        let pendingCommand = PendingBitcoinCommand(promise: pingPromise,
                                                   bitcoinCommandValue: ping)
        return pendingCommand
    }
    
    func testThrowsWhenOverloaded() throws {
        let writePromise1 = self.pendingEventLoop.makePromise(of: Void.self)
        let writePromise2 = self.pendingEventLoop.makePromise(of: Void.self)
        
        let ping1 = self.generatePingPendingCommand()
        writePromise1.futureResult.cascadeFailure(to: ping1.promise)
        self.pendingChannel.write(
            NIOAny(QueueHandler.OutboundIn.pending(
                    ping1)),
            promise: writePromise1
        )
        
        let ping2 = self.generatePingPendingCommand()
        writePromise2.futureResult.cascadeFailure(to: ping2.promise)
        self.pendingChannel.write(
            NIOAny(QueueHandler.OutboundIn.pending(
                    ping2)),
            promise: writePromise2)
        self.pendingChannel.flush()
        
        try writePromise1.futureResult.wait()
        
        do {
            try writePromise2.futureResult.wait()
            XCTFail()
        } catch (BitcoinNetworkError.illegalStatePendingPromisesNotNil) {
            ()
        } catch {
            XCTFail()
        }
        XCTAssertThrowsError(try self.pendingChannel.throwIfErrorCaught())
        
        XCTAssertNoThrow(try self.pendingChannel.close().wait())
    }
    
    func testDequeueEvent() {
        class TestHandler: ChannelInboundHandler {
            var triggered = false
            typealias InboundIn = QueueHandler.OutboundIn
            
            func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
                guard case let e as PendingCommandHandler.PendingState = event, e == .dequeue else {
                    XCTFail()
                    return
                }
                self.triggered = true
            }
        }
        
        let pingPendingCommand = generatePingPendingCommand()
        XCTAssertNoThrow(
            try self.pendingChannel.writeOutbound(
                QueueHandler.OutboundIn.pending(pingPendingCommand)
            )
        )
        
        guard case .some(.pending(let pending)) =
            try? self.pendingChannel.readOutbound(as: QueueHandler.OutboundIn.self),
            case let ping as PingCommand = pending.bitcoinCommandValue else {
                XCTFail()
                return
        }
        
        let testHandler = TestHandler()
        XCTAssertNoThrow(
            self.pendingChannel.pipeline.addHandler(testHandler, position: .last)
        )

        let pong: FrameToCommandHandler.InboundOut = .pending(PongCommand(nonce: ping.nonce))
        XCTAssertNoThrow(
            try self.pendingChannel.writeInbound(pong)
        )
        XCTAssertNoThrow(
            XCTAssertNil(try self.pendingChannel.readInbound(as: DelegatingHandler.InboundIn.self))
        )
        XCTAssertEqual(testHandler.triggered, true)
        
        XCTAssertNoThrow(
            try self.pendingChannel.writeOutbound(
                QueueHandler.OutboundIn.pending(generatePingPendingCommand())
            )
        )
        XCTAssertNoThrow(
            XCTAssertNotNil(try self.pendingChannel.readOutbound())
        )
    }
    
    func testIdleStateEvent() {
        let pingPendingCommand = generatePingPendingCommand()
        pingPendingCommand.promise.futureResult.whenComplete {
            switch $0 {
            case .failure(let error):
                switch error {
                case let error as BitcoinNetworkError where error == .pendingCommandStallingTimeout:
                    return
                default:
                    XCTFail()
                }
            case .success:
                XCTFail()
            }
        }
        XCTAssertNoThrow(
            try self.pendingChannel.writeOutbound(
                QueueHandler.OutboundIn.pending(pingPendingCommand)
            )
        )
        XCTAssertNoThrow(
            XCTAssertNotNil(try self.pendingChannel.readOutbound(as: QueueHandler.OutboundIn.self))
        )
        
        class TestHandler: ChannelInboundHandler {
            var triggered = false
            typealias InboundIn = QueueHandler.OutboundIn
            
            func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
                guard event is PendingCommandHandler.PendingThrottleTimeout else {
                    XCTFail()
                    return
                }
                self.triggered = true
            }
        }

        let testHandler = TestHandler()
        XCTAssertNoThrow(
            self.pendingChannel.pipeline.addHandler(testHandler, position: .last)
        )
        self.pendingChannel.pipeline.fireUserInboundEventTriggered(IdleStateHandler.IdleStateEvent.all)
        XCTAssertEqual(testHandler.triggered, true)
    }
    
    func testCleanup() {
        let pingPendingCommand = generatePingPendingCommand()
        pingPendingCommand.promise.futureResult.whenComplete {
            switch $0 {
            case .failure(let error) where error is TestError:
                return
            case .failure(let error):
                XCTFail(error.localizedDescription)
            case .success:
                XCTFail()
            }
        }
        XCTAssertNoThrow(
            try self.pendingChannel.writeOutbound(
                QueueHandler.OutboundIn.pending(pingPendingCommand)
            )
        )
        
        struct TestError: Error {}

        XCTAssertNoThrow(try self.pendingChannel.pipeline.triggerUserOutboundEvent(
            DelegatingHandler.ChannelStateEvent.cleanup(TestError())).wait()
        )
    }
}
