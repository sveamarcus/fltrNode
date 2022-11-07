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

final class VersionHandshakeHandlerTests: XCTestCase {
    var channel: EmbeddedChannel!
    var eventLoop: EmbeddedEventLoop!
    var donePromise: Promise<VersionCommand>!
    var handler: VersionHandshakeHandler!
    var savedSettings: NodeSettings!
    
    typealias InboundIn = FrameToCommandHandler.InboundOut
    typealias InboundOut = FrameToCommandHandler.InboundOut
    typealias OutboundOut = QueueHandler.OutboundIn

    override func setUp() {
        self.savedSettings = Settings
        TestingHelpers.diSetup()

        self.channel = .init()
        let loop = self.channel.eventLoop as! EmbeddedEventLoop
        self.eventLoop = loop
        self.donePromise = loop.makePromise(of: VersionCommand.self)
        let versionCommand = VersionCommand()
        self.handler = VersionHandshakeHandler(done: self.donePromise, outgoing: versionCommand)
        
        XCTAssertNoThrow(try self.channel.pipeline.addHandler(self.handler).wait())
        XCTAssertNoThrow(try self.channel.connect(to: .init(ipAddress: "1.1.1.1", port: 1)).wait())
    }
    
    override func tearDown() {
        XCTAssertNoThrow(
            try self.channel.finish(acceptAlreadyClosed: true)
        )
        self.eventLoop = nil
        self.channel = nil
        self.handler = nil
        self.donePromise = nil
        
        Settings = self.savedSettings
        self.savedSettings = nil
    }
    
    func testHandshakeVerackBeforeVersion() {
        let verackIn: InboundIn = .pending(VerackCommand())
        XCTAssertNoThrow(try self.channel.writeInbound(verackIn))

        let versionIn: InboundIn = .incoming(.version(.init()))
        XCTAssertNoThrow(try self.channel.writeInbound(versionIn))
        
        var outboundOut: OutboundOut!
        XCTAssertNoThrow(
            outboundOut = try self.channel.readOutbound(as: OutboundOut.self)
        )
        switch outboundOut {
        case .some(.immediate(let version)) where version is VersionCommand:
            break
        default:
            XCTFail()
        }

        XCTAssertNoThrow(
            outboundOut = try self.channel.readOutbound(as: OutboundOut.self)
        )
        switch outboundOut {
        case .some(.immediate(let verack)) where verack is VerackCommand:
            break
        default:
            XCTFail()
        }

        XCTAssertThrowsError(
            try self.channel.pipeline.handler(type: VersionHandshakeHandler.self).wait()
        )
    }

    func testHandshakeVersionBeforeVerack() {
        let versionIn: InboundIn = .incoming(.version(.init()))
        XCTAssertNoThrow(try self.channel.writeInbound(versionIn))
        
        var outboundOut: OutboundOut!
        XCTAssertNoThrow(
            outboundOut = try self.channel.readOutbound(as: OutboundOut.self)
        )
        switch outboundOut {
        case .some(.immediate(let version)) where version is VersionCommand:
            break
        default:
            XCTFail()
        }

        XCTAssertNoThrow(
            outboundOut = try self.channel.readOutbound(as: OutboundOut.self)
        )
        switch outboundOut {
        case .some(.immediate(let verack)) where verack is VerackCommand:
            break
        default:
            XCTFail()
        }

        let verackIn: InboundIn = .pending(VerackCommand())
        XCTAssertNoThrow(try self.channel.writeInbound(verackIn))
        
        XCTAssertThrowsError(
            try self.channel.pipeline.handler(type: VersionHandshakeHandler.self).wait()
        )
    }

    class ErrorCatcher: ChannelInboundHandler {
        var triggered = false
        typealias InboundIn = FrameToCommandHandler.InboundOut
        func errorCaught(context: ChannelHandlerContext, error: Error) {
            triggered = true
            context.fireErrorCaught(error)
        }
    }

    func testDoubleVersionError() {
        let versionIn: InboundIn = .incoming(.version(.init()))
        XCTAssertNoThrow(try self.channel.writeInbound(versionIn))
        XCTAssertNoThrow(try self.channel.readOutbound(as: OutboundOut.self))
        XCTAssertNoThrow(try self.channel.readOutbound(as: OutboundOut.self))
        
        let errorCatcher = ErrorCatcher()
        try! self.channel.pipeline.addHandler(errorCatcher).wait()

        XCTAssertThrowsError(try self.channel.writeInbound(versionIn), "",
                             verifyErrorType(type: VersionHandshakeHandler.StateMachine.SMError.self))
        XCTAssertThrowsError(try self.donePromise.futureResult.wait(), "",
                             verifyErrorType(type: VersionHandshakeHandler.StateMachine.SMError.self))
        XCTAssertTrue(errorCatcher.triggered)
    }
    
    func testDoubleVerackError() {
        let verackIn: InboundIn = .pending(VerackCommand())
        XCTAssertNoThrow(try self.channel.writeInbound(verackIn))
        XCTAssertNoThrow(try self.channel.readOutbound(as: OutboundOut.self))
        XCTAssertNoThrow(try self.channel.readOutbound(as: OutboundOut.self))

        let errorCatcher = ErrorCatcher()
        try! self.channel.pipeline.addHandler(errorCatcher).wait()

        XCTAssertThrowsError(try self.channel.writeInbound(verackIn), "",
                             verifyErrorType(type: VersionHandshakeHandler.StateMachine.SMError.self))
        XCTAssertThrowsError(try self.donePromise.futureResult.wait(), "",
                             verifyErrorType(type: VersionHandshakeHandler.StateMachine.SMError.self))
        XCTAssertTrue(errorCatcher.triggered)
    }
    
    func testTimeoutErrorNoCommands() {
        self.eventLoop.advanceTime(by: Settings.BITCOIN_COMMAND_TIMEOUT + .seconds(1))
        XCTAssertThrowsError(try self.donePromise.futureResult.wait()) {
            if case let error as BitcoinNetworkError = $0, error == .pendingCommandTimeout {
                return
            }
            XCTFail()
        }
        XCTAssertThrowsError(try self.channel.finish(), "",
                             verifyErrorType(type: BitcoinNetworkError.self))
    }
    
    func testTimeoutErrorVerackSent() {
        let verackIn: InboundIn = .pending(VerackCommand())
        XCTAssertNoThrow(try self.channel.writeInbound(verackIn))
        XCTAssertNoThrow(try self.channel.readOutbound(as: OutboundOut.self))
        XCTAssertNoThrow(try self.channel.readOutbound(as: OutboundOut.self))

        self.testTimeoutErrorNoCommands()
    }
    
    func testTimeoutErrorVersionSent() {
        let versionIn: InboundIn = .incoming(.version(.init()))
        XCTAssertNoThrow(try self.channel.writeInbound(versionIn))
        XCTAssertNoThrow(try self.channel.readOutbound(as: OutboundOut.self))
        XCTAssertNoThrow(try self.channel.readOutbound(as: OutboundOut.self))

        self.testTimeoutErrorNoCommands()
    }
    
    func testTimeoutCancel() {
        let verackIn: InboundIn = .pending(VerackCommand())
        XCTAssertNoThrow(try self.channel.writeInbound(verackIn))
        let versionIn: InboundIn = .incoming(.version(.init()))
        XCTAssertNoThrow(try self.channel.writeInbound(versionIn))

        self.eventLoop.advanceTime(by: Settings.BITCOIN_COMMAND_TIMEOUT + .seconds(1))
        XCTAssertNoThrow(try self.donePromise.futureResult.wait())
    }
}

func verifyErrorType<E: Error>(type: E.Type) -> (Error) -> () {
    { error in
        switch error {
        case let error where error is E:
            break
        default: XCTFail()
        }
    }
}

