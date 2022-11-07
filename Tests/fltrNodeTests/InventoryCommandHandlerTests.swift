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
import HaByLo
import NodeTestLibrary
import NIO
import XCTest

final class InventoryCommandHandlerTests: XCTestCase {
    var channel: EmbeddedChannel!
    var eventLoop: EmbeddedEventLoop!
    var handler: PrepareBundleRequestHandler!
    var savedSettings: NodeSettings!
    typealias OutboundIn = QueueHandler.OutboundIn
    typealias OutboundOut = BundleHandler.OutboundIn
    typealias InboundIn = FrameToCommandHandler.InboundOut
    typealias InboundOut = FrameToCommandHandler.InboundOut

    override func setUp() {
        self.savedSettings = Settings
        TestingHelpers.diSetup()

        self.channel = .init()
        let loop = self.channel.eventLoop as! EmbeddedEventLoop
        self.eventLoop = loop
        self.handler = PrepareBundleRequestHandler()
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
        
        Settings = self.savedSettings
        self.savedSettings = nil
    }
    
    func generatePingPendingCommand() -> PendingBitcoinCommand {
        let ping = PingCommand()
        let pingPromise = self.eventLoop.makePromise(of: BitcoinCommandValue.self)
        let pendingCommand = PendingBitcoinCommand(promise: pingPromise,
                                                   bitcoinCommandValue: ping)
        return pendingCommand
    }

    func testNonInventoryOutboundCommand() {
        let ping = generatePingPendingCommand()
        ping.promise.futureResult.whenComplete { _ in
            XCTFail()
        }
        XCTAssertNoThrow(try self.channel.writeAndFlush(outboundIn(ping)).wait())

        if case .some(.wrap(.pending(let command))) = try? self.channel.readOutbound(as: OutboundOut.self) {
            XCTAssertTrue(command.bitcoinCommandValue is PingCommand)
        } else {
            XCTFail()
        }
        
        XCTAssertNil(self.handler.inventoryTuple)
    }
    
    func testNonInventoryInboundCommand() {
        let pong = PongCommand(nonce: 0)
        XCTAssertNoThrow(try self.channel.writeInbound(inboundIn(pong)))
        if case .pending(let command) = try? self.channel.readInbound(as: FrameToCommandHandler.InboundOut.self) {
            XCTAssertTrue(command is PongCommand)
            XCTAssertEqual((command as? PongCommand)?.nonce, 0)
        } else {
            XCTFail()
        }
    }

    func testReadUnsolicitedInv() throws {
        let invCommand = InvCommand(
            inventory: [
                .block(.big((0..<32).map { $0 })),
            ]
        )
        XCTAssertNoThrow(try channel.writeInbound(InboundIn.unsolicited(.inv(invCommand))))
        
        if case .some(.unsolicited(.inv(let invCommand))) = try? self.channel.readInbound(as: InboundOut.self),
            case .some(.block(let hash)) = invCommand.inventory.first {
            XCTAssertEqual(hash, .big((0..<32).map { $0 }))
        } else {
            XCTFail()
        }
    }
    
    func testFullBlocksRoundtrip() {
        XCTAssertNoThrow(try addBundler(self.channel.pipeline))

        // 1. Writing getblocks command
        let getBlocks = makeGetBlocksTestCommand(eventLoop: self.eventLoop)
        getBlocks.promise.futureResult.whenComplete { _ in
            XCTFail()
        }
        XCTAssertNoThrow(try self.channel.writeAndFlush(outboundIn(getBlocks)).wait())
        
        // 2. Read actual outbound pipeline getblocks command
        if case .some(.pending(let outboundOut)) = try? self.channel.readOutbound(as: QueueHandler.OutboundIn.self) {
            XCTAssertTrue(outboundOut.bitcoinCommandValue is InventoryBundleCommand)
        } else {
            XCTFail()
        }
        
        // 3. Fake inv reply to getblocks and intercept with getdata matching two items
        let hash = BlockChain.Hash<BlockHeaderHash>.big((0..<32).map { $0 })
        let inv = InvCommand(
            inventory: [
            .block(hash),
            .block(hash),
            .block(hash),
            ]
        )
        XCTAssertNoThrow(try self.channel.writeInbound(inboundIn(inv)))
        XCTAssertNoThrow(
            XCTAssertNil(try self.channel.readInbound(as: InboundOut.self))
        )
        // getdata
        if case .some(.immediate(let command)) = try? self.channel.readOutbound(as: QueueHandler.OutboundIn.self),
            case let getData as GetdataCommand = command {
            XCTAssertEqual(getData.inventory, [
                .witnessBlock(hash),
                .witnessBlock(hash),
                .witnessBlock(hash),
            ])
        } else {
            XCTFail()
        }

        // 4. Write one block, then one NotfoundCommand, followed by a block
        XCTAssertNoThrow(try self.channel.writeInbound(InboundIn.pending(BlockCommand())))
        let notfound = NotfoundCommand(inventory: [
            .block(hash),
        ])
        XCTAssertNoThrow(try self.channel.writeInbound(InboundIn.pending(notfound)))
        XCTAssertNoThrow(try self.channel.writeInbound(InboundIn.pending(BlockCommand())))
        
        // 5. Receive inbound bundle
        if case .some(.pending(let data)) = try? self.channel.readInbound(as: InboundOut.self),
            case let bundle as BitcoinCommandBundle<Array<BlockCommand>> = data {
            XCTAssertEqual(bundle.notfound?.inventory.count, 1)
            XCTAssertEqual(bundle.value.count, 2)
        }
    }
}

@discardableResult
func addBundler(_ pipeline: ChannelPipeline) throws -> BundleHandler {
    let bundler = BundleHandler()
    try pipeline.addHandler(bundler, name: "bundler", position: .first).wait()
    return bundler
}

func getBundler(_ pipeline: ChannelPipeline) throws -> BundleHandler {
    let context = try pipeline.context(name: "bundler").wait()
    return context.handler as! BundleHandler
}

func outboundIn(_ command: PendingBitcoinCommand) -> NIOAny {
    let outbound: QueueHandler.OutboundIn = .pending(command)
    return NIOAny(outbound)
}

func inboundIn(_ command: BitcoinCommandValue) -> FrameToCommandHandler.InboundOut {
    return .pending(command)
}

func inboundIn(_ command: InvCommand) -> FrameToCommandHandler.InboundOut {
    return .unsolicited(.inv(command))
}

func makeGetBlocksTestCommand(eventLoop: EventLoop) -> PendingBitcoinCommand {
    let hash: BlockChain.Hash<BlockHeaderHash> = .big((0..<32).map { $0 })
    let locator: BlockLocator = .init(hashes: [hash], stop: hash)
    let getBlocks = GetblocksCommand(blockLocator: locator)
    let bundler: InventoryBundleCommand = .block(getBlocks)
    let promise = eventLoop.makePromise(of: BitcoinCommandValue.self)
    return .init(promise: promise, bitcoinCommandValue: bundler)
}

