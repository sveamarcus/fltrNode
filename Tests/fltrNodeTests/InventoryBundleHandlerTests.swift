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
import fltrTx
import HaByLo
import NodeTestLibrary
import NIO
import XCTest

final class InventoryBundleHandlerTests: XCTestCase {
    var channel: EmbeddedChannel!
    var eventLoop: EmbeddedEventLoop!
    var handler: BundleHandler!
    var savedSettings: NodeSettings!
    typealias OutboundIn = BundleHandler.OutboundIn
    typealias OutboundOut = QueueHandler.OutboundIn
    typealias InboundIn = FrameToCommandHandler.InboundOut
    typealias InboundOut = FrameToCommandHandler.InboundOut

    override func setUp() {
        self.savedSettings = Settings
        TestingHelpers.diSetup()

        self.channel = .init()
        let loop = self.channel.eventLoop as! EmbeddedEventLoop
        self.eventLoop = loop
        self.handler = BundleHandler()
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

    func testWriteWrapped() {
        let outboundIn: OutboundIn = .wrap(.immediate(PongCommand(nonce: 0)))
        XCTAssertNoThrow(try self.channel.writeOutbound(outboundIn))
        
        if case .some(.immediate(let command)) = try? self.channel.readOutbound(as: OutboundOut.self),
            case let pong as PongCommand = command {
            XCTAssertEqual(pong.nonce, 0)
        } else {
            XCTFail()
        }
    }

    func testWriteGetblocks() {
        let pendingBitcoinCommand = createGetBlocksPendingCommand(countHashes: 3, eventLoop: self.eventLoop)
        let outboundIn: OutboundIn = .inv(.pending(pendingBitcoinCommand), .block(3))
        XCTAssertNoThrow(try self.channel.writeOutbound(outboundIn))
        
        if case .some(.pending(let command)) = try? self.channel.readOutbound(as: OutboundOut.self),
            case let gb as GetblocksCommand = command.bitcoinCommandValue {
            XCTAssertEqual(gb.blockLocator.hashes.count, 3)
        } else {
            XCTFail()
        }
    }

    func testWriteGetBlocksReadTxPassthrough() {
        let pendingBitcoinCommand = createGetBlocksPendingCommand(countHashes: 3, eventLoop: self.eventLoop)
        let outboundIn: OutboundIn = .inv(.pending(pendingBitcoinCommand), .block(3))
        XCTAssertNoThrow(try self.channel.writeOutbound(outboundIn))
        XCTAssertNoThrow(try self.channel.readOutbound(as: OutboundOut.self))

        let testTransaction = Tx.Transaction()

        XCTAssertNoThrow(
            try self.channel.writeInbound(InboundIn.pending(testTransaction))
        )

        if let out = try? self.channel.readInbound(as: InboundOut.self) {
            switch out {
            case .pending(let transaction as Tx.Transaction):
                XCTAssertEqual(transaction, testTransaction)
            default: XCTFail()
            }
        } else {
            XCTFail()
        }
    }
    
    func testWriteGetBlocksFailOnNotfoundArityMismatch() {
        let pendingBitcoinCommand = createGetBlocksPendingCommand(countHashes: 3, eventLoop: self.eventLoop)
        let outboundIn: OutboundIn = .inv(.pending(pendingBitcoinCommand), .block(3))
        XCTAssertNoThrow(try self.channel.writeOutbound(outboundIn))
        XCTAssertNoThrow(try self.channel.readOutbound(as: OutboundOut.self))

        let hash = BlockChain.Hash<TransactionLegacyHash>.big((1...32).map { UInt8($0) })
        let inv = Inventory.tx(hash)
        let notfound = NotfoundCommand(inventory: [ inv, inv, inv, inv ])
        XCTAssertThrowsError(try self.channel.writeInbound(InboundIn.pending(notfound))) { error in
            print(error)
        }
    }
    
    func testFailOnNotFoundTwice() {
        let pendingBitcoinCommand = createGetBlocksPendingCommand(countHashes: 3, eventLoop: self.eventLoop)
        let outboundIn: OutboundIn = .inv(.pending(pendingBitcoinCommand), .block(3))
        XCTAssertNoThrow(try self.channel.writeOutbound(outboundIn))
        XCTAssertNoThrow(try self.channel.readOutbound(as: OutboundOut.self))

        let hash = BlockChain.Hash<TransactionLegacyHash>.big((1...32).map { UInt8($0) })
        let inv = Inventory.tx(hash)
        let notfound = NotfoundCommand(inventory: [ inv ])
        XCTAssertNoThrow(
            try self.channel.writeInbound(InboundIn.pending(notfound))
        )
        XCTAssertThrowsError(
            try self.channel.writeInbound(InboundIn.pending(notfound))
        ) { error in
            switch error {
            case BundleHandler.Error.receiveNotFoundTwice:
                break
            default:
                XCTFail()
            }
        }
    }
}

func createGetBlocksPendingCommand(countHashes: Int, eventLoop: EventLoop) -> PendingBitcoinCommand {
    let hash = BlockChain.Hash<BlockHeaderHash>.big((1...32).map { $0 })
    let hashes: [BlockChain.Hash] = (1...countHashes).map { _ in hash }
    let locator = BlockLocator(hashes: hashes,
                                                 stop: hash)
    let getBlocks = GetblocksCommand(blockLocator: locator)
    let promise = eventLoop.makePromise(of: BitcoinCommandValue.self)
    promise.fail(ChannelError.eof)
    return .init(promise: promise, bitcoinCommandValue: getBlocks)
}
