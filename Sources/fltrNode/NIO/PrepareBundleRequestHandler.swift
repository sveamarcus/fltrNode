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
import NIO

// MARK: OUTBOUND Pending command interceptor
final class PrepareBundleRequestHandler: ChannelOutboundHandler {
    typealias OutboundIn = QueueHandler.OutboundIn
    typealias OutboundOut = BundleHandler.OutboundIn
    
    private(set) var inventoryTuple: (filter: InventoryBundleCommand.FilterType,
                                      promise: Promise<[Inventory]>)? = nil
    

    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let unwrap = self.unwrapOutboundIn(data)
        switch unwrap {
        case .pending(let pendingBitcoinCommand) where pendingBitcoinCommand.bitcoinCommandValue is InventoryBundleCommand:
            let inventoryPromise = context.eventLoop.makePromise(of: [Inventory].self, file: #file, line: #line)
            // For timeout notification:
            pendingBitcoinCommand.promise.futureResult.cascadeFailure(to: inventoryPromise)

            // Executed when inventoryFilter matches
            inventoryPromise.futureResult.whenComplete {
                Self.filteredInvComplete($0,
                                         context: context,
                                         pendingBitcoinCommandPromise: pendingBitcoinCommand.promise)
            }
            
            self.inventoryTuple = (filter: (pendingBitcoinCommand.bitcoinCommandValue as! InventoryBundleCommand).filter,
                                   promise: inventoryPromise)
            context.write(self.wrapOutboundOut(.wrap(unwrap)), promise: promise)
            
        case .pending(let pendingBitcoinCommand) where pendingBitcoinCommand.bitcoinCommandValue is CFBundleCommand:
            assert(self.inventoryTuple == nil)
            
            let count = (pendingBitcoinCommand.bitcoinCommandValue as! CFBundleCommand).count
            context.write(self.wrapOutboundOut(.inv(unwrap, .cfilter(count))), promise: promise)
            
        case .pending,
             .immediate:
            context.write(self.wrapOutboundOut(.wrap(unwrap)), promise: promise)
        }
    }
}

extension PrepareBundleRequestHandler: ChannelInboundHandler {
    typealias InboundIn = FrameToCommandHandler.InboundOut
    typealias InboundOut = FrameToCommandHandler.InboundOut

    // MARK: INBOUND Inv filter execution
    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let unwrap = self.unwrapInboundIn(data)
        switch (unwrap, self.inventoryTuple) {
        case (.unsolicited(.inv(let invCommand)), .some((let filter, let promise))):
            do {
                let result = try filter(invCommand.inventory).get()
                self.inventoryTuple = nil
                promise.succeed(result)
            } catch {
                fallthrough
            }
        default:
            context.fireChannelRead(data)
        }
    }
}

extension PrepareBundleRequestHandler {
    // MARK: OUTBOUND Callback/getdata step
    static private func filteredInvComplete(_ filteredInvs: Result<[Inventory], Swift.Error>,
                                            context: ChannelHandlerContext,
                                            pendingBitcoinCommandPromise: Promise<BitcoinCommandValue>) -> Void {
        switch filteredInvs {
        // time-out errors handled elsewhere
        case .failure(let error as BitcoinNetworkError) where error == .pendingCommandTimeout,
             .failure(let error as BitcoinNetworkError) where error == .pendingCommandStallingTimeout:
            break
        case .failure(let error):
            context.fireErrorCaught(error)
        case .success(let filtered):
            if filtered.count > 0 {
                let inventoryBundleType: BundleHandler.InventoryBundleType
                switch filtered.first! {
                case .block:
                    inventoryBundleType = .block(filtered.count)
                case .tx:
                    inventoryBundleType = .txs(filtered.count)
                case .compactBlock, .filteredBlock, .error, .witnessBlock, .witnessTx:
                    pendingBitcoinCommandPromise.fail(BitcoinNetworkError.illegalInvCommand)
                    return
                }
                
                let witnessMapped: [Inventory] = filtered.compactMap {
                    switch $0 {
                    case .tx(let hash):
                        return .witnessTx(hash)
                    case .block(let hash):
                        return .witnessBlock(hash)
                    default:
                        return $0
                    }
                }
                
                let getdata = NIOAny(
                    BundleHandler.OutboundIn.inv(
                        // .immediate passthrough of data request since handled by BundleHandler
                        QueueHandler.OutboundIn.immediate(GetdataCommand(inventory: witnessMapped)),
                        inventoryBundleType
                    )
                )
                let writePromise = context.eventLoop.makePromise(of: Void.self, file: #file, line: #line)
                writePromise.futureResult.cascadeFailure(to: pendingBitcoinCommandPromise)
                context.writeAndFlush(getdata, promise: writePromise)
            } else {
                pendingBitcoinCommandPromise.fail(BitcoinNetworkError.emptyCommandReceived)
            }
        }
    }
}

extension PrepareBundleRequestHandler: CleanupLocalStateEvent {
    func cleanup(error: Error) {
        let save = self.inventoryTuple
        self.inventoryTuple = nil
        save?.promise.fail(error)
    }
}
