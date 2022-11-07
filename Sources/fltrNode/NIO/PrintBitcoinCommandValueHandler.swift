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
import var HaByLo.logger
import NIO

final class PrintBitcoinCommandValueHandler: ChannelDuplexHandler {
    typealias InboundIn = FrameToCommandHandler.InboundOut
    typealias InboundOut = InboundIn
    typealias OutboundIn = QueueHandler.OutboundIn
    typealias OutboundOut = OutboundIn
    
    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        var out: String = ""
        switch unwrapInboundIn(data) {
        case .incoming(.getaddr(let debug as CustomDebugStringConvertible)),
             .incoming(.getcfcheckpt(let debug as CustomDebugStringConvertible)),
             .incoming(.getcfheaders(let debug as CustomDebugStringConvertible)),
             .incoming(.getcfilters(let debug as CustomDebugStringConvertible)),
             .incoming(.getheaders(let debug as CustomDebugStringConvertible)),
             .incoming(.ping(let debug as CustomDebugStringConvertible)),
             .incoming(.unhandled(let debug as CustomDebugStringConvertible)),
             .incoming(.version(let debug as CustomDebugStringConvertible)):
            out.append("❓REQUEST \(debug.debugDescription)")
                
        case .pending(let debug as CustomDebugStringConvertible):
            out.append("❗️REPLY \(debug.debugDescription)")
            
        case .unsolicited(.addr(let debug as CustomDebugStringConvertible)),
             .unsolicited(.alert(let debug as CustomDebugStringConvertible)),
             .unsolicited(.feefilter(let debug as CustomDebugStringConvertible)),
             .unsolicited(.inv(let debug as CustomDebugStringConvertible)),
             .unsolicited(.reject(let debug as CustomDebugStringConvertible)),
             .unsolicited(.sendcmpct(let debug as CustomDebugStringConvertible)),
             .unsolicited(.sendheaders(let debug as CustomDebugStringConvertible)):
            out.append("🏳unsolicited \(debug.debugDescription)")
            
        case .incoming(let next):
            out.append(".incoming command (with no custom renderer) \(String(reflecting: next))")
        case .pending(let next):
            out.append(".pending command (with no custom renderer) \(String(reflecting: next))")
        case .unsolicited(let next):
            out.append(".unsolicited (with no custom renderer) \(String(reflecting: next))")
        }
        
        logger.trace("\n🟩🟩⏬\t IN", out)
        context.fireChannelRead(data)
    }
    
    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let unwrap = self.unwrapOutboundIn(data)

        var out = ""
        
        switch unwrap {
        
        case .immediate(let debug as CustomDebugStringConvertible):
            out.append(debug.debugDescription)
            
        case .pending(let command) where command.bitcoinCommandValue is CustomDebugStringConvertible:
            out.append("❓REQUEST \((command.bitcoinCommandValue as! CustomDebugStringConvertible).debugDescription)")
        
        case .immediate(let value):
            out.append(".immediate (with no custom renderer) \(String(reflecting: value))")
         
        case .pending(let pending):
            out.append(".pending (with no custom renderer) \(String(reflecting: pending))")
        }
        
        logger.trace("\n⏫🟧🟧\tOUT", out)

        context.write(data, promise: promise)
    }
}
