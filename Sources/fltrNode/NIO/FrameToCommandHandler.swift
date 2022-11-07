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

final class FrameToCommandHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer
    enum InboundOut {
        case incoming(IncomingCommand)
        case pending(BitcoinCommandValue)
        case unsolicited(UnsolicitedCommand)
    }

    enum IncomingCommand {
        case getaddr(GetaddrCommand)
        case getcfcheckpt(CF.GetcfcheckptCommand)
        case getcfheaders(CF.GetcfheadersCommand)
        case getcfilters(CF.GetcfiltersCommand)
        case getdata(GetdataCommand)
        case getheaders(GetheadersCommand)
        case ping(PingCommand)
        case unhandled(UnhandledCommand)
        case version(VersionCommand)
    }

    enum UnsolicitedCommand {
        case addr(AddrCommand)
        case alert(AlertCommand)
        case feefilter(FeefilterCommand)
        case inv(InvCommand)
        case reject(RejectCommand)
        case sendaddrv2
        case sendcmpct(SendcmpctCommand)
        case sendheaders(SendheadersCommand)
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        var buffer = unwrapInboundIn(data)
        let commandBytes = buffer.getBytes(at: 4, length: 12)!.filter { $0 > 0}
        let commandName = String(bytes: commandBytes, encoding: .ascii)!
        buffer.moveReaderIndex(forwardBy: 24)

        let command: InboundOut? = commandName.incomingCommand(buffer: &buffer)
        guard let guardCommand = command else {
            logger.error(String(describing: Self.self), #function, "- Received unknown command or failed decoding [", commandName, "]")
            context.fireErrorCaught(BitcoinNetworkError.cannotDecodeIncomingCommand)
            return
        }

        context.fireChannelRead(self.wrapInboundOut(guardCommand))
    }
}

