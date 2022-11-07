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
import struct Foundation.Data

public class PrintEverythingHandler: ChannelDuplexHandler {
    public typealias InboundIn = ByteBuffer
    public typealias InboundOut = ByteBuffer
    public typealias OutboundIn = ByteBuffer
    public typealias OutboundOut = ByteBuffer
    
    public func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let buffer = self.unwrapInboundIn(data)
        let dataData = buffer.getData(at: buffer.readerIndex, length: buffer.readableBytes)
        guard let d = dataData else { return }
        logger.trace("channel READ", "raw(\(String(decoding: d, as: UTF8.self))", " hex(\(d.hexEncodedString))")
        context.fireChannelRead(data)
    }
    
    public func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let buffer = Data(self.unwrapOutboundIn(data).readableBytesView)
        logger.trace("channel WRITE", "hex[", buffer.hexEncodedString, "] ascii[", String(decoding: buffer, as: UTF8.self), "]")
        context.write(data, promise: promise)
    }
    
    public init() {}
}
