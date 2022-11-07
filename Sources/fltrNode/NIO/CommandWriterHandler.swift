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
import fltrWAPI
import NIO

final class CommandWriterHandler: ChannelOutboundHandler {
    typealias OutboundIn = BitcoinCommandValue
    typealias OutboundOut = ByteBuffer
    
    let allocator = ByteBufferAllocator()
    private var framing: ByteBuffer! = nil
    private var buffer: ByteBuffer! = nil
    @Setting(\.BITCOIN_NETWORK) private var bitcoinNetwork: NetworkParameters
    
    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        defer {
            self.framing.clear()
            self.buffer.clear()
        }
        let writeRequest = self.unwrapOutboundIn(data)
        writeRequest.write(to: &buffer)
        let bufferCount = UInt32(buffer.readableBytes)
        let bufferChecksum = buffer.checksum
        self.framing.writeInteger(self.bitcoinNetwork.BITCOIN_NETWORK_MAGIC.rawValue, endianness: .little)
        let commandNameData = writeRequest.description.ascii
        self.framing.writeBytes(commandNameData)
        (commandNameData.count..<12).forEach { _ in
            self.framing.writeInteger(0, as: UInt8.self)
        }
        self.framing.writeInteger(bufferCount, endianness: .little, as: UInt32.self)
        self.framing.writeInteger(bufferChecksum, endianness: .little, as: UInt32.self)
        if bufferCount > 0 {
            context.write(self.wrapOutboundOut(self.framing), promise: nil)
            context.write(self.wrapOutboundOut(self.buffer), promise: promise)
        } else {
            context.write(self.wrapOutboundOut(self.framing), promise: promise)
        }
    }
    
    func handlerAdded(context: ChannelHandlerContext) {
        self.framing = allocator.buffer(capacity: 24)
        self.buffer = allocator.buffer(capacity: 128)
    }
}
