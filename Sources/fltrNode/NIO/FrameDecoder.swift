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

final class FrameDecoder: ByteToMessageDecoder {
    typealias InboundOut = ByteBuffer
    @Setting(\.BITCOIN_NETWORK) var bitcoinNetwork: NetworkParameters
    
    enum BitcoinDecoderState {
        case lookingForMagic
        case waitingForLength
        case readingPayload(length: UInt32)
    }
    
    func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
        guard let networkInt = buffer.getInteger(at: buffer.readerIndex, endianness: .little, as: UInt32.self),
            let headerLength = buffer.getInteger(at: buffer.readerIndex + 16, endianness: .little, as: UInt32.self),
            let command = buffer.readSlice(length: Int(headerLength) + 24) else {
                return .needMoreData
        }
        
        guard let network = BitcoinNetworkMagic(rawValue: networkInt),
            network == self.bitcoinNetwork.BITCOIN_NETWORK_MAGIC else {
                throw BitcoinNetworkError.decoderErrorIllegalNetworkReceived
        }
        
        context.fireChannelRead(self.wrapInboundOut(command))
        return .continue
    }

    func decodeLast(context: ChannelHandlerContext, buffer: inout ByteBuffer, seenEOF: Bool) throws -> DecodingState {
        return .needMoreData
    }
}
