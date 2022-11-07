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
import struct NIO.ByteBuffer
import enum fltrTx.BitcoinScript
import enum fltrTx.OpCodes
import typealias fltrTx.Script

extension ByteBuffer {
    @usableFromInline
    mutating func writeChunk(_ chunk: BitcoinScript.OpCodeData) {
        switch chunk {
        case .error(let opCode, let buffer):
            self.writeInteger(opCode)
            self.writeBytes(buffer)
        case .opN(let n):
            if n == 0 {
                self.writeInteger(n)
            } else {
                self.writeInteger(n - 1 + OpCodes.OP_1)
            }
        case .pushdata0(let buffer):
            assert(buffer.count < OpCodes.OP_PUSHDATA1)
            self.writeInteger(UInt8(buffer.count))
            self.writeBytes(buffer)
        case .pushdata1(let buffer):
            assert(buffer.count < UInt8.max)
            self.writeInteger(OpCodes.OP_PUSHDATA1)
            self.writeInteger(UInt8(buffer.count))
            self.writeBytes(buffer)
        case .pushdata2(let buffer):
            assert(buffer.count < UInt16.max)
            self.writeInteger(OpCodes.OP_PUSHDATA2)
            self.writeInteger(UInt16(buffer.count))
            self.writeBytes(buffer)
        case .pushdata4(let buffer):
            assert(buffer.count < UInt32.max)
            self.writeInteger(OpCodes.OP_PUSHDATA4)
            self.writeInteger(UInt32(buffer.count))
            self.writeBytes(buffer)
        case .other(let op):
            self.writeInteger(op)
        }
    }
    
    @usableFromInline
    mutating func writeChunk(_ chunks: Script) {
        for c in chunks {
            self.writeChunk(c)
        }
    }
}
