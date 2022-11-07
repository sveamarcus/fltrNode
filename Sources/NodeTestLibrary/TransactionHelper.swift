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
import fltrTx

public extension Tx.In {
    init(outpoint: Tx.Outpoint) {
        self.init(outpoint: outpoint,
                  scriptSig: [],
                  sequence: .disable,
                  witness: { nil })
    }
}

public extension Tx {
    static func makeSegwitTx(for outpoint: Tx.Outpoint,
                             scriptPubKey: [UInt8] = Script([ .opN(1),
                                                              .pushdata0((1...32)
                                                                            .map { _ in 0x99 }) ]).bytes)
    -> Tx.AnyTransaction {
        let witness: Tx.Witness = .init(witnessField: [
            "30440220366a7a23086502fe37631ec65702e407d4bce28d767ed312ae007fd5724f41bf022039d528c5036df43427d952742255f281686720794e08861f86383b10e483400101".hex2Bytes,
            "03932107d99664734886f3a2dc8e889f39bd8c751de5b5ee0066d9a70626464862".hex2Bytes,
        ])
        
        return .segwit(Tx.SegwitTransaction(version: 1,
                                            vin: [ Tx.In(outpoint: outpoint,
                                                         scriptSig: [],
                                                         sequence: .disable, witness: { witness }) ],
                                            vout: [ .init(value: 10_000, scriptPubKey: scriptPubKey) ],
                                            locktime: .disable(.max))!)
    }
}

@testable import fltrNode
extension Tx.SegwitTransaction: BitcoinCommandValue {
    public var description: String { "tx" }
}
