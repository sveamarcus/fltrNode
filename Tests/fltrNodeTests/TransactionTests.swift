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
import fltrECC
import fltrECCTesting
import fltrTx
import NodeTestLibrary
import NIO
import XCTest

final class TransactionTests: XCTestCase {
    var vector1: ByteBuffer!
    var vector2: ByteBuffer!
    var vector3: ByteBuffer!
    var vector4: ByteBuffer!
    var withWitness: ByteBuffer!
    var withoutWitness: ByteBuffer!
    let withoutWitnessPrevouts: [Tx.Out] = [
        .init(value: 800_000,
              scriptPubKey: [ OpCodes.OP_DUP, OpCodes.OP_HASH160, 0x14 ]
              + "664e81d2e8e5d76314fa83b3111d706bd444e292".hex2Bytes
              + [ OpCodes.OP_EQUALVERIFY, OpCodes.OP_CHECKSIG ]),
        .init(value: 220_000,
              scriptPubKey: [ OpCodes.OP_DUP, OpCodes.OP_HASH160, 0x14 ]
              + "d89f72913e7852882957af2bdc64ecbef95f71bf".hex2Bytes
              + [ OpCodes.OP_EQUALVERIFY, OpCodes.OP_CHECKSIG ]),
        .init(value: 97_340,
              scriptPubKey: [ OpCodes.OP_DUP, OpCodes.OP_HASH160, 0x14 ]
              + "a1a687e6d2476ff2ae8e00894e8be9a169068663".hex2Bytes
              + [ OpCodes.OP_EQUALVERIFY, OpCodes.OP_CHECKSIG ]),
    ]
    var allocator: ByteBufferAllocator!
    var savedSettings: NodeSettings!
    
    override func setUp() {
        self.savedSettings = Settings
        TestingHelpers.diSetup()

        self.allocator = ByteBufferAllocator()
        self.vector1 = self.loadTestTransaction1()
        self.vector2 = self.loadTestTransaction2()
        self.vector3 = self.loadTestTransaction3()
        self.vector4 = self.loadTestTransaction4()
        self.withWitness = self.loadWithWithness()
        self.withoutWitness = self.loadWithoutWitness()
    }
    
    override func tearDown() {
        self.vector1 = nil
        self.vector2 = nil
        self.vector3 = nil
        self.vector4 = nil
        self.withWitness = nil
        self.withoutWitness = nil
        self.allocator = nil
        
        Settings = self.savedSettings
        self.savedSettings = nil
    }
    
    func loadTestTransaction1() -> ByteBuffer {
        "0100000000010115e180dc28a2327e687facc33f10f2a20da717e5548406f7ae8b4c811072f856040000002322002001d5d92effa6ffba3efa379f9830d0f75618b13393827152d26e4309000e88b1ffffffff0188b3f505000000001976a9141d7cd6c75c2e86f4cbf98eaed221b30bd9a0b92888ac02473044022038421164c6468c63dc7bf724aa9d48d8e5abe3935564d38182addf733ad4cd81022076362326b22dd7bfaf211d5b17220723659e4fe3359740ced5762d0e497b7dcc012321038262a6c6cec93c2d3ecd6c6072efea86d02ff8e3328bbd0242b20af3425990acac00000000".hex2Bytes.buffer
    }
    
    func loadTestTransaction2() -> ByteBuffer {
        "0200000000010140d43a99926d43eb0e619bf0b3d83b4a31f60c176beecfb9d35bf45e54d0f7420100000017160014a4b4ca48de0b3fffc15404a1acdc8dbaae226955ffffffff0100e1f5050000000017a9144a1154d50b03292b3024370901711946cb7cccc387024830450221008604ef8f6d8afa892dee0f31259b6ce02dd70c545cfcfed8148179971876c54a022076d771d6e91bed212783c9b06e0de600fab2d518fad6f15a2b191d7fbd262a3e0121039d25ab79f41f75ceaf882411fd41fa670a4c672c23ffaf0e361a969cde0692e800000000".hex2Bytes.buffer
    }
    
    func loadTestTransaction3() -> ByteBuffer {
        "01000000017967a5185e907a25225574544c31f7b059c1a191d65b53dcc1554d339c4f9efc010000006a47304402206a2eb16b7b92051d0fa38c133e67684ed064effada1d7f925c842da401d4f22702201f196b10e6e4b4a9fff948e5c5d71ec5da53e90529c8dbd122bff2b1d21dc8a90121039b7bcd0824b9a9164f7ba098408e63e5b7e3cf90835cceb19868f54f8961a825ffffffff014baf2100000000001976a914db4d1141d0048b1ed15839d0b7a4c488cd368b0e88ac00000000".hex2Bytes.buffer
    }
    
    func loadTestTransaction4() -> ByteBuffer {
        let string = "01000000" // version
            + "00" // marker
            + "01" // flag
            + "02" // num txIn
            + "fff7f7881a8099afa6940d42d1e7f6362bec38171ea3edf433541db4e4ad969f" + "00000000"
            + "494830450221008b9d1dc26ba6a9cb62127b02742fa9d754cd3bebf337f7a55d114c8e5cdd30be022040529b194ba3f9281a99f2b1c0a19c0489bc22ede944ccf4ecbab4cc618ef3ed01"
            + "eeffffff" // txIn0
            + "ef51e1b804cc89d182d279655c3aa89e815b1b309fe287d9b2b55d57b90ec68a" + "01000000" + "00" + "ffffffff" // txIn1
            + "02" // num txOut
            + "202cb20600000000" + "1976a914" + "8280b37df378db99f66f85c95a783a76ac7a6d59" + "88ac" // txOut0
            + "9093510d00000000" + "1976a914" + "3bde42dbee7e4dbe6a21b2d50ce2f0167faa8159" + "88ac" // txOut1
            + "00" // witness0 (empty)
            + "02" // witness1 (2 pushes)
            + "47" // push length
            + "304402203609e17b84f6a7d30c80bfa610b5b4542f32a8a0d5447a12fb1366d7f01cc44a0220573a954c4518331561406f90300e8f3358f51928d43c212a8caed02de67eebee01" // push
            + "21" // push length
            + "025476c2e83188368da1ff3e292e7acafcdb3566bb0ad253f62fc70f07aeee6357" // push / witness size 110
            + "11000000"
        return string.hex2Bytes.buffer
    }
    
    func loadWithWithness() -> ByteBuffer {
        "0100000000010213206299feb17742091c3cb2ab45faa3aa87922d3c030cafb3f798850a2722bf0000000000feffffffa12f2424b9599898a1d30f06e1ce55eba7fabfeee82ae9356f07375806632ff3010000006b483045022100fcc8cf3014248e1a0d6dcddf03e80f7e591605ad0dbace27d2c0d87274f8cd66022053fcfff64f35f22a14deb657ac57f110084fb07bb917c3b42e7d033c54c7717b012102b9e4dcc33c9cc9cb5f42b96dddb3b475b067f3e21125f79e10c853e5ca8fba31feffffff02206f9800000000001976a9144841b9874d913c430048c78a7b18baebdbea440588ac8096980000000000160014e4873ef43eac347471dd94bc899c51b395a509a502483045022100dd8250f8b5c2035d8feefae530b10862a63030590a851183cb61b3672eb4f26e022057fe7bc8593f05416c185d829b574290fb8706423451ebd0a0ae50c276b87b43012102179862f40b85fa43487500f1d6b13c864b5eb0a83999738db0f7a6b91b2ec64f00db080000".hex2Bytes.buffer
    }
    
    func loadWithoutWitness() -> ByteBuffer {
        "0100000003362c10b042d48378b428d60c5c98d8b8aca7a03e1a2ca1048bfd469934bbda95010000008b483045022046c8bc9fb0e063e2fc8c6b1084afe6370461c16cbf67987d97df87827917d42d022100c807fa0ab95945a6e74c59838cc5f9e850714d8850cec4db1e7f3bcf71d5f5ef0141044450af01b4cc0d45207bddfb47911744d01f768d23686e9ac784162a5b3a15bc01e6653310bdd695d8c35d22e9bb457563f8de116ecafea27a0ec831e4a3e9feffffffffc19529a54ae15c67526cc5e20e535973c2d56ef35ff51bace5444388331c4813000000008b48304502201738185959373f04cc73dbbb1d061623d51dc40aac0220df56dabb9b80b72f49022100a7f76bde06369917c214ee2179e583fefb63c95bf876eb54d05dfdf0721ed772014104e6aa2cf108e1c650e12d8dd7ec0a36e478dad5a5d180585d25c30eb7c88c3df0c6f5fd41b3e70b019b777abd02d319bf724de184001b3d014cb740cb83ed21a6ffffffffbaae89b5d2e3ca78fd3f13cf0058784e7c089fb56e1e596d70adcfa486603967010000008b483045022055efbaddb4c67c1f1a46464c8f770aab03d6b513779ad48735d16d4c5b9907c2022100f469d50a5e5556fc2c932645f6927ac416aa65bc83d58b888b82c3220e1f0b73014104194b3f8aa08b96cae19b14bd6c32a92364bea3051cb9f018b03e3f09a57208ff058f4b41ebf96b9911066aef3be22391ac59175257af0984d1432acb8f2aefcaffffffff0340420f00000000001976a914c0fbb13eb10b57daa78b47660a4ffb79c29e2e6b88ac204e0000000000001976a9142cae94ffdc05f8214ccb2b697861c9c07e3948ee88ac1c2e0100000000001976a9146e03561cd4d6033456cc9036d409d2bf82721e9888ac00000000".hex2Bytes.buffer
    }
    
    // From BIP-143
    var testSignature1UnsignedTxBytes: ByteBuffer = "01000000000102fff7f7881a8099afa6940d42d1e7f6362bec38171ea3edf433541db4e4ad969f00000000494830450221008b9d1dc26ba6a9cb62127b02742fa9d754cd3bebf337f7a55d114c8e5cdd30be022040529b194ba3f9281a99f2b1c0a19c0489bc22ede944ccf4ecbab4cc618ef3ed01eeffffffef51e1b804cc89d182d279655c3aa89e815b1b309fe287d9b2b55d57b90ec68a0100000000ffffffff02202cb206000000001976a9148280b37df378db99f66f85c95a783a76ac7a6d5988ac9093510d000000001976a9143bde42dbee7e4dbe6a21b2d50ce2f0167faa815988ac000247304402203609e17b84f6a7d30c80bfa610b5b4542f32a8a0d5447a12fb1366d7f01cc44a0220573a954c4518331561406f90300e8f3358f51928d43c212a8caed02de67eebee0121025476c2e83188368da1ff3e292e7acafcdb3566bb0ad253f62fc70f07aeee635711000000".hex2Bytes.buffer
    let testSignature1: [UInt8] = "304402203609e17b84f6a7d30c80bfa610b5b4542f32a8a0d5447a12fb1366d7f01cc44a0220573a954c4518331561406f90300e8f3358f51928d43c212a8caed02de67eebee"
    let testSignature1InputIndex: Int = 1
    let testSignature1Amount: UInt64 = 600000000
    let testSignature1PrivateKey = Scalar("619c335025c7f4012e556c2a58b2506e30b8511b53ade95ea316fd8c3286feb9")
    
    var testSignature2UnsignedTxBytes: ByteBuffer = "01000000000101db6b1b20aa0fd7b23880be2ecbd4a98130974cf4748fb66092ac4d3ceb1a5477010000001716001479091972186c449eb1ded22b78e40d009bdf0089feffffff02b8b4eb0b000000001976a914a457b684d7f0d539a46a45bbc043f35b59d0d96388ac0008af2f000000001976a914fd270b1ee6abcaea97fea7ad0402e8bd8ad6d77c88ac02473044022047ac8e878352d3ebbde1c94ce3a10d057c24175747116f8288e5d794d12d482f0220217f36a485cae903c713331d877c1f64677e3622ad4010726870540656fe9dcb012103ad1d8e89212f0b92c74d23bb710c00662ad1470198ac48c43f7d6f93a2a2687392040000".hex2Bytes.buffer
    let testSignature2: [UInt8] = "3044022047ac8e878352d3ebbde1c94ce3a10d057c24175747116f8288e5d794d12d482f0220217f36a485cae903c713331d877c1f64677e3622ad4010726870540656fe9dcb"
    let testSignature2InputIndex: Int = 0
    let testSignature2Amount: UInt64 = 1000000000
    let testSignature2PrivateKey = Scalar("eb696a065ef48a2192da5b28b694f87544b30fae8327c4510137a922f32c6dcf")
    
    func testDecodeEncode() {
        let save1 = self.vector1!
        let t1 = Tx.Transaction(fromBuffer: &self.vector1)!
        var encode1 = self.allocator.buffer(capacity: save1.capacity)
        t1.write(to: &encode1)
        XCTAssertEqual(encode1, save1)
        
        let save2 = self.vector2!
        let t2 = Tx.Transaction(fromBuffer: &self.vector2)!
        var encode2 = self.allocator.buffer(capacity: save2.capacity)
        t2.write(to: &encode2)
        XCTAssertEqual(encode2, save2)

        let save3 = self.vector3!
        let t3 = Tx.Transaction(fromBuffer: &self.vector3)!
        var encode3 = self.allocator.buffer(capacity: save3.capacity)
        t3.write(to: &encode3)
        XCTAssertEqual(encode3, save3)
    }
    
    func testWithWithness() {
        var transaction = Tx.Transaction(fromBuffer: &self.withWitness)
        XCTAssertNotNil(transaction)
        XCTAssertEqual(transaction?.hasWitnesses, true)
        XCTAssertEqual(transaction?.vin.count, 2)
        XCTAssertEqual(transaction?.vout.count, 2)
        XCTAssertEqual(transaction?.coreSize, 264)
        XCTAssertEqual(transaction?.witnessSize, 111)
        XCTAssertEqual(transaction?.locktime, .enable(2267))
        
        if let input0 = transaction?.vin[0], let input1 = transaction?.vin[1] {
            XCTAssertEqual(input0.hasWitness, true)
            XCTAssertEqual(input0.witness?.witnessField.count, 2)
            XCTAssertEqual(input1.hasWitness, false)
            XCTAssertNil(input1.witness)
        } else {
            XCTFail()
        }
        
        XCTAssertThrowsError(try transaction?.validatedView())
        XCTAssertNoThrow(try transaction?.hash())
        XCTAssertThrowsError(try transaction?.validatedView())
        XCTAssertNoThrow(try transaction?.validate())
        let id = try? transaction?.validatedView()
        XCTAssertNotNil(id)
        XCTAssertNotEqual(id?.txId.asWtxId, id?.wtxId)
        XCTAssertEqual(id?.txId, "99e7484eafb6e01622c395c8cae7cb9f8822aab6ba993696b39df8b60b0f4b11".hex2Hash())
    }
    
    func testWithoutWitness() {
        var transaction = Tx.Transaction(fromBuffer: &self.withoutWitness)
        XCTAssertNotNil(transaction)
        XCTAssertEqual(transaction?.hasWitnesses, false)
        XCTAssertEqual(transaction?.vin.count, 3)
        XCTAssertEqual(transaction?.vout.count, 3)
        XCTAssertEqual(transaction?.coreSize, 652)
        XCTAssertEqual(transaction?.witnessSize, 0)
        XCTAssertEqual(transaction?.locktime, .disable(0))
        
        transaction?.vin.forEach {
            XCTAssertEqual($0.hasWitness, false)
            XCTAssertNil($0.witness)
        }
        
        XCTAssertThrowsError(try transaction?.validatedView())
        XCTAssertNoThrow(try transaction?.hash())
        XCTAssertThrowsError(try transaction?.validatedView())
        XCTAssertNoThrow(try transaction?.validate())
        let id = try? transaction?.validatedView()
        XCTAssertNotNil(id)
        XCTAssertEqual(id?.txId, "38d4cfeb57d6685753b7a3b3534c3cb576c34ca7344cd4582f9613ebf0c2b02a".hex2Hash())
        XCTAssertEqual(id?.txId.asWtxId, id?.wtxId)
        
        XCTAssertEqual(transaction?.verifySignature(index: 0, prevouts: self.withoutWitnessPrevouts), true)
        XCTAssertEqual(transaction?.verifySignature(index: 0, prevouts: self.withoutWitnessPrevouts.reversed()), false)
        XCTAssertEqual(transaction?.verifySignature(index: 1, prevouts: self.withoutWitnessPrevouts), true)
        XCTAssertEqual(transaction?.verifySignature(index: 2, prevouts: self.withoutWitnessPrevouts), true)
        XCTAssertEqual(transaction?.verifySignature(index: 2, prevouts: self.withoutWitnessPrevouts.reversed()), false)
    }
    
    func testWitnessWeightLimit() {
        TestingHelpers.diSetup {
            $0.PROTOCOL_SEGWIT_WEIGHT_LIMIT = 1056
        }
        
        var transaction = Tx.Transaction(fromBuffer: &self.withWitness)
        XCTAssertNotNil(transaction)
        XCTAssertNoThrow(try transaction?.hash())
        XCTAssertThrowsError(try transaction?.validate()) {
            XCTAssertTrue($0 is Tx.Transaction.TransactionError)
        }
    }
    
    func testWitnessVerifySignature1() {
        let tx: Tx.Transaction! = Tx.Transaction(fromBuffer: &self.testSignature1UnsignedTxBytes)
        XCTAssertNotNil(tx)
        
        guard let v0Signature = tx.vin[self.testSignature1InputIndex].v0Signature,
              let testSignature = DSA.Signature(from: self.testSignature1)
        else {
                XCTFail()
                return
        }

        XCTAssertEqual(v0Signature.signature, testSignature)
        XCTAssertEqual(v0Signature.sigHashType, .ALL)
        XCTAssertEqual(v0Signature.publicKey, DSA.SecretKey(self.testSignature1PrivateKey).pubkey())
        let publicKeyHash = PublicKeyHash(v0Signature.publicKey)
        let prevouts = [ Tx.Out(value: 0, scriptPubKey: []),
                         Tx.Out(value: self.testSignature1Amount,
                                scriptPubKey: publicKeyHash.scriptPubKeyWPKH) ]
        XCTAssertTrue(tx.verifySignature(index: self.testSignature1InputIndex, prevouts: prevouts))

        let sigHash = Tx.Signature.sigHash(tx: tx,
                                           signatureType: v0Signature.sigHashType,
                                           inputIndex: self.testSignature1InputIndex,
                                           amount: self.testSignature1Amount,
                                           outpointPublicKeyHash: publicKeyHash)
        XCTAssertTrue(v0Signature.publicKey.verify(signature: v0Signature.signature,
                                                   message: Array(sigHash.littleEndian)))
    }

    func testWitnessVerifySignature2() {
        let tx: Tx.Transaction! = Tx.Transaction(fromBuffer: &self.testSignature2UnsignedTxBytes)
        XCTAssertNotNil(tx)

        guard let v0Signature = tx.vin[self.testSignature2InputIndex].v0Signature,
              let testSignature = DSA.Signature(from: self.testSignature2)
        else {
                XCTFail()
                return
        }

        XCTAssertEqual(v0Signature.signature, testSignature)
        XCTAssertEqual(v0Signature.sigHashType, .ALL)
        XCTAssertEqual(v0Signature.publicKey, DSA.SecretKey(self.testSignature2PrivateKey).pubkey())
        let publicKeyHash = PublicKeyHash(v0Signature.publicKey)
        let prevouts = [ Tx.Out(value: self.testSignature2Amount, scriptPubKey: publicKeyHash.scriptPubKeyWPKH) ]
        XCTAssertTrue(tx.verifySignature(index: self.testSignature2InputIndex, prevouts: prevouts))

        let sigHash = Tx.Signature.sigHash(tx: tx,
                                           signatureType: v0Signature.sigHashType,
                                           inputIndex: self.testSignature2InputIndex,
                                           amount: self.testSignature2Amount,
                                           outpointPublicKeyHash: publicKeyHash)
        XCTAssertTrue(v0Signature.publicKey.verify(signature: v0Signature.signature, message: Array(sigHash.littleEndian)))
    }
    
    func testFindSpent() {
        var buffer = self.loadTestTransaction4()
        let tx = Tx.Transaction(fromBuffer: &buffer)!
        
        let outpoint1: Tx.Outpoint = .init(
            transactionId: .little("fff7f7881a8099afa6940d42d1e7f6362bec38171ea3edf433541db4e4ad969f".hex2Bytes),
            index: 0
        )
        let outpoint2: Tx.Outpoint = .init(
            transactionId: .little("ef51e1b804cc89d182d279655c3aa89e815b1b309fe287d9b2b55d57b90ec68a".hex2Bytes),
            index: 1
        )
        let zero = Tx.Outpoint(transactionId: .zero, index: .max)
    
        let outpoints: Outpoints = Set([ outpoint1, outpoint2, zero ])
        
        let found = tx.findSpent(for: outpoints)
        XCTAssertEqual(found.count, 2)
        XCTAssert(found.contains(outpoint1))
        XCTAssert(found.contains(outpoint2))
        XCTAssertFalse(found.contains(zero))
        
        var buffer2 = self.loadTestTransaction1()
        let tx2 = Tx.Transaction(fromBuffer: &buffer2)!
        XCTAssert(tx2.findSpent(for: outpoints).isEmpty)
    }
    
    func testFindFunding() {
        var buffer = self.loadTestTransaction4()
        guard var tx = Tx.Transaction(fromBuffer: &buffer)
        else {
            XCTFail()
            return
        }
        XCTAssertNoThrow(try tx.hash())

        let opcodes1 = "76a9148280b37df378db99f66f85c95a783a76ac7a6d5988ac".hex2Bytes
        let scriptPubKey1 = ScriptPubKey(
            tag: 11,
            index: 1,
            opcodes: opcodes1
        )
        let opcodes2 = "76a9143bde42dbee7e4dbe6a21b2d50ce2f0167faa815988ac".hex2Bytes
        let scriptPubKey2 = ScriptPubKey(
            tag: 12,
            index: 1,
            opcodes: opcodes2
        )
        let miss = ScriptPubKey(tag: 99, index: 99, opcodes: [ 1, 2, 3, ])
        
        let pubKeys = Set([ scriptPubKey1, scriptPubKey2, miss, ])
        let funding = tx.findFunding(for: pubKeys)
        
        XCTAssertEqual(funding.count, 2)
        
        guard let pk1 = funding.first(where: { $0.scriptPubKey.opcodes == opcodes1 }),
              let pk2 = funding.first(where: { $0.scriptPubKey.opcodes == opcodes2 })
        else {
            XCTFail()
            return
        }
        
        XCTAssertEqual(pk1.amount, 112340000)
        XCTAssertEqual(pk1.scriptPubKey.tag, 11)
        XCTAssertEqual(pk1.outpoint,
                       Tx.Outpoint(
                        transactionId: "e8151a2af31c368a35053ddd4bdb285a8595c769a3ad83e0fa02314a602d4609".hex2Hash(),
                        index: 0))
        
        XCTAssertEqual(pk2.amount, 223450000)
        XCTAssertEqual(pk2.scriptPubKey.tag, 12)
        XCTAssertEqual(pk2.outpoint,
                       Tx.Outpoint(
                        transactionId: "e8151a2af31c368a35053ddd4bdb285a8595c769a3ad83e0fa02314a602d4609".hex2Hash(),
                        index: 1))
        
        var buffer2 = self.loadTestTransaction1()
        let tx2 = Tx.Transaction(fromBuffer: &buffer2)!
        XCTAssert(tx2.findFunding(for: pubKeys).isEmpty)
    }
}
