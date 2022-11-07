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
import NodeTestLibrary
import NIO
import XCTest

final class BlockTests: XCTestCase {
    var savedSettings: NodeSettings!
    var allocator: ByteBufferAllocator!

    override func setUp() {
        self.savedSettings = Settings
        TestingHelpers.diSetup()

        self.allocator = ByteBufferAllocator()
    }
    
    override func tearDown() {
        self.allocator = nil
        
        Settings = self.savedSettings
        self.savedSettings = nil
    }
    
    func loadTestBlock1() -> ByteBuffer {
        "00000020a15e218f5f158a31053ea101b917a6113c807f6bcdc85a000000000000000000cc7cf9eab23c2eae050377375666cd7862c1dfeb81abd3198c3a3f8e045d91484a39225af6d00018659e5e8a0101000000010000000000000000000000000000000000000000000000000000000000000000ffffffff64030096072cfabe6d6d08d1c2f6d904f4e1cd10c6558f8e5aed5d6a89c43bb22862464ebb819dd8813404000000f09f909f104d696e6564206279206a6f73656d7372000000000000000000000000000000000000000000000000000000000000000000007f06000001807c814a000000001976a914c825a1ecf2a6830c4401620c3a16f1995057c2ab88acefebcf38".hex2Bytes.buffer
    }
    
    func loadSegwitBlock1() -> ByteBuffer {
        "0000002046a46141a390285dffb1ab4496eb71cac27f410d0c88051f244db0f600000000f7f28e938949cfdfff01f6a9a0ef310493d551764b30d9158ac6edb7c01f5cc34f585d5985e4071a46ab3cce08010000000001010000000000000000000000000000000000000000000000000000000000000000ffffffff450307931100044f585d59046462f61f0cfe4d5b598e550100000000000a636b706f6f6c212f6d696e65642062792077656564636f646572206d6f6c69206b656b636f696e2fffffffff02f9355109000000001976a91427f60a3b92e8a92149b18210457cc6bdc14057be88ac0000000000000000266a24aa21a9ed1a0e07935a4fe9fd2bed46a957662285c57a1a2f13b727349704594030e020aa0120000000000000000000000000000000000000000000000000000000000000000000000000010000000347067f25ba7e4ae320f2a6c073b505b00b326d3d7bdca3c10ef2f1caf2b11e4a020000006a4730440220186efd552a3330203a0cf3763fb4fb11628877859397a6ad4336de194319a73a02202443f498e8e7067394bbbb1eddb82fc12360666832fc11404b4640ba94abf49601210297941338d1fff1e3c07544f5555d504a54de1efb31ed7b5cc3497e5429eba84effffffff47067f25ba7e4ae320f2a6c073b505b00b326d3d7bdca3c10ef2f1caf2b11e4a010000006a4730440220469cdadf898206a305576030d58fddc800a55c6dbe7a3d2f0e48b755694d8f36022044a14bd69fa367d4f6594f66cb27f43adecdfa83b8b7bd32910d5af6516b5d23012102ba7387e34a7958414a2ffba513c84b655bfbee01170cfe1ed71ac073a819cb94ffffffff47067f25ba7e4ae320f2a6c073b505b00b326d3d7bdca3c10ef2f1caf2b11e4a030000006b4830450221008ff046bb703828b81c01dc1678e288f09d06ad775172e103b740c5942989bd430220103b0a55f45a25c2018af333fc478d8a7041d5c00c6fcdb8bf72db8eca6b9ca00121029f211728da9807b9c14b07ab5c82e9f146c7f9b8209e70c07c0dc3b83d51e9faffffffff05ce5605000000000017a9148f2c34a23c46e980b79e109b11a2c82d3992eb9587b2ce2b00000000001976a914abcb854975b1bde317703d9a49e93188231b9e4d88ac51c68e07000000001976a91491819d7cd7ca5fce1d8dd28e73cc1500b457d75f88ac48762202000000001976a914160eb7771a27f94d1194477d2034efdf5bd2e40288ac0000000000000000166a14c8f9ef162723a5feb42de56f8800db0f08c5fcf400000000010000000147da7a8f15e6437226142f4802c3b94674d56d8bc070176ce38231a91643b7ac01000000da00483045022100d139e93a35858b3318d0c50f2edef19b48ef3e6ae8147da5ced6688c642e4eeb022039d66a711612e2ba32f188c8b9a6cda40710f0364a67f7d45a750dd1f5709ce3014730440220400334a4dd27b49ad2c5a822b00c1723ca5108f4fcb33e6451d581161cf093fb02200c1d1e27ec5fa00dd1040e97d09dffc1e4a3ea42e887f3076741af0f1eb6225f0147522102632178d046673c9729d828cfee388e121f497707f810c131e0d3fc0fe0bd66d62103a0951ec7d3a9da9de171617026442fcd30f34d66100fab539853b43f508787d452aeffffffff0240420f000000000017a914385c058b7f45f692d2a2a195988bd202863ffbd187fdde836a0000000017a9148ce5408cfeaddb7ccb2545ded41ef4781094548487000000000100000001f07b584b50a26d05f9e5a60f3d71f0a3f45307adb3bb04dce4f763c797662e5001000000da00483045022100c05d1a3da5771af8d47806cad52e16d8b9e0033cbebbd56c86298810bc2424f60220252e6019376c9cfaa95874f1be09b42e388965f2ea3997aa235fc6c0a7241e680147304402202190cc322186e2a2a9bb7271fce3a01ca889b5e5024144138e8f347fbdd5a80402207f0d8cbd6b1ef531200ab6cdf1d6dee5dad422dd656c67b4d7575c3e0f4cd2710147522102632178d046673c9729d828cfee388e121f497707f810c131e0d3fc0fe0bd66d62103a0951ec7d3a9da9de171617026442fcd30f34d66100fab539853b43f508787d452aeffffffff0240420f000000000017a9142e81a820a896b0467d53186a8bbd84c30097d95587f139096a0000000017a9148ce5408cfeaddb7ccb2545ded41ef478109454848700000000010000000170f50556ff8488858f4e1465400b5f432c7ab31791c5094d2a56aaad83785962010000006a473044022069efea6cd50956553ce379ac6ae316aace23ae5f7079f3c6cc99ed122223c3270220064a073151ecf036d8b68e05b51b088d1f2a8c7b14424e863023602b9df127cc01210382728f9fa7906eee57d3f65560352e05b2d8736b4abdd5edb7c68daf80d97a27ffffffff02a8070f00000000001976a914f4e9c3d0eef8bcaccf6432a7036946e481483c7b88ac20a10700000000001976a91422446fe717de8a26807e1f879453c78f72ff9a4d88ac000000000100000001fe75343e39e80e5a2008680cef89fa66a444493b4b1845e79d980bb2322518e800000000fd670100483045022100ecce2c784dea2acba3ed6c8326d847df0063f3fac39de4debc9b0b46bfb3510202207f86610036617723f7343fc64631a08def939fbb51dbae7c540db48dca342c760147304402203069df45727a89596a2bd9fe1c625c24dc6a98ab049304f3ce0ffde0a5f72a1902207b909cea57110b3bf7490cf714e321f682a93683f23218d536f45a450f7d88db0147304402201726924593d783c6ba716376db9859545ac819a9c11f003ff63937c2efe9c90c02203b144284bd16d3ba2064636be196457d05c73cdd814dc772b0c8df2a86811a29014c8b53210345b94ff1ad5014575d4e0a1f298f741914fc56d9f309a39ae4f32691ae759936210358a91c70663e712953ec1285a2053fbebeee27f969c04aee669e2a00f8d8eee1210205309f2ea1930bccd1f87be765286d90f1afb8f3b2a93409ee269bb50a25adfd21031ced6a7a04be43ff21234617196ec0bce14e4973d229daa1277cdf8e0749a33354aeffffffff02008dfd060000000017a91404069c70bcd3b5051557cb1904e4cce91734cde2870000000000000000326a3045584f4e554d01006cb209000000000074d68f3b156206dec15dfc6d1852f136b90c9bed93f4db9c7430c898e057c73c0000000001000000018cfdd8f8ebb1d5557fe7859590e1ffb048faec8d6ccbb523f692e78edd4c932000000000fd680100483045022100da695643c2985d73f92e4761c16b7b47811db5092e7a50f2fc89b5c1a4f00ee90220521311fb303b0a544ef3947f276c9827f38e9ba9d878b8c5f950aca94725257901483045022100f63861d5ada73c808c60813b083422c6108fb1a04889f9c3013970f28daa51630220131189d3c3a1b4e5820b6775b07248896b2681c32afbac6d7f4a6f9eec7a0729014730440220649e07986dbae47e70c56d86f57625fa04bfb72a2645f692f5f045e753fcf23a02206aa7d494f8d6a5f09d193ba46e0fc5ed666bbda02073ee0bceb92bfab5fd9665014c8b5321022f4ffcf47cea7f941f7810700a2f4d06849f1d8641a6f65aaf43f8902dc0fa9621028713f5751272826a9a691da00f68f25a1f90dfa0cde3803dfdf19fc627d96ce221035a3404eceb34e42bdcbcfe28d480254b75996b8ef2e2e02d61056c7b46f831722103ef2f6002b7898eb55d1a094fb2c18fc665da2d7e206eb0665abf2ff47be002fc54aeffffffff02b0b292000000000017a9140a12ad889aecc8f6213bfd6bd47911cab1c30e5f870000000000000000326a3045584f4e554d0100c4f70200000000007003437104483e73adab434ca590ec2505051e90a56478d4e2dcdf3b7dd6e5a200000000010000000136b1a2860e8ae6b16c2735b4b84db5f14e5645c99ac389b66ab5dd372dd18afa00000000fd680100473044022038242a0e59184cdeeb40f081838819fac34b007f55e5f279b9948155f46ba68f02200546f705209d6b82894f14aeaf4a22b9670f98e18db19ce566f5533e56f89fb401483045022100f35a1dd53dcc90027b023cf3b64f7a8514d968d389aeed30473391f5102b0a5a022018bf91668be7b49ee89790fc23bee8386a6605b317d5a5952a2957fcab5c53eb0148304502210082ba3dc5980b9c9cde1d75bb6dd44fc8275739492f599fd9d599b58e48c25c5002202577637b7cc659fe9b6a0453c2ad9ee1e68cb207f681656b287af460c87815bc014c8b5321022f4ffcf47cea7f941f7810700a2f4d06849f1d8641a6f65aaf43f8902dc0fa9621028713f5751272826a9a691da00f68f25a1f90dfa0cde3803dfdf19fc627d96ce221035a3404eceb34e42bdcbcfe28d480254b75996b8ef2e2e02d61056c7b46f831722103ef2f6002b7898eb55d1a094fb2c18fc665da2d7e206eb0665abf2ff47be002fc54aeffffffff02c8ae92000000000017a9140a12ad889aecc8f6213bfd6bd47911cab1c30e5f870000000000000000326a3045584f4e554d0100b8f9020000000000caf2ccaf424d4f2406bc55aca385eae9408c188e4bf131ac576b6078cb3c222700000000".hex2Bytes.buffer
    }
    
    func loadEmptyBlock1() -> ByteBuffer {
        "02000000c21dcee61ecc6757e190ca5e9b1b3915e8c3ba9aaad354160000000000000000e228146c296b3cdf5b9122285fec95f3685e9fabc59910476a35d958e44d2257177fa654ca0d1b18757219c80101000000010000000000000000000000000000000000000000000000000000000000000000ffffffff5a03d02405e4b883e5bda9e7a59ee4bb99e9b1bcfabe6d6d7673b9b5ccae8c911de84d8fe714c32ca0a2a1772c0d7a31466157aa06c6c4661000000000000000011c5a1ec87202004d696e656420627920616e6479353138303338ffffffff0100f90295000000001976a914c825a1ecf2a6830c4401620c3a16f1995057c2ab88ac00000000".hex2Bytes.buffer
    }
    
    func testDecodeEncodeBlock1() {
        var blockBuffer = self.loadTestBlock1()
        let save = blockBuffer
        let decode = BlockCommand(fromBuffer: &blockBuffer)
        XCTAssertNotNil(decode)
        if let d = decode {
            var buffer = self.allocator.buffer(capacity: blockBuffer.capacity)
            d.write(to: &buffer)
            XCTAssertEqual(buffer, save)
        } else {
            XCTFail()
        }
    }

    func testDecodeEncodeSegwitBlock1() {
        var blockBuffer = self.loadSegwitBlock1()
        let save = blockBuffer
        let decode = BlockCommand(fromBuffer: &blockBuffer)
        XCTAssertNotNil(decode)
        if let d = decode {
            var buffer = self.allocator.buffer(capacity: blockBuffer.capacity)
            d.write(to: &buffer)
            XCTAssertEqual(buffer, save)
        } else {
            XCTFail()
        }
    }
    
    func verifyBlock(for buffer: inout ByteBuffer) {
        let save = buffer
        guard var header = BlockHeader(fromBuffer: &buffer) else {
            XCTFail()
            return
        }
        buffer = save

        XCTAssertNoThrow(try header.addBlockHash())
        var block = BlockCommand(fromBuffer: &buffer)
        XCTAssertNotNil(block)
        let saveBlock = block
        XCTAssertNoThrow(try block?.verify(hash: try header.blockHash()))
        block = saveBlock
        XCTAssertNoThrow(try block?.verify(header: header))
    }
    
    func testVerifyBlock() {
        var blockBuffer = self.loadTestBlock1()
        self.verifyBlock(for: &blockBuffer)
    }
    
    func testVerifySegwitBlock() {
        var blockBuffer = self.loadSegwitBlock1()
        self.verifyBlock(for: &blockBuffer)
    }
    
    func testVeriftEmptyBlock() throws {
        var blockBuffer = self.loadEmptyBlock1()
        self.verifyBlock(for: &blockBuffer)
    }
    
    func testTamperedBlockHash() {
        var segwitBlockBuffer = self.loadSegwitBlock1()
        var segwitBlock = BlockCommand(fromBuffer: &segwitBlockBuffer)
        XCTAssertNotNil(segwitBlock)
        XCTAssertThrowsError(try segwitBlock?.verify(hash: .zero)) {
            XCTAssertTrue($0 is BlockCommand.BlockError)
        }
    }
    
    func testTamperedBlockHeader() {
        var blockBuffer = self.loadSegwitBlock1()
        let saveBlockBuffer = blockBuffer
        blockBuffer.withVeryUnsafeMutableBytes {
            $0[2] = 1
        }
        guard var header = BlockHeader(fromBuffer: &blockBuffer) else {
            XCTFail()
            return
        }
        blockBuffer = saveBlockBuffer

        XCTAssertNoThrow(try header.addBlockHash())
        
        var block = BlockCommand(fromBuffer: &blockBuffer)
        XCTAssertNotNil(block)
        XCTAssertThrowsError(try block?.verify(header: header)) {
            XCTAssertTrue($0 is BlockCommand.BlockError)
        }
    }
    
    func testTamperedMerkleRoot() {
        var badBlockBuffer = self.loadTestBlock1()
        badBlockBuffer.withVeryUnsafeMutableBytes {
            $0[36] = 0xcd
        }
        let saveBadBlockBuffer = badBlockBuffer
        guard var header = BlockHeader(fromBuffer: &badBlockBuffer) else {
            XCTFail()
            return
        }
        badBlockBuffer = saveBadBlockBuffer
        
        XCTAssertNoThrow(try header.addBlockHash())
        
        var badBlock = BlockCommand(fromBuffer: &badBlockBuffer)
        XCTAssertNotNil(badBlock)
        let saveBlock = badBlock
        XCTAssertThrowsError(try badBlock?.verify(hash: try header.blockHash())) {
            XCTAssertTrue($0 is BlockCommand.BlockError)
        }
        badBlock = saveBlock
        XCTAssertThrowsError(try badBlock?.verify(header: header)) {
            XCTAssertTrue($0 is BlockCommand.BlockError)
        }
    }

    func testTamperedMerkleRootSegwit() {
        var badBlockBuffer = self.loadSegwitBlock1()
        badBlockBuffer.withVeryUnsafeMutableBytes {
            $0[36] = 0xf8
        }
        let saveBadBlockBuffer = badBlockBuffer
        guard var header = BlockHeader(fromBuffer: &badBlockBuffer) else {
            XCTFail()
            return
        }
        badBlockBuffer = saveBadBlockBuffer
        
        XCTAssertNoThrow(try header.addBlockHash())
        
        var badBlock = BlockCommand(fromBuffer: &badBlockBuffer)
        XCTAssertNotNil(badBlock)
        let saveBlock = badBlock
        XCTAssertThrowsError(try badBlock?.verify(hash: try header.blockHash())) {
            XCTAssertTrue($0 is BlockCommand.BlockError)
        }
        badBlock = saveBlock
        XCTAssertThrowsError(try badBlock?.verify(header: header)) {
            XCTAssertTrue($0 is BlockCommand.BlockError)
        }
    }

    func testTamperedCommitmentHash() {
        var badBlockBuffer = self.loadSegwitBlock1()
        badBlockBuffer.withVeryUnsafeMutableBytes {
            $0[274] = 1
        }
        let saveBadBlockBuffer = badBlockBuffer
        guard var header = BlockHeader(fromBuffer: &badBlockBuffer) else {
            XCTFail()
            return
        }
        badBlockBuffer = saveBadBlockBuffer
        
        XCTAssertNoThrow(try header.addBlockHash())
        
        var badBlock = BlockCommand(fromBuffer: &badBlockBuffer)
        XCTAssertNotNil(badBlock)
        let saveBlock = badBlock
        XCTAssertThrowsError(try badBlock?.verify(hash: try header.blockHash())) {
            XCTAssertTrue($0 is BlockCommand.BlockError)
        }
        badBlock = saveBlock
        XCTAssertThrowsError(try badBlock?.verify(header: header)) {
            XCTAssertTrue($0 is BlockCommand.BlockError)
        }
    }
    
    func testTamperedWitnessReservedValue() {
        var badBlockBuffer = self.loadSegwitBlock1()
        badBlockBuffer.withVeryUnsafeMutableBytes {
            $0[283] = 1
        }
        let saveBadBlockBuffer = badBlockBuffer
        guard var header = BlockHeader(fromBuffer: &badBlockBuffer) else {
            XCTFail()
            return
        }
        badBlockBuffer = saveBadBlockBuffer
        
        XCTAssertNoThrow(try header.addBlockHash())
        
        var badBlock = BlockCommand(fromBuffer: &badBlockBuffer)
        XCTAssertNotNil(badBlock)
        let saveBlock = badBlock
        XCTAssertThrowsError(try badBlock?.verify(hash: try header.blockHash())) {
            XCTAssertTrue($0 is BlockCommand.BlockError)
        }
        badBlock = saveBlock
        XCTAssertThrowsError(try badBlock?.verify(header: header)) {
            XCTAssertTrue($0 is BlockCommand.BlockError)
        }
    }

    func testLegacySizeLimit() {
        TestingHelpers.diSetup {
            $0.PROTOCOL_LEGACY_SIZE_LIMIT = 1
        }

        var blockBuffer = self.loadTestBlock1()
        let saveBlockBuffer = blockBuffer
        guard var header = BlockHeader(fromBuffer: &blockBuffer) else {
            XCTFail()
            return
        }
        blockBuffer = saveBlockBuffer

        XCTAssertNoThrow(try header.addBlockHash())
        
        var block = BlockCommand(fromBuffer: &blockBuffer)
        XCTAssertNotNil(block)
        XCTAssertThrowsError(try block?.verify(hash: try header.blockHash())) {
            XCTAssertTrue($0 is BlockCommand.BlockError)
        }
    }

    func testWitnessWeightLimit() {
        TestingHelpers.diSetup {
            $0.PROTOCOL_SEGWIT_WEIGHT_LIMIT = 2469
        }
        
        var blockBuffer = self.loadSegwitBlock1()
        let saveBlockBuffer = blockBuffer
        guard var header = BlockHeader(fromBuffer: &blockBuffer) else {
            XCTFail()
            return
        }
        blockBuffer = saveBlockBuffer

        XCTAssertNoThrow(try header.addBlockHash())
        
        var block = BlockCommand(fromBuffer: &blockBuffer)
        XCTAssertNotNil(block)
        XCTAssertThrowsError(try block?.verify(hash: try header.blockHash())) {
            XCTAssertTrue($0 is BlockCommand.BlockError)
        }
    }
}
