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
import Stream64
import HaByLo
import NodeTestLibrary
import NIO
import XCTest
import FastrangeSipHash


final class CompactFiltersTests: XCTestCase {
    var savedSettings: NodeSettings!
    var gcsClient: GolombCodedSetClient!
    
    override func setUp() {
        self.savedSettings = Settings
        self.gcsClient = Settings.BITCOIN_CF_GOLOMB_CODED_SET_CLIENT()
    }
    
    override func tearDown() {
        Settings = self.savedSettings
        self.savedSettings = nil
        self.gcsClient = nil
    }

    func testTestnetBlock1938592() {
        let bytes = "fdb6018c9a98c944ed77a51bc88d131af57614af061f9c81474d7a2713fd20dd2f04104b40fd854b9a555f1eb1371b6399569815a3a74081adb0f37e20035e34eefd73695b46d5bf1caf8bd74f38d8def32e6222a2ce88d0707bb9e1c07b6beb9096121debb94971c15956abafdeaa7661c88b170ebffe8fdc091ef78a5ed8e02b83bc297701f2a1e9ccea3acd0f54044ac98dc1541ef26a260e4dc40aff11d7c6cc4c43a6ba6a0346d519ad3ce8bbc22ff62063ac35296a2e1ba1b9af11e68e15766213142ced336e32d24f9ae5a775c0972ff0da5ec80618fe2c346e8ba2ad7ee2038454dc237890e461c6c6adac3f95b0d17c823695177b9e0e73c16bd19b770cdb39ae324b988ac950aa018ef0a5a97bcf00cda816c946fde2471494d55d5b330b2d96a61eadb9814ea871a7bfb7096a3056bb68614321812012841e6b8dcb212f815299d7309da67e9e444b7e15ab19057f8a9dcb64f74ebda70eb8ad3ae809ac79fb5a1cd89cbec98d63ddfaa2ed7dd3152164c8d811683039598d52171bcbed96b8a138ed244740c8bbac664626079efee7be258b69c3b10ddbb91fa853162853b4609617a704a281aa8a27e603a52d48b45c58ce3602ee39a4f1d7352c1593fbbfebb5817b17bb8f2c981e34c4e65f1da4caa5bf6d27302119ded8d49fa4a406af89e2d83ddef982bb40946e478683405ee217c080d51dbcbbd69283235f616b503844d1944c83132d7a08c00a80888374abc16db5370d8a0e18cd147239201ff2327cd4d4c687d581e2d9a1aa795b25133cf372156e78746522d74404be32dd77dc8c0d4b8fee597f0a78b3595c00a55e58f955bb2c203063864c43d1a466a68a79fc5ccf5465907e35f84a646b97eb5a3a329eedf849cc671039a581c589cd3c4cc992e768ed3a1d617ff22690831c52741ed3e97bbd0d2fb94a4a01c9e04cb8cdd4dda871e932445cebc8500aa532a132719b68d34fd285e9e1367c7ae02d0098858d755796db8bba30c2c5d30b36a7277e464878450c6961ed24bec9912207f033404ec1f70e2c3229964d802dbef236133e83521f8277b495f82e9f5eeeb1346865ad593353949cdfac66009cdcf610d39005ea46bfc1678f6b14662f53d6d9bb55c68529cb3b99fe34ff462e51ad36552ec450fbe6980c83036b5f8988207e839a87f92e22e0d80407164b00a5800583e2f20f501fff82cc7b3df61606a7fa92c45be6774e7a442691ce5817311fe93e0368b4603b75a2c73735c75f2476e57764275926f8d37246e82f58cc06aa628f8b8c006822c361e4f7be5c01630ee9d651b37e1a9e003f1e02e8917a177ec4bb5ec124eeff15c462543e2c2ecea7efc7936b43fa5591dec88bd341c1b28be4195794236f3d95010e5f1119103fa2537b645d2cbb26109d7e4d2ec17349b2f4ee381a565aa3b24c097d5524d1c2f97a9027343c2cda76f7a1125f560b2efcb249cebb80ffe720c6e186db84ca4dac49b1995ae0bd0066a281eeb6b4a2ff04a9a93abc92cb979cd49cef93cb175916d2587c700e3aa7b413effc75a2afc420ccb630a74dcc561fb3c910299862a814e565d83601bce70d113adb9a290007eee06366205422b3a2c97213925870"
            .hex2Bytes
        let buffer = bytes.buffer
        let filterHash: BlockChain.Hash<CompactFilterHash> = .big("fde444eb0e51bb71df06d239fc71d15813718bb5b0235834ba094433d0475b43".hex2Bytes)
        var copy = buffer

        guard let count = copy.readVarInt().map(Int.init),
              let compressed = copy.readBytes(length: copy.readableBytes)
        else { XCTFail(); return }
        XCTAssertEqual(count, 438)

        copy.writeBytes(Array<UInt8>(repeating: 0, count: 7))
        let decoded = self.gcsClient.decode(compressed: compressed, n: count, p: 19)!
        XCTAssertEqual(.makeHash(from: bytes), filterHash)
        XCTAssertEqual(decoded.last, 343587834)
    }

    func testMatchTestVectors() {
        XCTAssertNoThrow(
            try Self.testVectors.forEach { height, vector in
                var blockBuffer = vector.blockData.buffer
                guard var block = BlockCommand(fromBuffer: &blockBuffer)
                else { XCTFail(); return }
                
                // Will in turn hash the header
                XCTAssertNoThrow(try block.verify(hash: vector.blockHash))
                
                var basicFilterBuffer = vector.basicFilter.buffer
                guard let n = basicFilterBuffer.readVarInt().map(Int.init),
                      let compressed = basicFilterBuffer.readBytes(length: basicFilterBuffer.readableBytes),
                      var hashedBlockHeader = try? block.getHeader() else {
                    XCTFail()
                    return
                }

                guard let _ = self.gcsClient.decode(compressed: compressed, n: n, p: 19)
                else { XCTFail(); return }
                
                let filterHash: BlockChain.Hash<CompactFilterHash> = .makeHash(from: vector.basicFilter)
                
                var cfilter = CF.CfilterCommand(
                    .wire(filterType: .basic,
                          blockHash: vector.blockHash,
                          filterHash: filterHash,
                          n: n,
                          compressed: compressed)
                )
                
                // unranked -> full header state
                XCTAssertNoThrow(try hashedBlockHeader.addHeight(height))
                
                // hashedBlockHeader is used for matching blockHash and then extracting block height from full blockHeader
                XCTAssertNoThrow(try cfilter.addRank(from: hashedBlockHeader))
                // Validates *filter* header
                XCTAssertNoThrow(try cfilter.validate(cfHeader: .init(filterHash: filterHash,
                                                                      previousHeaderHash: .big(vector.previousBasicHeader),
                                                                      headerHash: .big(vector.basicHeader))))
                
                guard let v = vector.prevOutputScripts.first, !v.isEmpty else {
                    return
                }
                
                let save = cfilter
                XCTAssertNoThrow(
                    XCTAssertTrue(try cfilter.isIncluded(vector.prevOutputScripts[0]))
                )
                
                XCTAssertNoThrow(
                    try vector.prevOutputScripts.forEach {
                        cfilter = save
                        XCTAssertTrue(try cfilter.isIncluded($0))
                    }
                )
                
                let jibberish = v + [ 0 ]
                XCTAssertNoThrow(
                    XCTAssertFalse(try cfilter.isIncluded(jibberish))
                )
            }
        )
    }
    
    func testDecodeEncode() {
        Self.testVectors.forEach { _, vector in
            var cfilterBuffer = vector.basicFilter.buffer
            guard let n = cfilterBuffer.readVarInt().map(Int.init),
                  let compressed = cfilterBuffer.readBytes(length: cfilterBuffer.readableBytes),
                  let decoded = self.gcsClient.decode(compressed: compressed, n: n, p: 19),
                  let encoded = self.gcsClient.encode(sorted: decoded, p: 19)
            else { XCTFail(); return }
            
            let nBytes: [UInt8] = .init(decoded.count.variableLengthCode)
            
            XCTAssertEqual(nBytes + encoded, vector.basicFilter)
        }
    }

    func testDecodeEncodeFromCfilterCommand() {
        Self.testVectors.forEach { _, vector in
            var buffer = Settings.BYTEBUFFER_ALLOCATOR.buffer(capacity: vector.basicFilter.count + 64)
            // Setup CFilter from test data
            buffer.writeInteger(CF.FilterType.basic.rawValue)
            buffer.writeBytes(vector.blockHash.littleEndian)
            buffer.writeBytes(vector.basicFilter.count.variableLengthCode)
            buffer.writeBytes(vector.basicFilter)

            let copy = buffer

            guard let filter = CF.CfilterCommand(fromBuffer: &buffer) else {
                XCTFail()
                return
            }

            var result = Settings.BYTEBUFFER_ALLOCATOR.buffer(capacity: vector.basicFilter.count + 64)
            filter.write(to: &result)
            XCTAssertEqual(copy.readableBytesView, result.readableBytesView)
        }
    }
}

extension CF.CfilterCommand {
    func isIncluded(_ opCode: [UInt8]) throws -> Bool {
        func cfHash(_ bytes: [UInt8], _ key: [UInt8], _ f: UInt64) -> UInt64 {
            let sipHash = siphash(input: bytes, key: key)
            return fastrange(sipHash, f)
        }
        
        func gcsMultiMatch(opcodeHashes: [UInt64], filters: [UInt64], key: [UInt8], f: UInt64) -> Bool {
            var index = opcodeHashes.startIndex
            
            outer:
            for value in filters {
                inner:
                for target in opcodeHashes[index...] {
                    if target == value {
                        return true
                    }
                    
                    if target > value {
                        break inner
                    }
                    
                    guard index < opcodeHashes.endIndex else {
                        break outer
                    }
                    
                    opcodeHashes.formIndex(after: &index)
                }
            }
            return false
        }

        let key = Array(try self.getBlockHash().littleEndian.prefix(16))
        let f = UInt64(self.n) * self.filterType.parameters.M
        let hash = cfHash(opCode, key, f)
        let gcsClient = GolombCodedSetClient.stream64
        let decoded = gcsClient.decode(compressed: self.compressed, n: self.n, p: 19)!
        
        return gcsMultiMatch(opcodeHashes: [hash], filters: decoded, key: key, f: f)
    }
}

extension CompactFiltersTests {
    struct TestVector {
        let blockHeight: Int
        let blockHash: BlockChain.Hash<BlockHeaderHash>
        let blockData: [UInt8]
        let prevOutputScripts: [[UInt8]]
        let previousBasicHeader: [UInt8]
        let basicFilter: [UInt8]
        let basicHeader: [UInt8]
        let notes: String
    }

    static var testVectors: [Int : TestVector] = [
        0 :
            .init(blockHeight: 0,
                  blockHash: "000000000933ea01ad0ee984209779baaec3ced90fa3f408719526f8d77f4943",
                  blockData: "0100000000000000000000000000000000000000000000000000000000000000000000003ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4adae5494dffff001d1aa4ae180101000000010000000000000000000000000000000000000000000000000000000000000000ffffffff4d04ffff001d0104455468652054696d65732030332f4a616e2f32303039204368616e63656c6c6f72206f6e206272696e6b206f66207365636f6e64206261696c6f757420666f722062616e6b73ffffffff0100f2052a01000000434104678afdb0fe5548271967f1a67130b7105cd6a828e03909a67962e0ea1f61deb649f6bc3f4cef38c4f35504e51ec112de5c384df7ba0b8d578a4c702b6bf11d5fac00000000",
                  prevOutputScripts: [[]],
                  previousBasicHeader: "0000000000000000000000000000000000000000000000000000000000000000",
                  basicFilter: "019dfca8",
                  basicHeader: "21584579b7eb08997773e5aeff3a7f932700042d0ed2a6129012b7d7ae81b750",
                  notes: "Genesis block"),
        2 :
            .init(blockHeight: 2,
                  blockHash: "000000006c02c8ea6e4ff69651f7fcde348fb9d557a06e6957b65552002a7820",
                  blockData: "0100000006128e87be8b1b4dea47a7247d5528d2702c96826c7a648497e773b800000000e241352e3bec0a95a6217e10c3abb54adfa05abb12c126695595580fb92e222032e7494dffff001d00d235340101000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0e0432e7494d010e062f503253482fffffffff0100f2052a010000002321038a7f6ef1c8ca0c588aa53fa860128077c9e6c11e6830f4d7ee4e763a56b7718fac00000000",
                  prevOutputScripts: [[]],
                  previousBasicHeader: "d7bdac13a59d745b1add0d2ce852f1a0442e8945fc1bf3848d3cbffd88c24fe1",
                  basicFilter: "0174a170",
                  basicHeader: "186afd11ef2b5e7e3504f2e8cbf8df28a1fd251fe53d60dff8b1467d1b386cf0",
                  notes: ""),
        3 :
            .init(blockHeight: 3,
                  blockHash: "000000008b896e272758da5297bcd98fdc6d97c9b765ecec401e286dc1fdbe10",
                  blockData: "0100000020782a005255b657696ea057d5b98f34defcf75196f64f6eeac8026c0000000041ba5afc532aae03151b8aa87b65e1594f97504a768e010c98c0add79216247186e7494dffff001d058dc2b60101000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0e0486e7494d0151062f503253482fffffffff0100f2052a01000000232103f6d9ff4c12959445ca5549c811683bf9c88e637b222dd2e0311154c4c85cf423ac00000000",
                  prevOutputScripts: [[]],
                  previousBasicHeader: "186afd11ef2b5e7e3504f2e8cbf8df28a1fd251fe53d60dff8b1467d1b386cf0",
                  basicFilter: "016cf7a0",
                  basicHeader: "8d63aadf5ab7257cb6d2316a57b16f517bff1c6388f124ec4c04af1212729d2a",
                  notes: ""),
        15007 :
            .init(blockHeight: 15007,
                  blockHash: "0000000038c44c703bae0f98cdd6bf30922326340a5996cc692aaae8bacf47ad",
                  blockData: "0100000002394092aa378fe35d7e9ac79c869b975c4de4374cd75eb5484b0e1e00000000eb9b8670abd44ad6c55cee18e3020fb0c6519e7004b01a16e9164867531b67afc33bc94fffff001d123f10050101000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0e04c33bc94f0115062f503253482fffffffff0100f2052a01000000232103f268e9ae07e0f8cb2f6e901d87c510d650b97230c0365b021df8f467363cafb1ac00000000",
                  prevOutputScripts: [[]],
                  previousBasicHeader: "18b5c2b0146d2d09d24fb00ff5b52bd0742f36c9e65527abdb9de30c027a4748",
                  basicFilter: "013c3710",
                  basicHeader: "07384b01311867949e0c046607c66b7a766d338474bb67f66c8ae9dbd454b20e",
                  notes: "Tx has non-standard OP_RETURN output followed by opcodes"),
        49291 :
            .init(blockHeight: 49291,
                  blockHash: "0000000018b07dca1b28b4b5a119f6d6e71698ce1ed96f143f54179ce177a19c",
                  blockData: "02000000abfaf47274223ca2fea22797e44498240e482cb4c2f2baea088962f800000000604b5b52c32305b15d7542071d8b04e750a547500005d4010727694b6e72a776e55d0d51ffff001d211806480201000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0d038bc0000102062f503253482fffffffff01a078072a01000000232102971dd6034ed0cf52450b608d196c07d6345184fcb14deb277a6b82d526a6163dac0000000001000000081cefd96060ecb1c4fbe675ad8a4f8bdc61d634c52b3a1c4116dee23749fe80ff000000009300493046022100866859c21f306538152e83f115bcfbf59ab4bb34887a88c03483a5dff9895f96022100a6dfd83caa609bf0516debc2bf65c3df91813a4842650a1858b3f61cfa8af249014730440220296d4b818bb037d0f83f9f7111665f49532dfdcbec1e6b784526e9ac4046eaa602204acf3a5cb2695e8404d80bf49ab04828bcbe6fc31d25a2844ced7a8d24afbdff01ffffffff1cefd96060ecb1c4fbe675ad8a4f8bdc61d634c52b3a1c4116dee23749fe80ff020000009400483045022100e87899175991aa008176cb553c6f2badbb5b741f328c9845fcab89f8b18cae2302200acce689896dc82933015e7230e5230d5cff8a1ffe82d334d60162ac2c5b0c9601493046022100994ad29d1e7b03e41731a4316e5f4992f0d9b6e2efc40a1ccd2c949b461175c502210099b69fdc2db00fbba214f16e286f6a49e2d8a0d5ffc6409d87796add475478d601ffffffff1e4a6d2d280ea06680d6cf8788ac90344a9c67cca9b06005bbd6d3f6945c8272010000009500493046022100a27400ba52fd842ce07398a1de102f710a10c5599545e6c95798934352c2e4df022100f6383b0b14c9f64b6718139f55b6b9494374755b86bae7d63f5d3e583b57255a01493046022100fdf543292f34e1eeb1703b264965339ec4a450ec47585009c606b3edbc5b617b022100a5fbb1c8de8aaaa582988cdb23622838e38de90bebcaab3928d949aa502a65d401ffffffff1e4a6d2d280ea06680d6cf8788ac90344a9c67cca9b06005bbd6d3f6945c8272020000009400493046022100ac626ac3051f875145b4fe4cfe089ea895aac73f65ab837b1ac30f5d875874fa022100bc03e79fa4b7eb707fb735b95ff6613ca33adeaf3a0607cdcead4cfd3b51729801483045022100b720b04a5c5e2f61b7df0fcf334ab6fea167b7aaede5695d3f7c6973496adbf1022043328c4cc1cdc3e5db7bb895ccc37133e960b2fd3ece98350f774596badb387201ffffffff23a8733e349c97d6cd90f520fdd084ba15ce0a395aad03cd51370602bb9e5db3010000004a00483045022100e8556b72c5e9c0da7371913a45861a61c5df434dfd962de7b23848e1a28c86ca02205d41ceda00136267281be0974be132ac4cda1459fe2090ce455619d8b91045e901ffffffff6856d609b881e875a5ee141c235e2a82f6b039f2b9babe82333677a5570285a6000000006a473044022040a1c631554b8b210fbdf2a73f191b2851afb51d5171fb53502a3a040a38d2c0022040d11cf6e7b41fe1b66c3d08f6ada1aee07a047cb77f242b8ecc63812c832c9a012102bcfad931b502761e452962a5976c79158a0f6d307ad31b739611dac6a297c256ffffffff6856d609b881e875a5ee141c235e2a82f6b039f2b9babe82333677a5570285a601000000930048304502205b109df098f7e932fbf71a45869c3f80323974a826ee2770789eae178a21bfc8022100c0e75615e53ee4b6e32b9bb5faa36ac539e9c05fa2ae6b6de5d09c08455c8b9601483045022009fb7d27375c47bea23b24818634df6a54ecf72d52e0c1268fb2a2c84f1885de022100e0ed4f15d62e7f537da0d0f1863498f9c7c0c0a4e00e4679588c8d1a9eb20bb801ffffffffa563c3722b7b39481836d5edfc1461f97335d5d1e9a23ade13680d0e2c1c371f030000006c493046022100ecc38ae2b1565643dc3c0dad5e961a5f0ea09cab28d024f92fa05c922924157e022100ebc166edf6fbe4004c72bfe8cf40130263f98ddff728c8e67b113dbd621906a601210211a4ed241174708c07206601b44a4c1c29e5ad8b1f731c50ca7e1d4b2a06dc1fffffffff02d0223a00000000001976a91445db0b779c0b9fa207f12a8218c94fc77aff504588ac80f0fa02000000000000000000",
                  prevOutputScripts: [
                    "5221033423007d8f263819a2e42becaaf5b06f34cb09919e06304349d950668209eaed21021d69e2b68c3960903b702af7829fadcd80bd89b158150c85c4a75b2c8cb9c39452ae",
                    "52210279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f8179821021d69e2b68c3960903b702af7829fadcd80bd89b158150c85c4a75b2c8cb9c39452ae",
                    "522102a7ae1e0971fc1689bd66d2a7296da3a1662fd21a53c9e38979e0f090a375c12d21022adb62335f41eb4e27056ac37d462cda5ad783fa8e0e526ed79c752475db285d52ae",
                    "52210279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f8179821022adb62335f41eb4e27056ac37d462cda5ad783fa8e0e526ed79c752475db285d52ae",
                    "512103b9d1d0e2b4355ec3cdef7c11a5c0beff9e8b8d8372ab4b4e0aaf30e80173001951ae",
                    "76a9149144761ebaccd5b4bbdc2a35453585b5637b2f8588ac",
                    "522103f1848b40621c5d48471d9784c8174ca060555891ace6d2b03c58eece946b1a9121020ee5d32b54d429c152fdc7b1db84f2074b0564d35400d89d11870f9273ec140c52ae",
                    "76a914f4fa1cc7de742d135ea82c17adf0bb9cf5f4fb8388ac" ],
                  previousBasicHeader: "ed47705334f4643892ca46396eb3f4196a5e30880589e4009ef38eae895d4a13",
                  basicFilter: "0afbc2920af1b027f31f87b592276eb4c32094bb4d3697021b4c6380",
                  basicHeader: "b6d98692cec5145f67585f3434ec3c2b3030182e1cb3ec58b855c5c164dfaaa3",
                  notes: "Tx pays to empty output script"),
        180480 :
            .init(blockHeight: 180480, blockHash: "00000000fd3ceb2404ff07a785c7fdcc76619edc8ed61bd25134eaa22084366a", blockData: "020000006058aa080a655aa991a444bd7d1f2defd9a3bbe68aabb69030cf3b4e00000000d2e826bfd7ef0beaa891a7eedbc92cd6a544a6cb61c7bdaa436762eb2123ef9790f5f552ffff001d0002c90f0501000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0e0300c102024608062f503253482fffffffff01c0c6072a01000000232102e769e60137a4df6b0df8ebd387cca44c4c57ae74cc0114a8e8317c8f3bfd85e9ac00000000010000000381a0802911a01ffb025c4dea0bc77963e8c1bb46313b71164c53f72f37fe5248010000000151ffffffffc904b267833d215e2128bd9575242232ac2bc311550c7fc1f0ef6f264b40d14c010000000151ffffffffdf0915666649dba81886519c531649b7b02180b4af67d6885e871299e9d5f775000000000151ffffffff0180817dcb00000000232103bb52138972c48a132fc1f637858c5189607dd0f7fe40c4f20f6ad65f2d389ba4ac0000000001000000018da38b434fba82d66052af74fc5e4e94301b114d9bc03f819dc876398404c8b4010000006c493046022100fe738b7580dc5fb5168e51fc61b5aed211125eb71068031009a22d9bbad752c5022100be5086baa384d40bcab0fa586e4f728397388d86e18b66cc417dc4f7fa4f9878012103f233299455134caa2687bdf15cb0becdfb03bd0ff2ff38e65ec6b7834295c34fffffffff022ebc1400000000001976a9147779b7fba1c1e06b717069b80ca170e8b04458a488ac9879c40f000000001976a9142a0307cd925dbb66b534c4db33003dd18c57015788ac0000000001000000026139a62e3422a602de36c873a225c1d3ca5aeee598539ceecb9f0dc8d1ad0f83010000006b483045022100ad9f32b4a0a2ddc19b5a74eba78123e57616f1b3cfd72ce68c03ea35a3dda1f002200dbd22aa6da17213df5e70dfc3b2611d40f70c98ed9626aa5e2cde9d97461f0a012103ddb295d2f1e8319187738fb4b230fdd9aa29d0e01647f69f6d770b9ab24eea90ffffffff983c82c87cf020040d671956525014d5c2b28c6d948c85e1a522362c0059eeae010000006b4830450221009ca544274c786d30a5d5d25e17759201ea16d3aedddf0b9e9721246f7ef6b32e02202cfa5564b6e87dfd9fd98957820e4d4e6238baeb0f65fe305d91506bb13f5f4f012103c99113deac0d5d044e3ac0346abc02501542af8c8d3759f1382c72ff84e704f7ffffffff02c0c62d00000000001976a914ae19d27efe12f5a886dc79af37ad6805db6f922d88ac70ce2000000000001976a9143b8d051d37a07ea1042067e93efe63dbf73920b988ac000000000100000002be566e8cd9933f0c75c4a82c027f7d0c544d5c101d0607ef6ae5d07b98e7f1dc000000006b483045022036a8cdfd5ea7ebc06c2bfb6e4f942bbf9a1caeded41680d11a3a9f5d8284abad022100cacb92a5be3f39e8bc14db1710910ef7b395fa1e18f45d41c28d914fcdde33be012102bf59abf110b5131fae0a3ce1ec379329b4c896a6ae5d443edb68529cc2bc7816ffffffff96cf67645b76ceb23fe922874847456a15feee1655082ff32d25a6bf2c0dfc90000000006a47304402203471ca2001784a5ac0abab583581f2613523da47ec5f53df833c117b5abd81500220618a2847723d57324f2984678db556dbca1a72230fc7e39df04c2239942ba942012102925c9794fd7bb9f8b29e207d5fc491b1150135a21f505041858889fa4edf436fffffffff026c840f00000000001976a914797fb8777d7991d8284d88bfd421ce520f0f843188ac00ca9a3b000000001976a9146d10f3f592699265d10b106eda37c3ce793f7a8588ac00000000", prevOutputScripts: [
                "76a9142903b138c24be9e070b3e73ec495d77a204615e788ac",
                "76a91433a1941fd9a37b9821d376f5a51bd4b52fa50e2888ac",
                "76a914e4374e8155d0865742ca12b8d4d14d41b57d682f88ac",
                "76a914001fa7459a6cfc64bdc178ba7e7a21603bb2568f88ac",
                "76a914f6039952bc2b307aeec5371bfb96b66078ec17f688ac"
            ], previousBasicHeader: "d34ef98386f413769502808d4bac5f20f8dfd5bffc9eedafaa71de0eb1f01489", basicFilter: "0db414c859a07e8205876354a210a75042d0463404913d61a8e068e58a3ae2aa080026", basicHeader: "c582d51c0ca365e3fcf36c51cb646d7f83a67e867cb4743fd2128e3e022b700c", notes: "Tx spends from empty output script"),
        926485 :
            .init(blockHeight: 926485, blockHash: "000000000000015d6077a411a8f5cc95caf775ccf11c54e27df75ce58d187313", blockData: "0000002060bbab0edbf3ef8a49608ee326f8fd75c473b7e3982095e2d100000000000000c30134f8c9b6d2470488d7a67a888f6fa12f8692e0c3411fbfb92f0f68f67eedae03ca57ef13021acc22dc4105010000000001010000000000000000000000000000000000000000000000000000000000000000ffffffff2f0315230e0004ae03ca57043e3d1e1d0c8796bf579aef0c0000000000122f4e696e6a61506f6f6c2f5345475749542fffffffff038427a112000000001976a914876fbb82ec05caa6af7a3b5e5a983aae6c6cc6d688ac0000000000000000266a24aa21a9ed5c748e121c0fe146d973a4ac26fa4a68b0549d46ee22d25f50a5e46fe1b377ee00000000000000002952534b424c4f434b3acd16772ad61a3c5f00287480b720f6035d5e54c9efc71be94bb5e3727f10909001200000000000000000000000000000000000000000000000000000000000000000000000000100000000010145310e878941a1b2bc2d33797ee4d89d95eaaf2e13488063a2aa9a74490f510a0100000023220020b6744de4f6ec63cc92f7c220cdefeeb1b1bed2b66c8e5706d80ec247d37e65a1ffffffff01002d3101000000001976a9143ebc40e411ed3c76f86711507ab952300890397288ac0400473044022001dd489a5d4e2fbd8a3ade27177f6b49296ba7695c40dbbe650ea83f106415fd02200b23a0602d8ff1bdf79dee118205fc7e9b40672bf31563e5741feb53fb86388501483045022100f88f040e90cc5dc6c6189d04718376ac19ed996bf9e4a3c29c3718d90ffd27180220761711f16c9e3a44f71aab55cbc0634907a1fa8bb635d971a9a01d368727bea10169522103b3623117e988b76aaabe3d63f56a4fc88b228a71e64c4cc551d1204822fe85cb2103dd823066e096f72ed617a41d3ca56717db335b1ea47a1b4c5c9dbdd0963acba621033d7c89bd9da29fa8d44db7906a9778b53121f72191184a9fee785c39180e4be153ae00000000010000000120925534261de4dcebb1ed5ab1b62bfe7a3ef968fb111dc2c910adfebc6e3bdf010000006b483045022100f50198f5ae66211a4f485190abe4dc7accdabe3bc214ebc9ea7069b97097d46e0220316a70a03014887086e335fc1b48358d46cd6bdc9af3b57c109c94af76fc915101210316cff587a01a2736d5e12e53551b18d73780b83c3bfb4fcf209c869b11b6415effffffff0220a10700000000001976a91450333046115eaa0ac9e0216565f945070e44573988ac2e7cd01a000000001976a914c01a7ca16b47be50cbdbc60724f701d52d75156688ac00000000010000000203a25f58630d7a1ea52550365fd2156683f56daf6ca73a4b4bbd097e66516322010000006a47304402204efc3d70e4ca3049c2a425025edf22d5ca355f9ec899dbfbbeeb2268533a0f2b02204780d3739653035af4814ea52e1396d021953f948c29754edd0ee537364603dc012103f7a897e4dbecab2264b21917f90664ea8256189ea725d28740cf7ba5d85b5763ffffffff03a25f58630d7a1ea52550365fd2156683f56daf6ca73a4b4bbd097e66516322000000006a47304402202d96defdc5b4af71d6ba28c9a6042c2d5ee7bc6de565d4db84ef517445626e03022022da80320e9e489c8f41b74833dfb6a54a4eb5087cdb46eb663eef0b25caa526012103f7a897e4dbecab2264b21917f90664ea8256189ea725d28740cf7ba5d85b5763ffffffff0200e1f5050000000017a914b7e6f7ff8658b2d1fb107e3d7be7af4742e6b1b3876f88fc00000000001976a914913bcc2be49cb534c20474c4dee1e9c4c317e7eb88ac0000000001000000043ffd60d3818431c495b89be84afac205d5d1ed663009291c560758bbd0a66df5010000006b483045022100f344607de9df42049688dcae8ff1db34c0c7cd25ec05516e30d2bc8f12ac9b2f022060b648f6a21745ea6d9782e17bcc4277b5808326488a1f40d41e125879723d3a012103f7a897e4dbecab2264b21917f90664ea8256189ea725d28740cf7ba5d85b5763ffffffffa5379401cce30f84731ef1ba65ce27edf2cc7ce57704507ebe8714aa16a96b92010000006a473044022020c37a63bf4d7f564c2192528709b6a38ab8271bd96898c6c2e335e5208661580220435c6f1ad4d9305d2c0a818b2feb5e45d443f2f162c0f61953a14d097fd07064012103f7a897e4dbecab2264b21917f90664ea8256189ea725d28740cf7ba5d85b5763ffffffff70e731e193235ff12c3184510895731a099112ffca4b00246c60003c40f843ce000000006a473044022053760f74c29a879e30a17b5f03a5bb057a5751a39f86fa6ecdedc36a1b7db04c022041d41c9b95f00d2d10a0373322a9025dba66c942196bc9d8adeb0e12d3024728012103f7a897e4dbecab2264b21917f90664ea8256189ea725d28740cf7ba5d85b5763ffffffff66b7a71b3e50379c8e85fc18fe3f1a408fc985f257036c34702ba205cef09f6f000000006a4730440220499bf9e2db3db6e930228d0661395f65431acae466634d098612fd80b08459ee022040e069fc9e3c60009f521cef54c38aadbd1251aee37940e6018aadb10f194d6a012103f7a897e4dbecab2264b21917f90664ea8256189ea725d28740cf7ba5d85b5763ffffffff0200e1f5050000000017a9148fc37ad460fdfbd2b44fe446f6e3071a4f64faa6878f447f0b000000001976a914913bcc2be49cb534c20474c4dee1e9c4c317e7eb88ac00000000", prevOutputScripts: [
                "a914feb8a29635c56d9cd913122f90678756bf23887687",
                "76a914c01a7ca16b47be50cbdbc60724f701d52d75156688ac",
                "76a914913bcc2be49cb534c20474c4dee1e9c4c317e7eb88ac",
                "76a914913bcc2be49cb534c20474c4dee1e9c4c317e7eb88ac",
                "76a914913bcc2be49cb534c20474c4dee1e9c4c317e7eb88ac",
                "76a914913bcc2be49cb534c20474c4dee1e9c4c317e7eb88ac",
                "76a914913bcc2be49cb534c20474c4dee1e9c4c317e7eb88ac",
                "76a914913bcc2be49cb534c20474c4dee1e9c4c317e7eb88ac",
            ], previousBasicHeader: "8f13b9a9c85611635b47906c3053ac53cfcec7211455d4cb0d63dc9acc13d472", basicFilter: "09027acea61b6cc3fb33f5d52f7d088a6b2f75d234e89ca800", basicHeader: "546c574a0472144bcaf9b6aeabf26372ad87c7af7d1ee0dbfae5e099abeae49c", notes: "Duplicate pushdata 913bcc2be49cb534c20474c4dee1e9c4c317e7eb"),
        987876 :
            .init(blockHeight: 987876, blockHash: "0000000000000c00901f2049055e2a437c819d79a3d54fd63e6af796cd7b8a79", blockData: "000000202694f74969fdb542090e95a56bc8aa2d646e27033850e32f1c5f000000000000f7e53676b3f12d5beb524ed617f2d25f5a93b5f4f52c1ba2678260d72712f8dd0a6dfe5740257e1a4b1768960101000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1603e4120ff9c30a1c216900002f424d4920546573742fffffff0001205fa012000000001e76a914c486de584a735ec2f22da7cd9681614681f92173d83d0aa68688ac00000000", prevOutputScripts: [[]], previousBasicHeader: "fe4d230dbb0f4fec9bed23a5283e08baf996e3f32b93f52c7de1f641ddfd04ad", basicFilter: "010c0b40", basicHeader: "0965a544743bbfa36f254446e75630c09404b3d164a261892372977538928ed5", notes: "Coinbase tx has unparseable output script"),
        1263442 :
            .init(blockHeight: 1263442, blockHash: "000000006f27ddfe1dd680044a34548f41bed47eba9e6f0b310da21423bc5f33", blockData: "000000201c8d1a529c39a396db2db234d5ec152fa651a2872966daccbde028b400000000083f14492679151dbfaa1a825ef4c18518e780c1f91044180280a7d33f4a98ff5f45765aaddc001d38333b9a02010000000001010000000000000000000000000000000000000000000000000000000000000000ffffffff230352471300fe5f45765afe94690a000963676d696e6572343208000000000000000000ffffffff024423a804000000001976a914f2c25ac3d59f3d674b1d1d0a25c27339aaac0ba688ac0000000000000000266a24aa21a9edcb26cb3052426b9ebb4d19c819ef87c19677bbf3a7c46ef0855bd1b2abe83491012000000000000000000000000000000000000000000000000000000000000000000000000002000000000101d20978463906ba4ff5e7192494b88dd5eb0de85d900ab253af909106faa22cc5010000000004000000014777ff000000000016001446c29eabe8208a33aa1023c741fa79aa92e881ff0347304402207d7ca96134f2bcfdd6b536536fdd39ad17793632016936f777ebb32c22943fda02206014d2fb8a6aa58279797f861042ba604ebd2f8f61e5bddbd9d3be5a245047b201004b632103eeaeba7ce5dc2470221e9517fb498e8d6bd4e73b85b8be655196972eb9ccd5566754b2752103a40b74d43df244799d041f32ce1ad515a6cd99501701540e38750d883ae21d3a68ac00000000", prevOutputScripts: [ "002027a5000c7917f785d8fc6e5a55adfca8717ecb973ebb7743849ff956d896a7ed" ], previousBasicHeader: "31d66d516a9eda7de865df29f6ef6cb8e4bf9309e5dac899968a9a62a5df61e3", basicFilter: "0385acb4f0fe889ef0", basicHeader: "4e6d564c2a2452065c205dd7eb2791124e0c4e0dbb064c410c24968572589dec", notes: "Includes witness data"),
        1414221 :
            .init(blockHeight: 1414221, blockHash: "0000000000000027b2b3b3381f114f674f481544ff2be37ae3788d7e078383b1", blockData: "000000204ea88307a7959d8207968f152bedca5a93aefab253f1fb2cfb032a400000000070cebb14ec6dbc27a9dfd066d9849a4d3bac5f674665f73a5fe1de01a022a0c851fda85bf05f4c19a779d1450102000000010000000000000000000000000000000000000000000000000000000000000000ffffffff18034d94154d696e6572476174653030310d000000f238f401ffffffff01c817a804000000000000000000", prevOutputScripts: [[]], previousBasicHeader: "5e5e12d90693c8e936f01847859404c67482439681928353ca1296982042864e", basicFilter: "00", basicHeader: "021e8882ef5a0ed932edeebbecfeda1d7ce528ec7b3daa27641acf1189d7b5dc", notes: "Empty data"),
    ]
}
