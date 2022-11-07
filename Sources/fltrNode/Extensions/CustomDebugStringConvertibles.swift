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
import HaByLo
import NIO

extension AddrCommand: CustomDebugStringConvertible {
    var debugDescription: String {
        var result: [String] = []
        result.append("addr")
        
        if self.addresses.count > 1 {
            result.append("#[\(self.addresses.count)]")
        } else if let first = self.addresses.first {
            result.append("IP#[\(first.address.asIPv4?.debugDescription ?? first.address.asString)]")
        } else {
            result.append("ERRONEOUS🚨 empty command")
        }
        
        let services = self.addresses.map(\.services)
        let totalFull = services.compactMap {
            $0.isFullNode ? .some(()) : .none
        }
        .count
        result.append("full#[\(totalFull)]")
        
        
        let totalFilter = services.compactMap {
            $0.isFilterNode ? .some(()) : .none
        }
        .count
        result.append("filter#[\(totalFilter)]")
        
        return result.joined(separator: " ")
    }
}

extension CF.CfheadersCommand: CustomDebugStringConvertible {
    var debugDescription: String {
        [ "cfheader🐤 #[\(self.headers.count)]",
          self.headers.first.map { "first prevHeaderHash[\($0.previousHeaderHash)]" } ?? "" ]
        .joined(separator: " ")
    }
}

extension FeefilterCommand: CustomDebugStringConvertible {
    var debugDescription: String {
        "feefilter with rate#[\(self.feerate)]"
    }
}

extension CF.GetcfheadersCommand: CustomDebugStringConvertible {
    var debugDescription: String {
        [ "getcfheaders t[\(self.filterType)] startHeight[\(self.startHeight)] stopHash[\(self.stopHash)]",
          self.timeout.map { "timeout:[\($0)]" } ?? "" ]
        .joined(separator: " ")
        
    }
}

extension CF.GetcfiltersCommand: CustomDebugStringConvertible {
    var debugDescription: String {
        [ "getcfilters t[\(self.filterType)] startHeight[\(self.startHeight)] stopHash[\(self.stopHash)]",
          self.timeout.map { "timeout:[\($0)]" } ?? "" ]
        .joined(separator: " ")
    }
}

extension GetdataCommand: CustomDebugStringConvertible {
    var debugDescription: String {
        var out: [String] = ["getdata📠"]
        
        if self.inventory.count > 0 {
            out.append(
                "inventory#[\(self.inventory.count)]"
            )
            
            let countBlock = self.inventory.filter {
                if case .witnessBlock = $0 {
                    return true
                } else if case .block = $0 {
                    return true
                } else {
                    return false
                }
            }
            .count
            if countBlock > 0 {
                out.append("block#[\(countBlock)]")
            }
            
            let countTx = self.inventory.filter {
                if case .witnessTx = $0 {
                    return true
                } else if case .tx = $0 {
                    return true
                } else {
                    return false
                }
            }
            .count
            if countTx > 0 {
                out.append("tx#[\(countTx)]")
            }
        }
        
        self.timeout.map { out.append("timeout:[\($0)]") }
        
        return out.joined(separator: " ")
    }
}

extension GetheadersCommand: CustomDebugStringConvertible {
    var debugDescription: String {
        var out: [String] = ["getheaders👥"]
        out.append(
            "locator#[\(self.blockLocator.hashes.count)]"
        )
        
        if let first = self.blockLocator.hashes.first {
            out.append(
                "first[\(first)]"
            )
        }
        
        if self.blockLocator.stop == .zero {
            out.append(
                "stop[.zero]"
            )
        } else {
            out.append(
                "stop[\(self.blockLocator.stop)]"
            )
        }
        
        return out.joined(separator: " ")
    }
}

extension HeadersCommand: CustomDebugStringConvertible {
    var debugDescription: String {
        [ "headers👥 #[\(self.headers.count)]",
          (try? self.headers.first?.previousBlockHash()).map { "first PREVHASH:[\($0)]" } ?? "" ]
        .joined(separator: " ")
    }
}

extension InvCommand: CustomDebugStringConvertible {
    var debugDescription: String {
        var out = [ "inv🗂" ]
            
        guard !self.inventory.isEmpty else {
            out.append("📭 empty inv")
            return out.joined(separator: " ")
        }
        
        func mixAndMatch(predicate: (Inventory) -> Bool) -> Int? {
            (
                self.inventory.filter(predicate)
                .count as Int?
            )
            .flatMap({ $0 == 0 ? .none : .some($0) })
        }

        if self.inventory.count > 2 {
            out.append(
                contentsOf: [
                    mixAndMatch {
                        if case .block = $0 {
                            return true
                        } else {
                            return false
                        }
                    }
                    .map {
                        "block#[\($0)]"
                    },
                    mixAndMatch {
                        if case .compactBlock = $0 {
                            return true
                        } else {
                            return false
                        }
                    }
                    .map {
                        "compactBlock#[\($0)]"
                    },
                    mixAndMatch {
                        if case .filteredBlock = $0 {
                            return true
                        } else {
                            return false
                        }
                    }
                    .map {
                        "filteredBlock#[\($0)]"
                    },
                    mixAndMatch {
                        if case .tx = $0 {
                            return true
                        } else {
                            return false
                        }
                    }
                    .map {
                        "tx#[\($0)]"
                    },
                    mixAndMatch {
                        if case .witnessBlock = $0 {
                            return true
                        } else {
                            return false
                        }
                    }
                    .map {
                        "witnessBlock#[\($0)]"
                    },
                    mixAndMatch {
                        if case .witnessTx = $0 {
                            return true
                        } else {
                            return false
                        }
                    }
                    .map {
                        "witnessTx#[\($0)]"
                    },
                ].compactMap { $0 }
            )
        } else {
            out.append(String(reflecting: self.inventory))
        }

        return out.joined(separator: " ")
    }
}

extension Inventory: CustomDebugStringConvertible {
    var debugDescription: String {
        func printHash<H>(_ hash: BlockChain.Hash<H>, prefix: String) -> String {
            "\(prefix)[\(hash)]"
        }

        var out: [String] = []
        switch self {
        case .block(let hash):
            out.append(printHash(hash, prefix: "B📦"))
        case .compactBlock(let hash):
            out.append(printHash(hash, prefix: "cB📦"))
        case .error:
            out.append("🎈ErrorInv")
        case .filteredBlock(let hash):
            out.append(printHash(hash, prefix: "fB📦"))
        case .tx(let hash):
            out.append(printHash(hash, prefix: "Tx💳"))
        case .witnessBlock(let hash):
            out.append(printHash(hash, prefix: "wB📦"))
        case .witnessTx(let hash):
            out.append(printHash(hash, prefix: "wTx💳"))
        }
        
        return out.joined(separator: " ")
    }
}

extension PingCommand: CustomDebugStringConvertible {
    var debugDescription: String {
        "ping🔔 with nonce:[\(self.nonce)]"
    }
}

extension PongCommand: CustomDebugStringConvertible {
    var debugDescription: String {
        var out = "pong🔕"
        out.append(
            self.latencyTimer > 0 ? " T#[\(self.latencyTimer)] " : " "
        )
        out.append("with nonce:[\(self.nonce)]")
        return out
    }
}

extension RejectCommand.CCode: CustomDebugStringConvertible {
    var debugDescription: String {
        switch self {
        case .malformed:
            return ".malformed"
        case .invalid:
            return ".invalid"
        case .obsolete:
            return ".obsolete"
        case .duplicate:
            return ".duplicate"
        case .nonstandard:
            return ".nonstandard"
        case .dust:
            return ".dust"
        case .insufficientFee:
            return ".insufficientFee"
        case .checkpoint:
            return ".checkpoint"
        }
    }
}

extension RejectCommand: CustomDebugStringConvertible {
    var debugDescription: String {
        var out: [String] = [ "REJECT💣" ]
        out.append("message:[\(self.message)]")
        out.append("reason:[CCodes\(self.ccode)"
                   + (self.reason.isEmpty
                      ? "]"
                      : "||\(self.reason)]")
        )
        if let hash = self.hash {
            out.append("hash:[\(hash)]")
        }
        
        return out.joined(separator: " ")
    }
}

extension SendcmpctCommand: CustomDebugStringConvertible {
    var debugDescription: String {
        "sendcmpct V#[\(self.version)]\(self.includingNewBlocks ? " with include new blocks" : "")"
    }
}

extension SendheadersCommand: CustomDebugStringConvertible {
    var debugDescription: String {
        "sendheaders"
        
    }
}

extension TimeAmount: CustomDebugStringConvertible {
    public var debugDescription: String {
        func divide(_ t: TimeAmount, by divisor: Int64) -> TimeAmount {
            let ns = t.nanoseconds
            return .nanoseconds(ns / divisor)
        }
        
        var time = [ "⏲" ]
        
        let abs: Int64
        if self.nanoseconds < 0 {
            time.append("MINUS")
            abs = -self.nanoseconds
        } else {
            abs = self.nanoseconds
        }
        
        let s: Int64 = 1_000_000_000
        let m = 60 * s
        let h = 60 * m
        
        switch abs {
        case let t where t < 1000:
            time.append("\(t) ns")
        case let t where t < 1_000_000:
            time.append("\(t / 1000) us")
        case let t where t < s:
            time.append("\(t / 1_000_000) ms")
        case let t where t < m:
            time.append("\(t / s) s")
        case let t where t < 60 * m:
            let (minutes, r) = abs.quotientAndRemainder(dividingBy: m)
            time.append("\(minutes) min \(r / s) s")
        default:
            let (hours, r1) = abs.quotientAndRemainder(dividingBy: h)
            let (minutes, r2) = r1.quotientAndRemainder(dividingBy: m)
            time.append("\(hours) h \(minutes) min \(r2 / s) s")
        }
        
        return time.joined(separator: " ")
    }
}

extension UnhandledCommand: CustomDebugStringConvertible {
    var debugDescription: String {
        "UNHANDLED🚨 command:[\(self.description)] payload:[\(self.buffer.readableBytesView.hexEncodedString)]"
    }
}

extension VerackCommand: CustomDebugStringConvertible {
    var debugDescription: String {
        self.description
    }
}

extension VersionCommand: CustomDebugStringConvertible {
    var debugDescription: String {
        "🅥ersion UA[\(self.userAgent)]   Services[\(self.services)]   Relay[\(self.relay ? "✅" : "❎")]"
    }
}
