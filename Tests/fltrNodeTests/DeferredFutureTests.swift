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
import HaByLo
import NodeTestLibrary
import NIO
import NIOTransportServices
import XCTest

final class DeferredFutureTests: XCTestCase {
    var elg: NIOTSEventLoopGroup!
    var eventLoop: EventLoop!
    var threadPool: NIOThreadPool!
    var savedSettings: NodeSettings!
    
    override func setUp() {
        self.savedSettings = Settings
        TestingHelpers.diSetup {
            $0.EVENT_LOOP_DEFAULT_QOS = .default
        }

        self.elg = .init(loopCount: 1)
        self.eventLoop = self.elg.next()
        self.threadPool = NIOThreadPool(numberOfThreads: 2)
    }
    
    override func tearDown() {
        Settings = self.savedSettings
        self.savedSettings = nil
    }
    
    struct TestingError: Swift.Error {}
    
    func testDeferredPriorityFuture() throws {
        let d = DeferredPriorityFuture<Int, Bool>.must(then: 10, eventLoop: self.eventLoop) { inputInt in
            inputInt % 2 == 0
                ? self.eventLoop.makeSucceededFuture(true)
                : self.eventLoop.makeSucceededFuture(false)
        }
        
        var start = 0
        var result = true
        var next = d.next(input: start)
        switch next.priority {
        case .must: break
        default: XCTFail()
        }
        XCTAssertNoThrow(
            XCTAssertEqual(try next.deferred().wait(), result)
        )
        result.toggle()
        start += 1
        
        for _ in (1...10) {
            next = d.next(input: start)
            switch next.priority {
            case .may: break
            default: XCTFail()
            }
            XCTAssertNoThrow(
                XCTAssertEqual(try next.deferred().wait(), result)
            )
            result.toggle()
            start += 1

        }

        switch d.next(input: start).priority {
        case .skip: break
        default: XCTFail()
        }
    }
    

    func testDeferredSequence() {
        let d = DeferredPriorityFeedback<Int>.must(then: 10, eventLoop: self.eventLoop) { number in
            self.eventLoop.makeSucceededFuture(number >> 1)
        }
        
        let start = 65536
        var next = d.next(input: start)
        switch next.priority {
        case .must: break
        default: XCTFail()
        }
        XCTAssertNoThrow(
            XCTAssertEqual(try next.deferred().wait(), start >> 1)
        )
        
        for i in 1...10 {
            next = d.next(input: start >> i)
            switch next.priority {
            case .may: break
            default: XCTFail()
            }
            XCTAssertNoThrow(
                XCTAssertEqual(try next.deferred().wait(), start >> (i + 1))
            )
        }
        
        switch d.next(input: start >> 10).priority {
        case .skip: break
        default: XCTFail()
        }
    }
}
