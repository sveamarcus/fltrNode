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
import Dispatch
import XCTest

final class RepeatingTimerTests: XCTestCase {
    var timer: fltrNode.RepeatingTimer!
    
    override func setUp() {
    }
    
    override func tearDown() {
        self.timer = nil
    }
    
    func testSimpleTimer() {
        var start = 0
        self.timer = RepeatingTimer.init(interval: DispatchTimeInterval.nanoseconds(1))
        let e = expectation(description: "timer")
        let workItem: DispatchWorkItem = .init() {
            if start == 10_000 {
                e.fulfill()
            }
            start += 1
        }
        XCTAssertNoThrow(try self.timer.start(workItem))
        wait(for: [e], timeout: 0.5)
        XCTAssertNoThrow(try self.timer.stop())
    }
    
    func testStopOnDeinit() {
        var start = 0
        self.timer = RepeatingTimer.init(interval: DispatchTimeInterval.milliseconds(1))
        
        let workItem: DispatchWorkItem = .init() {
            if start > 0 {
                XCTFail()
            }
            start += 1
        }
        XCTAssertNoThrow(try self.timer.start(workItem))
        self.timer = nil
        
        Thread.sleep(forTimeInterval: 0.1)
    }
    
    func testDoubleStart() {
        let workItem: DispatchWorkItem = .init() {}
        self.timer = RepeatingTimer(interval: .nanoseconds(1))
        XCTAssertNoThrow(try self.timer.start(workItem))
        XCTAssertThrowsError(try self.timer.start(workItem))
        XCTAssertNoThrow(try self.timer.stop())
    }
    
    func testDoubleStop() {
        let workItem: DispatchWorkItem = .init() {}
        self.timer = RepeatingTimer(interval: .nanoseconds(1))
        XCTAssertNoThrow(try self.timer.start(workItem))
        XCTAssertNoThrow(try self.timer.stop())
        XCTAssertThrowsError(try self.timer.stop())
    }
    
    func testStopOnInit() {
        self.timer = RepeatingTimer(interval: .nanoseconds(1))
        XCTAssertThrowsError(try self.timer.stop())
    }
}
