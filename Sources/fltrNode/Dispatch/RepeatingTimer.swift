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
import var HaByLo.logger
import Dispatch

final class RepeatingTimer {
    enum State {
        case initialized(DispatchSourceTimer, DispatchTimeInterval)
        case started(DispatchSourceTimer)
        case stopped
    }
    
    enum Error: Swift.Error {
        case timerNeverStarted
        case timerPreviouslyStarted
        case timerPreviouslyStopped
    }
    
    private var t: State
    
    init(interval: DispatchTimeInterval) {
        let timer = DispatchSource.makeTimerSource()
        self.t = .initialized(timer, interval)
    }
    
    deinit {
        switch self.t {
        case .initialized:
            logger.error("RepeatingTimer", #function, "- WARNING timer leaked without being started")
            try! self.start(.init(block: {}))
            try! self.stop()
        case .started:
            logger.error("RepeatingTimer", #function, "- WARNING timer leaked without being stopped")
            try! self.stop()
        case .stopped:
            break
        }
    }

    func start(_ work: DispatchWorkItem) throws {
        switch self.t {
        case .initialized(let timer, let interval):
            self.t = .started(timer)
            timer.setEventHandler(handler: work)
            timer.schedule(deadline: .now() + interval, repeating: interval)
            timer.resume()
        case .started:
            throw Error.timerPreviouslyStarted
        case .stopped:
            throw Error.timerPreviouslyStopped
        }
    }
    
    func stop() throws {
        switch self.t {
        case .initialized:
            throw Error.timerNeverStarted
        case .started(let timer):
            timer.suspend()
            timer.setEventHandler(handler: nil)
            timer.cancel()
            timer.resume()
            self.t = .stopped
        case .stopped:
            throw Error.timerPreviouslyStopped
        }
    }
}
