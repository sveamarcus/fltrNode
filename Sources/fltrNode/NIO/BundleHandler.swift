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
import var HaByLo.logger
import NIO
import protocol Foundation.LocalizedError

final class BundleHandler: ChannelOutboundHandler {
    typealias OutboundOut = QueueHandler.OutboundIn

    enum OutboundIn {
        case inv(QueueHandler.OutboundIn, InventoryBundleType)
        case wrap(QueueHandler.OutboundIn)
    }
    
    enum InventoryBundleType {
        case block(Int)
        case txs(Int)
        case cfilter(Int)
    }
    
    fileprivate enum StateMachineType {
        case block(StateMachine<BlockCommand>)
        case tx(StateMachine<Tx.Transaction>)
        case cfilter(StateMachine<CF.CfilterCommand>)
        case initialized
    }

    private var stateMachine: StateMachineType = .initialized

    // MARK: Outbound
    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let unwrap = self.unwrapOutboundIn(data)
        switch unwrap {
        case let .inv(command, inventoryBundleType):
            promise!.futureResult.whenSuccess {
                switch (self.stateMachine, inventoryBundleType) {
                case (.initialized, .block(let remaining)):
                    self.stateMachine = .block(.init(count: remaining))
                    logger.trace("BundleHandler", #function, "- Initializing receive BLOCKS🧱 count (", remaining, ")")
                case (.initialized, .txs(let remaining)):
                    self.stateMachine = .tx(.init(count: remaining))
                    logger.trace("BundleHandler", #function, "- Initializing receive TXs📁 count (", remaining, ")")
                case (.initialized, .cfilter(let remaining)):
                    self.stateMachine = .cfilter(.init(count: remaining))
                    logger.trace("BundleHandler", #function, "- Initializing receive CFs📠 count (", remaining, ")")
                case (.block, _),
                     (.tx, _),
                     (.cfilter, _):
                    context.fireErrorCaught(Error.illegalStateReceiveOutgoingInvBeforeCompletion)
                }
            }

            context.write(self.wrapOutboundOut(command), promise: promise)
        case .wrap(let command):
            context.write(self.wrapOutboundOut(command), promise: promise)
        }
    }
}

extension BundleHandler: ChannelInboundHandler {
    typealias InboundIn = FrameToCommandHandler.InboundOut
    typealias InboundOut = FrameToCommandHandler.InboundOut

    // MARK: Inbound
    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        func complete<T: BitcoinCommandValue>(with bundle: BitcoinCommandBundle<[T]>?) {
            guard let bundle = bundle else {
                return
            }

            self.stateMachine = .initialized

            logger.trace("BundleHandler", #function, "- Completing 🎁 bundle for \(T.self)")
            context.fireChannelRead(self.wrapInboundOut(
                .pending(
                    bundle
                )
            ))
        }

        func evaluate<T: BitcoinCommandValue>(with stateMachine: BundleHandler.StateMachine<T>,
                                              for command: T) {
            do {
                try stateMachine.receive(command)
            } catch {
                context.fireErrorCaught(error)
            }

            complete(with: stateMachine.checkPayloadReady())
        }
        
        switch (self.stateMachine, self.unwrapInboundIn(data)) {
        case (.block(let stateMachine), .pending(let command as BlockCommand)):
            evaluate(with: stateMachine, for: command)
        case (.tx(let stateMachine), .pending(let command as Tx.Transaction)):
            evaluate(with: stateMachine, for: command)
        case (.cfilter(let stateMachine), .pending(let command as CF.CfilterCommand)):
            evaluate(with: stateMachine, for: command)
        case (.block(let stateMachine), .pending(let notfoundCommand as NotfoundCommand)):
            do {
                try stateMachine.receive(notfoundCommand)
            } catch {
                context.fireErrorCaught(error)
            }
            complete(with: stateMachine.checkPayloadReady())
        case (.tx(let stateMachine), .pending(let notfoundCommand as NotfoundCommand)):
            do {
                try stateMachine.receive(notfoundCommand)
            } catch {
                context.fireErrorCaught(error)
            }
            complete(with: stateMachine.checkPayloadReady())
        case (.initialized, _),
             (.block, _),
             (.tx, _),
             (.cfilter, _): // Forward any other type of data down the pipeline
            context.fireChannelRead(data)
        }
    }
}

extension BundleHandler: CleanupLocalStateEvent {
    func cleanup(error: Swift.Error) {
        self.stateMachine = .initialized
    }
}

// MARK: StateMachine
fileprivate protocol ReceiveDataTransition {
    associatedtype Data: BitcoinCommandValue
    
    var dataCompleteState: BundleHandler.StateMachine<Data>.State { get }
    var dataIncompleteState: BundleHandler.StateMachine<Data>.State { get }
    
    var remaining: Int { get set }
    var accumulator: [Data] { get set }
}

extension ReceiveDataTransition {
    mutating func receiveData(_ input: Data) throws -> BundleHandler.StateMachine<Data>.State {
        guard self.remaining > 0 else {
            throw BundleHandler.Error.receiveDataRemainingNegative(action: #function)
        }
        
        self.accumulator.append(input)
        self.remaining -= 1
        
        if self.remaining > 0 {
            return self.dataIncompleteState
        } else {
            return self.dataCompleteState
        }
    }
}

fileprivate protocol ReceiveNotFoundTransition {
    associatedtype Data: BitcoinCommandValue
    
    func notfoundCompleteState(_ notfound: NotfoundCommand) -> BundleHandler.StateMachine<Data>.State
    func notfoundIncompleteState(_ notfound: NotfoundCommand) -> BundleHandler.StateMachine<Data>.State

    var remaining: Int { get set }
}

extension ReceiveNotFoundTransition {
    mutating func receiveNotfound(_ notfound: NotfoundCommand) throws -> BundleHandler.StateMachine<Data>.State {
        guard notfound.inventory.count <= self.remaining else {
            throw BundleHandler.Error.receiveNotFoundRemainingNegative(action: #function)
        }
        
        self.remaining -= notfound.inventory.count
        
        if self.remaining > 0 {
            return self.notfoundIncompleteState(notfound)
        } else {
            return self.notfoundCompleteState(notfound)
        }
    }
}

extension BundleHandler {
    fileprivate final class StateMachine<T: BitcoinCommandValue> {
        enum State {
            case awaitingPayload(PayloadState)
            case awaitingPayloadAfterNotFound(NotFoundState)
            case payloadReady(BundleState)
        }
    
        private var state: State
    
        init(count: Int) {
            self.state = .awaitingPayload(PayloadState(remaining: count))
        }
        
        func receive(_ payload: T) throws {
            switch self.state {
            case .awaitingPayload(var state):
                self.state = try state.receiveData(payload)
            case .awaitingPayloadAfterNotFound(var state):
                self.state = try state.receiveData(payload)
            case .payloadReady:
                throw Error.invalidTransition(state: String(describing: self.state), action: #function)
            }
        }
        
        func receive(_ notfound: NotfoundCommand) throws {
            guard notfound.inventory.count > 0 else {
                return
            }

            switch self.state  {
            case .awaitingPayload(var state):
                self.state = try state.receiveNotfound(notfound)
            case .awaitingPayloadAfterNotFound:
                throw Error.receiveNotFoundTwice(action: #function)
            case .payloadReady:
                throw Error.invalidTransition(state: String(describing: self.state), action: #function)
            }
        }
        
        func checkPayloadReady() -> BitcoinCommandBundle<[T]>? {
            switch self.state {
            case .payloadReady(let bundle):
                return bundle.bundle
            case .awaitingPayload, .awaitingPayloadAfterNotFound:
                return nil
            }
        }
    }
}

// MARK: Associated Value States
extension BundleHandler.StateMachine {
    fileprivate struct PayloadState: ReceiveDataTransition, ReceiveNotFoundTransition {
        var accumulator: [T] = []
        var remaining: Int
        
        var dataCompleteState: BundleHandler.StateMachine<T>.State {
            .payloadReady(BundleState(self))
        }
        
        var dataIncompleteState: BundleHandler.StateMachine<T>.State {
            .awaitingPayload(self)
        }
        
        func notfoundCompleteState(_ notfound: NotfoundCommand) -> BundleHandler.StateMachine<T>.State {
            let notfoundState = NotFoundState(self, notFound: notfound)
            return .payloadReady(BundleState(notfoundState))
        }
        
        func notfoundIncompleteState(_ notfound: NotfoundCommand) -> BundleHandler.StateMachine<T>.State {
            .awaitingPayloadAfterNotFound(NotFoundState(self, notFound: notfound))
        }
    }
    
    fileprivate struct NotFoundState: ReceiveDataTransition {
        var accumulator: [T]
        var remaining: Int
        let notfound: NotfoundCommand
        
        init(_ payloadState: PayloadState, notFound: NotfoundCommand) {
            self.accumulator = payloadState.accumulator
            self.notfound = notFound
            self.remaining = payloadState.remaining
            assert(self.remaining >= 0)
        }
        
        var dataCompleteState: BundleHandler.StateMachine<T>.State {
            .payloadReady(BundleState(self))
        }
        
        var dataIncompleteState: BundleHandler.StateMachine<T>.State {
            .awaitingPayloadAfterNotFound(self)
        }
    }
    
    fileprivate struct BundleState {
        let bundle: BitcoinCommandBundle<[T]>
        
        init(_ state: PayloadState) {
            assert(state.remaining == 0)
            self.bundle = .init(value: state.accumulator, notfound: nil)
        }
        
        init(_ state: NotFoundState) {
            assert(state.remaining == 0)
            self.bundle = .init(value: state.accumulator, notfound: state.notfound)
        }
    }

}

// MARK: Error
extension BundleHandler {
    enum Error: Swift.Error, CustomStringConvertible, LocalizedError {
        case illegalStateReceiveOutgoingInvBeforeCompletion
        case invalidTransition(state: String, action: StaticString)
        case receiveDataRemainingNegative(action: StaticString)
        case receiveNotFoundRemainingNegative(action: StaticString)
        case receiveNotFoundTwice(action: StaticString)
        
        var errorDescription: String {
            self.description
        }
        
        var description: String {
            switch self {
            case .illegalStateReceiveOutgoingInvBeforeCompletion:
                return ".illegalStateReceiveOutgoingInvBeforeCompletion"
            case .invalidTransition(state: let state, action: let action):
                return ".invalidTransition: ERROR during state (\(state)) for action (\(action))"
            case .receiveDataRemainingNegative(let action):
                return ".receiveDataRemainingNegative: (\(action)) received data is greater than expected remaining"
            case .receiveNotFoundRemainingNegative(let action):
                return ".receiveNotFoundRemainingNegative: (\(action)) received notfound count is greater than expected remaining"
            case .receiveNotFoundTwice(let action):
                return ".receiveNotFoundTwice: (\(action)) received second NotfoundCommand when already in a notfound state"
            }
        }
    }
}
