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
import NIO
import NIOConcurrencyHelpers
@testable import fltrNode

private final class BlockingQueue<Element> {
    private let condition = ConditionLock(value: false)
    private var buffer = CircularBuffer<Result<Element, Error>>()

    public var count: Int {
        self.buffer.count
    }
    
    public struct TimeoutError: Error {}

    internal func append(_ element: Result<Element, Error>) {
        logger.trace("BlockingQueue", #function, "Appending element (", element, ")")
        self.condition.lock()
        self.buffer.append(element)
        self.condition.unlock(withValue: true)
    }

    internal var isEmpty: Bool {
        self.condition.lock()
        defer { self.condition.unlock() }
        return self.buffer.isEmpty
    }

    internal func popFirst(deadline: NIODeadline) throws -> Element {
        let secondsUntilDeath = ((deadline - .now()).nanoseconds - 1) / 1_000_000_000 + 1
        logger.trace("BlockingQueue", #function, "🟢 Requesting first", buffer.isEmpty ? "while buffer is empty LOCKING" : "with available data in buffer (\(buffer.count))", "timeout (", secondsUntilDeath, ")")
        guard self.condition.lock(whenValue: true, timeoutSeconds: .init(secondsUntilDeath)) else {
            logger.trace("BlockingQueue", #function, "🟢 Timeout triggered - throwing Error")
            throw TimeoutError()
        }
        let first = self.buffer.removeFirst()
        logger.trace("BlockingQueue", #function, "🟢 Condition fulfilled popping value(", first, ")")
        self.condition.unlock(withValue: !self.buffer.isEmpty)
        return try first.get()
    }
}

public final class TestServer {
    private let eventLoop: EventLoop
    private let inboundBuffer: BlockingQueue<FrameToCommandHandler.InboundOut> = .init()
    private var currentClientChannel: Channel? = nil
    private var channelTestsDone: EventLoopPromise<Void>? = nil
    private var serverChannel: Channel! = nil
    
    enum State {
        case idle
        case waiting(EventLoopPromise<Void>)
        case working(CircularBuffer<Channel>)
        case stopped
    }
    private var state: State = .idle {
        didSet {
//            logger.trace("TestServer 🍋🍊🍏 State changed from (", oldValue, ") to (", state, ")")
        }
    }
    
    public var count: Future<Int> {
        self.eventLoop.submit {
            self.inboundBuffer.count
        }
    }

    public init(group: EventLoopGroup) {
        // EventLoop <=> EventLoopGroup, this ensures single EventLoop
        self.eventLoop = group.next()
        
        self.serverChannel = try! ServerBootstrap(group: self.eventLoop)
        .childChannelOption(ChannelOptions.autoRead, value: false)
        .childChannelInitializer { channel in
            logger.trace("TestServer childChannelInitializer {} 🟡 CONNECTION for channel (", channel.remoteAddress!, ")")
            switch self.state {
            case .idle:
                // This state is only reachable when server is currently serving the last channel
                // when finished with the last channel it would run handleChannels on .idle and move to .waiting
                self.state = .working([channel])
            case .waiting(let promise):
                self.state = .working([channel])
                promise.succeed(())
            case .working(var channels):
                channels.append(channel)
                self.state = .working(channels)
            case .stopped:
                channel.close(promise: nil)
            }
            return channel.eventLoop.makeSucceededFuture(())
        }
        .bind(host: "127.0.0.1", port: 0)
        .map { channel in
            logger.trace("TestServer", #function, "ServerBootstrap completed (executed once), serverChannel set. Listening on port:", channel.localAddress!.port!)
            self.handleChannels()
            return channel
        }
        .wait()
    }
    
    func handleChannels() {
        self.eventLoop.assertInEventLoop()
        
        switch self.state {
        case .idle:
            let promise = self.eventLoop.makePromise(of: Void.self)
            promise.futureResult.whenSuccess(self.handleChannels)
            self.state = .waiting(promise)
        case .waiting(_):
            logger.error("TestServer", #function, "Illegal state calling function with current state .waiting")
            preconditionFailure()
        case .working(var channels):
            let channel = channels.removeFirst()
            if channels.isEmpty {
                self.state = .idle
            } else {
                self.state = .working(channels)
            }
            assert(self.currentClientChannel == nil)
            assert(self.channelTestsDone == nil)
            self.currentClientChannel = channel
            self.channelTestsDone = eventLoop.makePromise(of: Void.self)
            self.channelTestsDone!.futureResult.whenSuccess {
                logger.trace("TestServer", "channelTestsDone!.futureResult.whenSuccess {}", "🛑 Running completion handlers for current channel")
                self.currentClientChannel = nil
                self.channelTestsDone = nil
                self.handleChannels()
            }
            channel.pipeline.addHandlers([
                ByteToMessageHandler(FrameDecoder()),
                CommandWriterHandler(),
                FrameToCommandHandler(),
                ServerHandler(btcTestServer: self),
            ])
            .whenSuccess {
                logger.trace("TestServer", #function, "✅♿️🔛 setting channel options READ")
                _ = channel.setOption(ChannelOptions.autoRead, value: true)
            }
        case .stopped:
            break
        }
    }
    
    fileprivate func pushHandlerRead(_ command: FrameToCommandHandler.InboundOut) {
        self.eventLoop.assertInEventLoop()
        if case .incoming(.version) = command {
            logger.trace("TestServer channelRead() -> pushHandlerRead(_ command:) 🚩 VERSION command recevied, sending VERACK")
            let cmd: BitcoinCommandValue = VerackCommand()
            currentClientChannel!.writeAndFlush(NIOAny(cmd), promise: nil)
        }
        self.inboundBuffer.append(.success(command))
    }
    
    fileprivate func pushHandlerError(_ error: Error) {
        self.eventLoop.assertInEventLoop()
    }
}

extension TestServer {
    public func channelIn(timeout: Int = 2) throws -> FrameToCommandHandler.InboundOut {
        assert(!self.eventLoop.inEventLoop)
        return try self.eventLoop.submit { () -> BlockingQueue<FrameToCommandHandler.InboundOut> in
            return self.inboundBuffer
        }
        .wait()
        .popFirst(deadline: .now() + TimeAmount.seconds(.init(timeout)))
    }
    
    public func channelOut(command: BitcoinCommandValue) throws {
        assert(!self.eventLoop.inEventLoop)
        try self.eventLoop.flatSubmit {
            self.currentClientChannel == nil ? self.eventLoop.makeFailedFuture(ChannelError.ioOnClosedChannel) : self.currentClientChannel!.writeAndFlush(NIOAny(command))
        }
        .wait()
    }
    
    public func nextChannel() {
        _ = self.eventLoop.submit {
            logger.trace("TestServer", #function, "🛑 Closing current channel and calling .channelTestsDone! callback handler")
            self.channelTestsDone?.succeed(())
        }
    }
    
    public func stop() throws {
        struct InboundBufferRemaining: Error {}
        
        assert(!self.eventLoop.inEventLoop)
        try self.eventLoop.flatSubmit { () -> EventLoopFuture<Void> in
            switch self.state {
            case .idle:
                self.state = .stopped
            case .waiting(let promise):
                self.state = .stopped
                promise.fail(ChannelError.ioOnClosedChannel)
            case .working(let channels):
                self.state = .stopped
                channels.forEach {
                    $0.close(promise: nil)
                }
            case .stopped:
                preconditionFailure("Already stopped")
            }
            return self.serverChannel.close().flatMapThrowing {
                self.serverChannel = nil
                guard self.inboundBuffer.isEmpty else {
                    throw InboundBufferRemaining()
                }
            }
            .always { _ in self.currentClientChannel?.close(promise: nil) }
        }
        .wait()
    }
}

extension TestServer {
    public var serverPort: Int {
        return self.serverChannel.localAddress!.port!
    }
}

extension TestServer {
    private final class ServerHandler: ChannelInboundHandler {
        public typealias InboundIn = FrameToCommandHandler.InboundOut
        
        private let btcTestServer: TestServer
        
        fileprivate init(btcTestServer: TestServer) {
            logger.trace("ServerHandler initialized")
            self.btcTestServer = btcTestServer
        }
        
        func channelActive(context: ChannelHandlerContext) {
            context.channel.writeAndFlush(NIOAny(VersionCommand(testing: nil)), promise: nil)
        }

        func channelRead(context: ChannelHandlerContext, data: NIOAny) {
            logger.trace("ServerHandler", #function, "channelRead for data(", data, ")")
            self.btcTestServer.pushHandlerRead(unwrapInboundIn(data))
        }
        
        func errorCaught(context: ChannelHandlerContext, error: Error) {
            logger.trace("ServerHandler", #function, "Error caught(", error, ")")
            self.btcTestServer.pushHandlerError(error)
            context.channel.close(mode: .all, promise: nil)
        }
    }
}
