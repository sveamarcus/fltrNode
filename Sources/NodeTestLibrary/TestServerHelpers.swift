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
@testable import fltrNode
import NIO
import NIOTransportServices

public struct TestServerHelpers {
    private let group = NIOTSEventLoopGroup()
    public init() {}
}

extension TestServerHelpers {
    private final class TestClientHandler: ChannelInboundHandler {
        public typealias InboundIn = DelegatingHandler.InboundIn
        
        private let responsePromise: EventLoopPromise<DelegatingHandler.InboundIn>
        
        fileprivate init(responsePromise: EventLoopPromise<DelegatingHandler.InboundIn>) {
            self.responsePromise = responsePromise
        }
        
        public func handlerRemoved(context: ChannelHandlerContext) {
            struct HandlerRemovedBeforeReceivingFullRequestError: Error {}
            self.responsePromise.fail(HandlerRemovedBeforeReceivingFullRequestError())
        }
        
        public func channelRead(context: ChannelHandlerContext, data: NIOAny) {
            let command = self.unwrapInboundIn(data)
            responsePromise.succeed(command)
        }
        
        public func errorCaught(context: ChannelHandlerContext, error: Error) {
            self.responsePromise.fail(error)
            context.channel.close(mode: .all, promise: nil)
        }
    }
}

extension TestServerHelpers {
    public func connect(port: Int, responsePromise: EventLoopPromise<DelegatingHandler.InboundIn>) throws -> EventLoopFuture<Channel> {
        let bootstrap = NIOTSConnectionBootstrap(group: group)
        .channelInitializer { channel in
            return channel.pipeline.addHandlers([
                ByteToMessageHandler(FrameDecoder()),
                CommandWriterHandler(),
                FrameToCommandHandler(),
                PendingToValueHandler(),
                PendingCommandHandler(),
                QueueHandler(),
                TestClientHandler(responsePromise: responsePromise),
            ])
        }
        return bootstrap.connect(host: "127.0.0.1", port: port)
    }
    
    public func sendCommand(_ command: QueueHandler.OutboundIn, port: Int) throws -> Future<DelegatingHandler.InboundIn> {
        let responsePromise = self.group.next().makePromise(of: DelegatingHandler.InboundIn.self)
        let channel = try self.connect(port: port, responsePromise: responsePromise).wait()
        logger.trace("TestServerHelpers", #function, "test server connected on channel(", channel.remoteAddress!, ")")
        channel.writeAndFlush(command, promise: nil)
        return responsePromise.futureResult
    }
}
