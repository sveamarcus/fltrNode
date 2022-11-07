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
import NIO
import fltrNode
import FileRepo
import Foundation
import HaByLo
import NIOTransportServices
import NIOConcurrencyHelpers

extension NonBlockingFileIOClient {
    public static func embedded(threadPool: NIOThreadPool, elg: NIOTSEventLoopGroup) -> Self {
        let lock = NIOConcurrencyHelpers.NIOLock()
        let real = elg.next()
        let io = NonBlockingFileIO(threadPool: threadPool)
        let waitQueue = DispatchQueue.init(label: "wait queue")
        func selectEventloop(_ el: EventLoop) -> EventLoop {
            el is EmbeddedEventLoopLocked ? el : real
        }
        
        func execute<T>(on eventLoop: EventLoop,
                        command: @escaping () -> EventLoopFuture<T>) -> EventLoopFuture<T> {
            if eventLoop is EmbeddedEventLoopLocked {
                let promise = lock.withLock { eventLoop.makePromise(of: T.self) }
                
                waitQueue.async {
                    do {
                        promise.succeed(try command().wait())
                    } catch {
                        promise.fail(error)
                    }
                }
                
                return lock.withLock { promise.futureResult }
            } else {
                return real.flatSubmit(command)
                .hop(to: eventLoop)
            }
        }
        
        
        let client: Self = .init(
            changeFileSize0: { fileHandle, size, eventLoop in
//                print("changeFileSize")
                return execute(on: eventLoop) {
                    io.changeFileSize(fileHandle: fileHandle, size: size, eventLoop: real)
                }
            },
            close0: { fileHandle, eventLoop in
//                print("closing file")
                lock.withLockVoid {
                    try? fileHandle.close()
                }
                
                return eventLoop.makeSucceededFuture(())
            },
            openFile0: { name, fileHandleMode, fileHandleFlags, eventLoop in
//                print("openFile")
                return execute(on: eventLoop) {
                    io.openFile(path: name, mode: fileHandleMode, flags: fileHandleFlags, eventLoop: real)
                }
            },
            readChunkedFileHandle: { fileHandle, number1, number2, allocator, eventLoop, chunkHandler in
//                print("readChunkedFileHandle")
                let hoppedHandler: (ByteBuffer) -> Future<Void> = { bb in
                    real.flatSubmit {
                        chunkHandler(bb)
                    }
                }

                return execute(on: eventLoop) {
                    io.readChunked(fileHandle: fileHandle,
                                   byteCount: number1,
                                   chunkSize: number2,
                                   allocator: allocator,
                                   eventLoop: real,
                                   chunkHandler: hoppedHandler)
                }
            },
            readChunkedFileOffset: { fileHandle, offset, number1, number2, allocator, eventLoop, chunkHandler in
//                print("readChunkedFileOffset")
                let hoppedHandler: (ByteBuffer) -> Future<Void> = { bb in
                    real.flatSubmit {
                        chunkHandler(bb)
                    }
                }
                
                return execute(on: eventLoop) {
                    io.readChunked(fileHandle: fileHandle,
                                   fromOffset: offset,
                                   byteCount: number1,
                                   chunkSize: number2,
                                   allocator: allocator,
                                   eventLoop: real,
                                   chunkHandler: hoppedHandler)
                }
            },
            readChunkedFileRegion: { fileRegion, number1, allocator, eventLoop, chunkHandler in
//                print("readChunkedFileRegion")
                let hoppedHandler: (ByteBuffer) -> Future<Void> = { bb in
                    real.flatSubmit {
                        chunkHandler(bb)
                    }
                }
                
                return execute(on: eventLoop) {
                    io.readChunked(fileRegion: fileRegion,
                                   chunkSize: number1,
                                   allocator: allocator,
                                   eventLoop: real,
                                   chunkHandler: hoppedHandler)
                }
            },
            readFileHandle: { fileHandle, number, allocator, eventLoop in
//                print("readFileHandle")
                return execute(on: eventLoop) {
                    io.read(fileHandle: fileHandle,
                            byteCount: number,
                            allocator: allocator,
                            eventLoop: real)
                }
            },
            readFileOffset: { fileHandle, offset, number, allocator, eventLoop in
//                print("read from offset")
                return execute(on: eventLoop) {
                    io.read(fileHandle: fileHandle,
                            fromOffset: offset,
                            byteCount: number,
                            allocator: allocator,
                            eventLoop: real)
                }
            },
            readFileRegion: { fileRegion, allocator, eventLoop in
//                print("readFileRegion")
                return execute(on: eventLoop) {
                    io.read(fileRegion: fileRegion, allocator: allocator, eventLoop: real)
                }
            },
            readFileSize0: { fileHandle, eventLoop in
//                print("reading file size")
                return execute(on: eventLoop) {
                    io.readFileSize(fileHandle: fileHandle, eventLoop: real)
                }
            },
            write0: { fileHandle, offset, byteBuffer, eventLoop in
//                print("calling write0")
                return execute(on: eventLoop) {
                    io.write(fileHandle: fileHandle,
                             toOffset: offset,
                             buffer: byteBuffer,
                             eventLoop: real)
                }
            },
            sync0: { fileHandle, eventLoop -> Future<Void> in
                execute(on: eventLoop) {
                    threadPool.runIfActive(eventLoop: real) {
                        try fileHandle.withUnsafeFileDescriptor { fd in
                            _ = fcntl(fd, F_FULLFSYNC)
                        }
                    }
                }
            }
//            sync0: { fileHandle, eventLoop in
//                execute(on: eventLoop) {
//                    fileHandle.withUnsafeFileDescriptor { fd in
//                        _ = fcntl(fd, F_FULLFSYNC)
//                    }
//                }
//            }
        )
        
        return client
    }
}
