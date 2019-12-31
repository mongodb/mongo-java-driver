/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Original Work: MIT License, Copyright (c) [2015-2018] all contributors
 * https://github.com/marianobarrios/tls-channel
 */

package com.mongodb.internal.connection.tlschannel.async;

import com.mongodb.internal.connection.ExtendedAsynchronousByteChannel;
import com.mongodb.internal.connection.tlschannel.TlsChannel;
import com.mongodb.internal.connection.tlschannel.impl.ByteBufferSet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousByteChannel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.CompletionHandler;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

/**
 * An {@link AsynchronousByteChannel} that works using {@link TlsChannel}s.
 */
public class AsynchronousTlsChannel implements ExtendedAsynchronousByteChannel {

    private class FutureReadResult extends CompletableFuture<Integer> {
        AsynchronousTlsChannelGroup.ReadOperation op;

        @Override
        public boolean cancel(final boolean mayInterruptIfRunning) {
            super.cancel(mayInterruptIfRunning);
            return group.doCancelRead(registeredSocket, op);
        }
    }

    private class FutureWriteResult extends CompletableFuture<Integer> {
        AsynchronousTlsChannelGroup.WriteOperation op;

        @Override
        public boolean cancel(final boolean mayInterruptIfRunning) {
            super.cancel(mayInterruptIfRunning);
            return group.doCancelWrite(registeredSocket, op);
        }
    }

    private final AsynchronousTlsChannelGroup group;
    private final TlsChannel tlsChannel;
    private final AsynchronousTlsChannelGroup.RegisteredSocket registeredSocket;

    /**
     * Initializes a new instance of this class.
     *
     * @param channelGroup  group to associate new new channel to
     * @param tlsChannel    existing TLS channel to be used asynchronously
     * @param socketChannel underlying socket
     * @throws ClosedChannelException   if any of the underlying channels are closed.
     * @throws IllegalArgumentException is the socket is in blocking mode
     */
    public AsynchronousTlsChannel(
            final AsynchronousTlsChannelGroup channelGroup,
            final TlsChannel tlsChannel,
            final SocketChannel socketChannel) throws ClosedChannelException, IllegalArgumentException {
        if (!socketChannel.isOpen()) {
            throw new ClosedChannelException();
        }
        if (!tlsChannel.isOpen()) {
            throw new ClosedChannelException();
        }
        if (socketChannel.isBlocking()) {
            throw new IllegalArgumentException("socket channel must be in non-blocking mode");
        }
        this.group = channelGroup;
        this.tlsChannel = tlsChannel;
        this.registeredSocket = channelGroup.registerSocket(tlsChannel, socketChannel);
    }

    @Override
    public <A> void read(
            final ByteBuffer dst,
            final A attach, final CompletionHandler<Integer, ? super A> handler) {
        checkReadOnly(dst);
        if (!dst.hasRemaining()) {
            completeWithZeroInt(attach, handler);
            return;
        }
        group.startRead(
                registeredSocket,
                new ByteBufferSet(dst),
                0, TimeUnit.MILLISECONDS,
                new LongConsumer() {
                    @Override
                    public void accept(final long c) {
                        group.executor.submit(new Runnable() {
                            @Override
                            public void run() {
                                handler.completed((int) c, attach);
                            }
                        });
                    }
                },
                new Consumer<Throwable>() {
                    @Override
                    public void accept(final Throwable e) {
                        group.executor.submit(new Runnable() {
                            @Override
                            public void run() {
                                handler.failed(e, attach);
                            }
                        });
                    }
                });
    }

    @Override
    public <A> void read(
            final ByteBuffer dst,
            final long timeout, final TimeUnit unit,
            final A attach, final CompletionHandler<Integer, ? super A> handler) {
        checkReadOnly(dst);
        if (!dst.hasRemaining()) {
            completeWithZeroInt(attach, handler);
            return;
        }
        group.startRead(
                registeredSocket,
                new ByteBufferSet(dst),
                timeout, unit,
                new LongConsumer() {
                    @Override
                    public void accept(final long c) {
                        group.executor.submit(new Runnable() {
                            @Override
                            public void run() {
                                handler.completed((int) c, attach);
                            }
                        });
                    }
                },
                new Consumer<Throwable>() {
                    @Override
                    public void accept(final Throwable e) {
                        group.executor.submit(new Runnable() {
                            @Override
                            public void run() {
                                handler.failed(e, attach);
                            }
                        });
                    }
                });
    }

    @Override
    public <A> void read(
            final ByteBuffer[] dsts, final int offset, final int length,
            final long timeout, final TimeUnit unit,
            final A attach, final CompletionHandler<Long, ? super A> handler) {
        ByteBufferSet bufferSet = new ByteBufferSet(dsts, offset, length);
        if (bufferSet.isReadOnly()) {
            throw new IllegalArgumentException("buffer is read-only");
        }
        if (!bufferSet.hasRemaining()) {
            completeWithZeroLong(attach, handler);
            return;
        }
        group.startRead(
                registeredSocket,
                bufferSet,
                timeout, unit,
                new LongConsumer() {
                    @Override
                    public void accept(final long c) {
                        group.executor.submit(new Runnable() {
                            @Override
                            public void run() {
                                handler.completed(c, attach);
                            }
                        });
                    }
                },
                new Consumer<Throwable>() {
                    @Override
                    public void accept(final Throwable e) {
                        group.executor.submit(new Runnable() {
                            @Override
                            public void run() {
                                handler.failed(e, attach);
                            }
                        });
                    }
                });
    }

    @Override
    public Future<Integer> read(final ByteBuffer dst) {
        checkReadOnly(dst);
        if (!dst.hasRemaining()) {
            return CompletableFuture.completedFuture(0);
        }
        final FutureReadResult future = new FutureReadResult();
        future.op = group.startRead(
                registeredSocket,
                new ByteBufferSet(dst),
                0, TimeUnit.MILLISECONDS,
                new LongConsumer() {
                    @Override
                    public void accept(final long c) {
                        future.complete((int) c);
                    }
                },
                new Consumer<Throwable>() {
                    @Override
                    public void accept(final Throwable ex) {
                        future.completeExceptionally(ex);
                    }
                });
        return future;
    }

    private void checkReadOnly(final ByteBuffer dst) {
        if (dst.isReadOnly()) {
            throw new IllegalArgumentException("buffer is read-only");
        }
    }

    @Override
    public <A> void write(final ByteBuffer src, final A attach, final CompletionHandler<Integer, ? super A> handler) {
        if (!src.hasRemaining()) {
            completeWithZeroInt(attach, handler);
            return;
        }
        group.startWrite(
                registeredSocket,
                new ByteBufferSet(src),
                0, TimeUnit.MILLISECONDS,
                new LongConsumer() {
                    @Override
                    public void accept(final long c) {
                        group.executor.submit(new Runnable() {
                            @Override
                            public void run() {
                                handler.completed((int) c, attach);
                            }
                        });
                    }
                },
                new Consumer<Throwable>() {
                    @Override
                    public void accept(final Throwable e) {
                        group.executor.submit(new Runnable() {
                            @Override
                            public void run() {
                                handler.failed(e, attach);
                            }
                        });
                    }
                });
    }

    @Override
    public <A> void write(
            final ByteBuffer src,
            final long timeout, final TimeUnit unit,
            final A attach, final CompletionHandler<Integer, ? super A> handler) {
        if (!src.hasRemaining()) {
            completeWithZeroInt(attach, handler);
            return;
        }
        group.startWrite(
                registeredSocket,
                new ByteBufferSet(src),
                timeout, unit,
                new LongConsumer() {
                    @Override
                    public void accept(final long c) {
                        group.executor.submit(new Runnable() {
                            @Override
                            public void run() {
                                handler.completed((int) c, attach);
                            }
                        });
                    }
                },
                new Consumer<Throwable>() {
                    @Override
                    public void accept(final Throwable e) {
                        group.executor.submit(new Runnable() {
                            @Override
                            public void run() {
                                handler.failed(e, attach);
                            }
                        });
                    }
                });
    }

    @Override
    public <A> void write(
            final ByteBuffer[] srcs, final int offset, final int length,
            final long timeout, final TimeUnit unit,
            final A attach, final CompletionHandler<Long, ? super A> handler) {
        ByteBufferSet bufferSet = new ByteBufferSet(srcs, offset, length);
        if (!bufferSet.hasRemaining()) {
            completeWithZeroLong(attach, handler);
            return;
        }
        group.startWrite(
                registeredSocket,
                bufferSet,
                timeout, unit,
                new LongConsumer() {
                    @Override
                    public void accept(final long c) {
                        group.executor.submit(new Runnable() {
                            @Override
                            public void run() {
                                handler.completed(c, attach);
                            }
                        });
                    }
                },
                new Consumer<Throwable>() {
                    @Override
                    public void accept(final Throwable e) {
                        group.executor.submit(new Runnable() {
                            @Override
                            public void run() {
                                handler.failed(e, attach);
                            }
                        });
                    }
                });
    }

    @Override
    public Future<Integer> write(final ByteBuffer src) {
        if (!src.hasRemaining()) {
            return CompletableFuture.completedFuture(0);
        }
        final FutureWriteResult future = new FutureWriteResult();
        future.op = group.startWrite(
                registeredSocket,
                new ByteBufferSet(src),
                0, TimeUnit.MILLISECONDS,
                new LongConsumer() {
                    @Override
                    public void accept(final long c) {
                        future.complete((int) c);
                    }
                },
                new Consumer<Throwable>() {
                    @Override
                    public void accept(final Throwable ex) {
                        future.completeExceptionally(ex);
                    }
                });
        return future;
    }

    private <A> void completeWithZeroInt(final A attach, final CompletionHandler<Integer, ? super A> handler) {
        group.executor.submit(new Runnable() {
            @Override
            public void run() {
                handler.completed(0, attach);
            }
        });
    }

    private <A> void completeWithZeroLong(final A attach, final CompletionHandler<Long, ? super A> handler) {
        group.executor.submit(new Runnable() {
            @Override
            public void run() {
                handler.completed(0L, attach);
            }
        });
    }

    /**
     * Tells whether or not this channel is open.
     *
     * @return <tt>true</tt> if, and only if, this channel is open
     */
    @Override
    public boolean isOpen() {
        return tlsChannel.isOpen();
    }

    /**
     * Closes this channel.
     *
     * <p>This method will close the underlying {@link TlsChannel} and also deregister it from its group.</p>
     *
     * @throws IOException If an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        tlsChannel.close();
        registeredSocket.close();
    }
}
