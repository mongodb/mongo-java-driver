/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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
 */

package org.mongodb.connection;

import org.bson.ByteBuf;
import org.mongodb.operation.SingleResultFuture;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.Iterator;
import java.util.List;

final class AsynchronousSocketChannelStream implements Stream {
    private final ServerAddress serverAddress;
    private final BufferProvider bufferProvider;
    private volatile AsynchronousSocketChannel channel;
    private volatile boolean isClosed;

    AsynchronousSocketChannelStream(final ServerAddress serverAddress, final BufferProvider bufferProvider) {
        this.serverAddress = serverAddress;
        this.bufferProvider = bufferProvider;
    }

    @Override
    public ByteBuf getBuffer(final int size) {
        return bufferProvider.getBuffer(size);
    }

    @Override
    public void write(final List<ByteBuf> buffers) throws IOException {
        SingleResultFuture<Void> future = new SingleResultFuture<Void>();
        writeAsync(buffers, new FutureAsyncCompletionHandler<Void>(future));
        future.get();
    }

    @Override
    public ByteBuf read(final int numBytes) throws IOException {
        SingleResultFuture<ByteBuf> future = new SingleResultFuture<ByteBuf>();
        readAsync(numBytes, new FutureAsyncCompletionHandler<ByteBuf>(future));
        return future.get();
    }

    @Override
    public void writeAsync(final List<ByteBuf> buffers, final AsyncCompletionHandler<Void> handler) {
        final AsyncWritableByteChannel byteChannel = new AsyncWritableByteChannelAdapter();
        final Iterator<ByteBuf> iter = buffers.iterator();
        pipeOneBuffer(byteChannel, iter.next(), new AsyncCompletionHandler<Void>() {
            @Override
            public void completed(final Void t) {
                if (iter.hasNext()) {
                    pipeOneBuffer(byteChannel, iter.next(), this);
                } else {
                    handler.completed(null);
                }
            }

            @Override
            public void failed(final Throwable t) {
                handler.failed(t);
            }
        });
    }

    @Override
    public void readAsync(final int numBytes, final AsyncCompletionHandler<ByteBuf> handler) {
        ByteBuf buffer = bufferProvider.getBuffer(numBytes);
        channel.read(buffer.asNIO(), null, new BasicCompletionHandler(buffer, handler));
    }

    @Override
    public ServerAddress getAddress() {
        return serverAddress;
    }

    /**
     * Closes the connection.
     */
    @Override
    public void close() {
        try {
            if (channel != null) {
                channel.close();
            }
        } catch (IOException e) { // NOPMD
            // ignore
        } finally {
            channel = null;
            isClosed = true;
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    void ensureOpen(final AsyncCompletionHandler<Void> handler) {
        try {
            if (channel != null) {
                handler.completed(null);
            } else {
                channel = AsynchronousSocketChannel.open();
                channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
                channel.connect(serverAddress.getSocketAddress(), null, new CompletionHandler<Void, Object>() {
                    @Override
                    public void completed(final Void result, final Object attachment) {
                        handler.completed(null);
                    }

                    @Override
                    public void failed(final Throwable exc, final Object attachment) {
                        handler.failed(exc);
                    }
                });
            }
        } catch (IOException e) {
            throw new MongoSocketOpenException("Exception opening socket", serverAddress, e);
        }
    }

    private void pipeOneBuffer(final AsyncWritableByteChannel byteChannel, final ByteBuf byteBuffer,
                               final AsyncCompletionHandler<Void> outerHandler) {
        byteChannel.write(byteBuffer.asNIO(), new AsyncCompletionHandler<Void>() {
            @Override
            public void completed(final Void t) {
                if (byteBuffer.hasRemaining()) {
                    byteChannel.write(byteBuffer.asNIO(), this);
                } else {
                    outerHandler.completed(null);
                }
            }

            @Override
            public void failed(final Throwable t) {
                outerHandler.failed(t);
            }
        });
    }

    private class AsyncWritableByteChannelAdapter implements AsyncWritableByteChannel {
        @Override
        public void write(final ByteBuffer src, final AsyncCompletionHandler<Void> handler) {
            ensureOpen(new AsyncCompletionHandler<Void>() {
                @Override
                public void completed(final Void t) {
                    channel.write(src, null, new CompletionHandler<Integer, Object>() {
                        @Override
                        public void completed(final Integer result, final Object attachment) {
                            handler.completed(null);
                        }

                        @Override
                        public void failed(final Throwable exc, final Object attachment) {
                            handler.failed(exc);
                        }
                    });
                }

                @Override
                public void failed(final Throwable t) {
                    handler.failed(t);
                }
            });
        }
    }

    private final class BasicCompletionHandler implements CompletionHandler<Integer, Void> {
        private final ByteBuf dst;
        private final AsyncCompletionHandler<ByteBuf> handler;

        private BasicCompletionHandler(final ByteBuf dst, final AsyncCompletionHandler<ByteBuf> handler) {
            this.dst = dst;
            this.handler = handler;
        }

        @Override
        public void completed(final Integer result, final Void attachment) {
            if (!dst.hasRemaining()) {
                dst.flip();
                handler.completed(dst);
            } else {
                channel.read(dst.asNIO(), null, new BasicCompletionHandler(dst, handler));
            }
        }

        @Override
        public void failed(final Throwable t, final Void attachment) {
            handler.failed(t);
        }
    }
}
