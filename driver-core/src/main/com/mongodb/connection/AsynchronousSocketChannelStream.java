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

package com.mongodb.connection;

import com.mongodb.MongoSocketOpenException;
import com.mongodb.MongoSocketReadTimeoutException;
import com.mongodb.ServerAddress;
import org.bson.ByteBuf;

import java.io.IOException;
import java.net.ConnectException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.InterruptedByTimeoutException;
import java.util.Iterator;
import java.util.List;

import static com.mongodb.assertions.Assertions.isTrue;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

final class AsynchronousSocketChannelStream implements Stream {
    private final ServerAddress serverAddress;
    private final SocketSettings settings;
    private final BufferProvider bufferProvider;
    private volatile AsynchronousSocketChannel channel;
    private volatile boolean isClosed;

    AsynchronousSocketChannelStream(final ServerAddress serverAddress, final SocketSettings settings,
                                    final BufferProvider bufferProvider) {
        this.serverAddress = serverAddress;
        this.settings = settings;
        this.bufferProvider = bufferProvider;
    }

    @Override
    public ByteBuf getBuffer(final int size) {
        return bufferProvider.getBuffer(size);
    }

    @Override
    public void open() throws IOException {
        FutureAsyncCompletionHandler<Void> handler = new FutureAsyncCompletionHandler<Void>();
        openAsync(handler);
        handler.getOpen();
    }

    @Override
    public void openAsync(final AsyncCompletionHandler<Void> handler) {
        isTrue("unopened", channel == null);
        try {
            channel = AsynchronousSocketChannel.open();
            channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
            channel.setOption(StandardSocketOptions.SO_KEEPALIVE, settings.isKeepAlive());
            if (settings.getReceiveBufferSize() > 0) {
                channel.setOption(StandardSocketOptions.SO_RCVBUF, settings.getReceiveBufferSize());
            }
            if (settings.getSendBufferSize() > 0) {
                channel.setOption(StandardSocketOptions.SO_SNDBUF, settings.getSendBufferSize());
            }

            channel.connect(serverAddress.getSocketAddress(), null, new CompletionHandler<Void, Object>() {
                @Override
                public void completed(final Void result, final Object attachment) {
                    handler.completed(null);
                }

                @Override
                public void failed(final Throwable exc, final Object attachment) {
                    if (exc instanceof ConnectException) {
                        handler.failed(new MongoSocketOpenException("Exception opening socket", getAddress(), exc));
                    } else {
                        handler.failed(exc);
                    }
                }
            });
        } catch (IOException e) {
            handler.failed(new MongoSocketOpenException("Exception opening socket", serverAddress, e));
        } catch (Throwable t) {
            handler.failed(t);
        }
    }

    @Override
    public void write(final List<ByteBuf> buffers) throws IOException {
        FutureAsyncCompletionHandler<Void> handler = new FutureAsyncCompletionHandler<Void>();
        writeAsync(buffers, handler);
        handler.getWrite();
    }

    @Override
    public ByteBuf read(final int numBytes) throws IOException {
        FutureAsyncCompletionHandler<ByteBuf> handler = new FutureAsyncCompletionHandler<ByteBuf>();
        readAsync(numBytes, handler);
        return handler.getRead();
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
        channel.read(buffer.asNIO(), settings.getReadTimeout(MILLISECONDS), MILLISECONDS, null,
                     new BasicCompletionHandler(buffer, handler));
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
                channel.read(dst.asNIO(), settings.getReadTimeout(MILLISECONDS), MILLISECONDS, null,
                             new BasicCompletionHandler(dst, handler));
            }
        }

        @Override
        public void failed(final Throwable t, final Void attachment) {
            dst.release();
            if (t instanceof InterruptedByTimeoutException) {
                handler.failed(new MongoSocketReadTimeoutException("Timeout while receiving message", serverAddress, t));
            } else {
                handler.failed(t);
            }
        }
    }
}
