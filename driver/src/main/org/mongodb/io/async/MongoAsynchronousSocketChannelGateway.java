/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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
 */

package org.mongodb.io.async;

import org.bson.io.BasicInputBuffer;
import org.bson.io.InputBuffer;
import org.mongodb.MongoException;
import org.mongodb.ServerAddress;
import org.mongodb.async.SingleResultCallback;
import org.mongodb.io.BufferPool;
import org.mongodb.io.CachingAuthenticator;
import org.mongodb.io.ChannelAwareOutputBuffer;
import org.mongodb.io.MongoSocketOpenException;
import org.mongodb.io.PooledInputBuffer;
import org.mongodb.io.ResponseBuffers;
import org.mongodb.protocol.MongoReplyHeader;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;

import static org.mongodb.protocol.MongoReplyHeader.REPLY_HEADER_LENGTH;

/**
 * An asynchronous gateway for the MongoDB protocol.
 * <p/>
 * Note: This class is not part of the public API.  It may break binary compatibility even in minor releases.
 */
public class MongoAsynchronousSocketChannelGateway {
    private final ServerAddress address;
    private CachingAuthenticator authenticator;
    private final BufferPool<ByteBuffer> pool;
    private volatile AsynchronousSocketChannel asynchronousSocketChannel;

    public MongoAsynchronousSocketChannelGateway(final ServerAddress address, final CachingAuthenticator authenticator,
                                                 final BufferPool<ByteBuffer> pool) {
        this.address = address;
        this.authenticator = authenticator;
        this.pool = pool;
    }

    public ServerAddress getAddress() {
        return address;
    }

    public void sendMessage(final ChannelAwareOutputBuffer buffer, final SingleResultCallback<ResponseBuffers> callback) {
        sendOneWayMessage(buffer, new AsyncCompletionHandler() {
            @Override
            public void completed(final int bytesWritten) {
                callback.onResult(null, null);
            }

            @Override
            public void failed(final Throwable t) {
                callback.onResult(null, new MongoException("", t));  // TODO
            }
        });
    }

    public void sendAndReceiveMessage(final ChannelAwareOutputBuffer buffer, final SingleResultCallback<ResponseBuffers> callback) {
        sendOneWayMessage(buffer, new ReceiveMessageCompletionHandler(System.nanoTime(), callback));
    }

    private void receiveMessage(final long start, final SingleResultCallback<ResponseBuffers> callback) {
        fillAndFlipBuffer(pool.get(REPLY_HEADER_LENGTH), new ResponseHeaderCallback(callback, start));
    }

    public void sendMessage(final ChannelAwareOutputBuffer buffer) {
        sendOneWayMessage(buffer, new NoOpAsyncCompletionHandler());
    }

    private void sendOneWayMessage(final ChannelAwareOutputBuffer buffer, final AsyncCompletionHandler handler) {
        buffer.pipeAndClose(new AsyncWritableByteChannelAdapter(), handler);
    }

    private void fillAndFlipBuffer(final ByteBuffer buffer, final SingleResultCallback<ByteBuffer> callback) {
        asynchronousSocketChannel.read(buffer, null, new BasicCompletionHandler(buffer, callback));
    }

    private final class BasicCompletionHandler implements CompletionHandler<Integer, Void> {
        private final ByteBuffer dst;
        private final SingleResultCallback<ByteBuffer> callback;

        private BasicCompletionHandler(final ByteBuffer dst, final SingleResultCallback<ByteBuffer> callback) {
            this.dst = dst;
            this.callback = callback;
        }

        @Override
        public void completed(final Integer result, final Void attachment) {
            if (!dst.hasRemaining()) {
                dst.flip();
                callback.onResult(dst, null);
            }
            else {
                asynchronousSocketChannel.read(dst, null, new BasicCompletionHandler(dst, callback));
            }
        }

        @Override
        public void failed(final Throwable t, final Void attachment) {
            // TODO: need a proper subclass for the exception
            callback.onResult(null, new MongoException("Exception reading from channel", t));
        }
    }

    private void ensureOpen(final AsyncCompletionHandler handler) {
        try {
            if (asynchronousSocketChannel != null) {
                handler.completed(0);
            }
            else {
                asynchronousSocketChannel = AsynchronousSocketChannel.open();
                asynchronousSocketChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
                asynchronousSocketChannel.connect(address.getSocketAddress(), null, new CompletionHandler<Void, Object>() {
                    @Override
                    public void completed(final Void result, final Object attachment) {
                        authenticator.asyncAuthenticateAll(new SingleResultCallback<Void>() {
                            @Override
                            public void onResult(final Void result, final MongoException e) {
                                if (e != null) {
                                    handler.failed(e);
                                }
                                else {
                                    handler.completed(0);
                                }
                            }
                        });
                    }

                    @Override
                    public void failed(final Throwable exc, final Object attachment) {
                        handler.failed(exc);
                    }
                });
            }
        } catch (IOException e) {
            throw new MongoSocketOpenException("Exception opening socket", address, e);
        }
    }

    //CHECKSTYLE:OFF
    public void close() {
        try {
            if (asynchronousSocketChannel != null) {
                asynchronousSocketChannel.close();
                asynchronousSocketChannel = null;
            }
        } catch (IOException e) { // NOPMD
            // ignore
        }
    }
    //CHECKSTYLE:ON

    private class AsyncWritableByteChannelAdapter implements AsyncWritableByteChannel {

        @Override
        public void write(final ByteBuffer src, final AsyncCompletionHandler handler) {
            ensureOpen(new AsyncCompletionHandler() {
                @Override
                public void completed(final int bytesWritten) {
                    asynchronousSocketChannel.write(src, null, new CompletionHandler<Integer, Object>() {
                        @Override
                        public void completed(final Integer result, final Object attachment) {
                            handler.completed(result);
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

    private class ReceiveMessageCompletionHandler implements AsyncCompletionHandler {
        private final long start;
        private final SingleResultCallback<ResponseBuffers> callback;

        public ReceiveMessageCompletionHandler(final long start, final SingleResultCallback<ResponseBuffers> callback) {
            this.start = start;
            this.callback = callback;
        }

        @Override
        public void completed(final int bytesWritten) {
            receiveMessage(start, callback);
        }

        @Override
        public void failed(final Throwable t) {
            callback.onResult(null, new MongoException("", t));  // TODO
        }
    }

    private class ResponseHeaderCallback implements SingleResultCallback<ByteBuffer> {
        private final SingleResultCallback<ResponseBuffers> callback;
        private final long start;

        public ResponseHeaderCallback(final SingleResultCallback<ResponseBuffers> callback, final long start) {
            this.callback = callback;
            this.start = start;
        }

        @Override
        public void onResult(final ByteBuffer result, final MongoException e) {
            if (e != null) {
                callback.onResult(null, e);
            }
            else {
                final InputBuffer headerInputBuffer = new BasicInputBuffer(result);

                final MongoReplyHeader replyHeader = new MongoReplyHeader(headerInputBuffer);

                pool.done(result);

                if (replyHeader.getMessageLength() == REPLY_HEADER_LENGTH) {
                    callback.onResult(new ResponseBuffers(replyHeader, null, System.nanoTime() - start), null);
                }
                else {
                    fillAndFlipBuffer(pool.get(replyHeader.getMessageLength() - REPLY_HEADER_LENGTH), new ResponseBodyCallback(replyHeader));
                }
            }
        }

        private class ResponseBodyCallback implements SingleResultCallback<ByteBuffer> {
            private final MongoReplyHeader replyHeader;

            public ResponseBodyCallback(final MongoReplyHeader replyHeader) {
                this.replyHeader = replyHeader;
            }

            @Override
            public void onResult(final ByteBuffer result, final MongoException e) {
                if (e != null) {
                    callback.onResult(null, e);
                }
                else {
                    PooledInputBuffer bodyInputBuffer = new PooledInputBuffer(result, pool);
                    try {
                        callback.onResult(new ResponseBuffers(replyHeader, bodyInputBuffer, System.nanoTime() - start), null);
                    } catch (Throwable t) {
                        callback.onResult(null, new MongoException("", t)); // TODO: proper subclass
                    }
                }
            }
        }
    }

    private static class NoOpAsyncCompletionHandler implements AsyncCompletionHandler {
        @Override
        public void completed(final int bytesWritten) {
        }

        @Override
        public void failed(final Throwable t) {
        }
    }
}
