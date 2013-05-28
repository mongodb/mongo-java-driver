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
 */

package org.mongodb.connection;

import org.bson.io.BasicInputBuffer;
import org.bson.io.InputBuffer;
import org.mongodb.MongoException;
import org.mongodb.MongoInternalException;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;

import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.connection.ReplyHeader.REPLY_HEADER_LENGTH;

class DefaultAsyncConnection implements AsyncConnection {
    private static final int MAXIMUM_EXPECTED_REPLY_MESSAGE_LENGTH = 48000000;

    private final ServerAddress serverAddress;
    private final BufferPool<ByteBuffer> bufferPool;
    private volatile AsynchronousSocketChannel channel;
    private volatile boolean isClosed;

    public DefaultAsyncConnection(final ServerAddress serverAddress, final BufferPool<ByteBuffer> bufferPool) {
        this.serverAddress = serverAddress;
        this.bufferPool = bufferPool;
    }

    @Override
    public ServerAddress getServerAddress() {
        return serverAddress;
    }

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

    public void sendMessage(final ChannelAwareOutputBuffer buffer, final SingleResultCallback<ResponseBuffers> callback) {
        isTrue("open", !isClosed());
        sendOneWayMessage(buffer, new AsyncCompletionHandler() {
            @Override
            public void completed(final int bytesWritten) {
                callback.onResult(null, null);
            }

            @Override
            public void failed(final Throwable t) {
                callback.onResult(null, new MongoInternalException("", t));  // TODO
            }
        });
    }

    public void sendAndReceiveMessage(final ChannelAwareOutputBuffer buffer, final SingleResultCallback<ResponseBuffers> callback) {
        isTrue("open", !isClosed());
        sendOneWayMessage(buffer, new ReceiveMessageCompletionHandler(System.nanoTime(), callback));
    }

    @Override
    public void receiveMessage(final SingleResultCallback<ResponseBuffers> callback) {
        fillAndFlipBuffer(bufferPool.get(REPLY_HEADER_LENGTH), new ResponseHeaderCallback(callback, System.nanoTime()));
    }

    private void receiveMessage(final long start, final SingleResultCallback<ResponseBuffers> callback) {
        fillAndFlipBuffer(bufferPool.get(REPLY_HEADER_LENGTH), new ResponseHeaderCallback(callback, start));
    }

    private void sendOneWayMessage(final ChannelAwareOutputBuffer buffer, final AsyncCompletionHandler handler) {
        buffer.pipeAndClose(new AsyncWritableByteChannelAdapter(), handler);
    }

    private void fillAndFlipBuffer(final ByteBuffer buffer, final SingleResultCallback<ByteBuffer> callback) {
        channel.read(buffer, null, new BasicCompletionHandler(buffer, callback));
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
                channel.read(dst, null, new BasicCompletionHandler(dst, callback));
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
            if (channel != null) {
                handler.completed(0);
            }
            else {
                channel = AsynchronousSocketChannel.open();
                channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
                channel.connect(serverAddress.getSocketAddress(), null, new CompletionHandler<Void, Object>() {
                    @Override
                    public void completed(final Void result, final Object attachment) {
                        handler.completed(0);
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

    private class AsyncWritableByteChannelAdapter implements AsyncWritableByteChannel {

        @Override
        public void write(final ByteBuffer src, final AsyncCompletionHandler handler) {
            ensureOpen(new AsyncCompletionHandler() {
                @Override
                public void completed(final int bytesWritten) {
                    channel.write(src, null, new CompletionHandler<Integer, Object>() {
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

                final ReplyHeader replyHeader = new ReplyHeader(headerInputBuffer);

                bufferPool.release(result);

                if (replyHeader.getMessageLength() == REPLY_HEADER_LENGTH) {
                    callback.onResult(new ResponseBuffers(replyHeader, null, System.nanoTime() - start), null);
                }
                else {
                    if (replyHeader.getMessageLength() > MAXIMUM_EXPECTED_REPLY_MESSAGE_LENGTH) {
                        callback.onResult(null,
                                new MongoInternalException(String.format("Unexpectedly large message length of %d exceeds maximum of %d",
                                replyHeader.getMessageLength(), MAXIMUM_EXPECTED_REPLY_MESSAGE_LENGTH)));
                    }

                    fillAndFlipBuffer(bufferPool.get(replyHeader.getMessageLength() - REPLY_HEADER_LENGTH),
                            new ResponseBodyCallback(replyHeader));
                }
            }
        }

        private class ResponseBodyCallback implements SingleResultCallback<ByteBuffer> {
            private final ReplyHeader replyHeader;

            public ResponseBodyCallback(final ReplyHeader replyHeader) {
                this.replyHeader = replyHeader;
            }

            @Override
            public void onResult(final ByteBuffer result, final MongoException e) {
                if (e != null) {
                    callback.onResult(null, e);
                }
                else {
                    PooledInputBuffer bodyInputBuffer = new PooledInputBuffer(result, bufferPool);
                    callback.onResult(new ResponseBuffers(replyHeader, bodyInputBuffer, System.nanoTime() - start), null);
                }
            }
        }
    }
}
