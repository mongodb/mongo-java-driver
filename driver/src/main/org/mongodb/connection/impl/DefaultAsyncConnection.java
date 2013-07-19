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


package org.mongodb.connection.impl;


import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.Iterator;
import java.util.List;

import org.bson.ByteBuf;
import org.bson.io.BasicInputBuffer;
import org.mongodb.MongoException;
import org.mongodb.MongoInternalException;
import org.mongodb.connection.AsyncConnection;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.MongoSocketOpenException;
import org.mongodb.connection.MongoSocketReadException;
import org.mongodb.connection.ReplyHeader;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ResponseSettings;
import org.mongodb.connection.ServerAddress;
import org.mongodb.connection.SingleResultCallback;

import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.connection.ReplyHeader.REPLY_HEADER_LENGTH;


class DefaultAsyncConnection implements AsyncConnection {

    private final ServerAddress serverAddress;
    private final BufferProvider bufferProvider;
    private volatile AsynchronousSocketChannel channel;
    private volatile boolean isClosed;

    public DefaultAsyncConnection(final ServerAddress serverAddress, final BufferProvider bufferProvider) {
        this.serverAddress = serverAddress;
        this.bufferProvider = bufferProvider;
    }

    @Override
    public ServerAddress getServerAddress() {
        return serverAddress;
    }

    public BufferProvider getBufferProvider() {
        return bufferProvider;
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

    public void sendMessage(final List<ByteBuf> byteBuffers, final SingleResultCallback<Void> callback) {
        isTrue("open", !isClosed());
        sendOneWayMessage(byteBuffers, new AsyncCompletionHandler() {
            @Override
            public void completed() {
                callback.onResult(null, null);
            }

            @Override
            public void failed(final Throwable t) {
                callback.onResult(null, new MongoInternalException("", t));  // TODO
            }
        });
    }

    @Override
    public void receiveMessage(final ResponseSettings responseSettings, final SingleResultCallback<ResponseBuffers> callback) {
        fillAndFlipBuffer(bufferProvider.get(REPLY_HEADER_LENGTH), new ResponseHeaderCallback(responseSettings, System.nanoTime(),
            callback));
    }

    void fillAndFlipBuffer(final ByteBuf buffer, final SingleResultCallback<ByteBuf> callback) {
        channel.read(buffer.asNIO(), null, new BasicCompletionHandler(buffer, callback));
    }

    private void sendOneWayMessage(final List<ByteBuf> byteBuffers, final AsyncCompletionHandler handler) {
        final AsyncWritableByteChannel byteChannel = new AsyncWritableByteChannelAdapter();
        final Iterator<ByteBuf> iter = byteBuffers.iterator();
        pipeOneBuffer(byteChannel, iter.next(), new AsyncCompletionHandler() {
            @Override
            public void completed() {
                if (iter.hasNext()) {
                    pipeOneBuffer(byteChannel, iter.next(), this);
                } else {
                    handler.completed();
                }
            }

            @Override
            public void failed(final Throwable t) {
                handler.failed(t);
            }
        });
    }

    private void pipeOneBuffer(final AsyncWritableByteChannel byteChannel, final ByteBuf byteBuffer,
        final AsyncCompletionHandler outerHandler) {
        byteChannel.write(byteBuffer.asNIO(), new AsyncCompletionHandler() {
            @Override
            public void completed() {
                if (byteBuffer.hasRemaining()) {
                    byteChannel.write(byteBuffer.asNIO(), this);
                } else {
                    outerHandler.completed();
                }
            }

            @Override
            public void failed(final Throwable t) {
                outerHandler.failed(t);
            }
        });
    }

    private final class BasicCompletionHandler implements CompletionHandler<Integer, Void> {
        private final ByteBuf dst;
        private final SingleResultCallback<ByteBuf> callback;

        private BasicCompletionHandler(final ByteBuf dst, final SingleResultCallback<ByteBuf> callback) {
            this.dst = dst;
            this.callback = callback;
        }

        @Override
        public void completed(final Integer result, final Void attachment) {
            if (!dst.hasRemaining()) {
                dst.flip();
                callback.onResult(dst, null);
            } else {
                channel.read(dst.asNIO(), null, new BasicCompletionHandler(dst, callback));
            }
        }

        @Override
        public void failed(final Throwable t, final Void attachment) {
            callback.onResult(null, new MongoSocketReadException("Exception reading from channel", getServerAddress(), t));
        }
    }

    void ensureOpen(final AsyncCompletionHandler handler) {
        try {
            if (channel != null) {
                handler.completed();
            } else {
                channel = AsynchronousSocketChannel.open();
                channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
                channel.connect(serverAddress.getSocketAddress(), null, new CompletionHandler<Void, Object>() {
                    @Override
                    public void completed(final Void result, final Object attachment) {
                        handler.completed();
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
                public void completed() {
                    channel.write(src, null, new CompletionHandler<Integer, Object>() {
                        @Override
                        public void completed(final Integer result, final Object attachment) {
                            handler.completed();
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

    private class ResponseHeaderCallback implements SingleResultCallback<ByteBuf> {
        private ResponseSettings responseSettings;
        private final SingleResultCallback<ResponseBuffers> callback;
        private final long start;

        public ResponseHeaderCallback(final ResponseSettings responseSettings, final long start,
            final SingleResultCallback<ResponseBuffers> callback) {
            this.responseSettings = responseSettings;
            this.callback = callback;
            this.start = start;
        }

        @Override
        public void onResult(final ByteBuf result, final MongoException e) {
            if (e != null) {
                callback.onResult(null, e);
            } else {
                ReplyHeader replyHeader;
                final BasicInputBuffer headerInputBuffer = new BasicInputBuffer(result);
                try {
                    replyHeader = new ReplyHeader(headerInputBuffer);
                } finally {
                    headerInputBuffer.close();
                }

                if (replyHeader.getResponseTo() != responseSettings.getResponseTo()) {
                    callback.onResult(null, new MongoInternalException(String.format(
                        "The responseTo (%d) in the response does not match the requestId (%d) in the request", replyHeader.getResponseTo(),
                        responseSettings.getResponseTo())));
                }

                if (replyHeader.getMessageLength() == REPLY_HEADER_LENGTH) {
                    callback.onResult(new ResponseBuffers(replyHeader, null, System.nanoTime() - start), null);
                } else {
                    if (replyHeader.getMessageLength() > responseSettings.getMaxMessageSize()) {
                        callback.onResult(null, new MongoInternalException(String.format(
                            "Unexpectedly large message length of %d exceeds maximum of %d", replyHeader.getMessageLength(),
                            responseSettings.getMaxMessageSize())));
                    }

                    fillAndFlipBuffer(bufferProvider.get(replyHeader.getMessageLength() - REPLY_HEADER_LENGTH), new ResponseBodyCallback(
                        replyHeader));
                }
            }
        }

        private class ResponseBodyCallback implements SingleResultCallback<ByteBuf> {
            private final ReplyHeader replyHeader;

            public ResponseBodyCallback(final ReplyHeader replyHeader) {
                this.replyHeader = replyHeader;
            }

            @Override
            public void onResult(final ByteBuf result, final MongoException e) {
                if (e != null) {
                    callback.onResult(null, e);
                } else {
                    callback.onResult(new ResponseBuffers(replyHeader, result, System.nanoTime() - start), null);
                }
            }
        }
    }
}
