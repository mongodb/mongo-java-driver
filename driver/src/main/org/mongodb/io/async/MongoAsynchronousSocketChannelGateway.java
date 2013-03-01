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
import org.mongodb.Document;
import org.mongodb.io.BufferPool;
import org.mongodb.MongoCursorNotFoundException;
import org.mongodb.MongoException;
import org.mongodb.MongoInternalException;
import org.mongodb.MongoInterruptedException;
import org.mongodb.MongoQueryFailureException;
import org.mongodb.ServerAddress;
import org.mongodb.async.SingleResultCallback;
import org.mongodb.io.MongoSocketOpenException;
import org.mongodb.protocol.MongoGetMoreMessage;
import org.mongodb.protocol.MongoQueryMessage;
import org.mongodb.protocol.MongoReplyHeader;
import org.mongodb.protocol.MongoReplyMessage;
import org.mongodb.protocol.MongoRequestMessage;
import org.mongodb.result.ServerCursor;
import org.mongodb.serialization.Serializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.ExecutionException;

import static org.mongodb.protocol.MongoReplyHeader.REPLY_HEADER_LENGTH;

/**
 * An asynchronous gateway for the MongoDB protocol.
 * <p>
 * Note: This class is not part of the public API.  It may break binary compatibility even in minor releases.
 */
public class MongoAsynchronousSocketChannelGateway {
    private final ServerAddress address;
    private final BufferPool<ByteBuffer> pool;
    private final Serializer<Document> errorSerializer;
    private volatile AsynchronousSocketChannel asynchronousSocketChannel;

    public MongoAsynchronousSocketChannelGateway(final ServerAddress address, final BufferPool<ByteBuffer> pool,
                                                 final Serializer<Document> errorSerializer) {
        this.address = address;
        this.pool = pool;
        this.errorSerializer = errorSerializer;
    }

    public ServerAddress getAddress() {
        return address;
    }

    public void sendMessage(final MongoRequestMessage message) {
        sendOneWayMessage(message, new NoOpAsyncCompletionHandler());
    }

    public void sendMessage(final MongoRequestMessage message,
                            final SingleResultCallback<MongoReplyMessage<Document>> callback) {
        sendMessage(message, null, null, callback);
    }

    public void sendMessage(final MongoRequestMessage message, final MongoQueryMessage writeConcernMessage,
                            final Serializer<Document> serializer,
                            final SingleResultCallback<MongoReplyMessage<Document>> callback) {
        sendOneWayMessage(message, new AsyncCompletionHandler() {
            @Override
            public void completed(final int bytesWritten) {
                if (writeConcernMessage != null) {
                    sendAndReceiveMessage(writeConcernMessage, serializer, callback);
                }
                else {
                    callback.onResult(null, null);
                }
            }

            @Override
            public void failed(final Throwable t) {
                callback.onResult(null, new MongoException("", t));  // TODO
            }
        });
    }

    public <T> void sendAndReceiveMessage(final MongoRequestMessage message, final Serializer<T> serializer,
                                          final SingleResultCallback<MongoReplyMessage<T>> callback) {
        sendOneWayMessage(message, new ReceiveMessageCompletionHandler<T>(message, serializer, System.nanoTime(), callback));
    }

    private <T> void receiveMessage(final MongoRequestMessage message, final Serializer<T> serializer, final long start,
                                    final SingleResultCallback<MongoReplyMessage<T>> callback) {
        fillAndFlipBuffer(pool.get(REPLY_HEADER_LENGTH), new ResponseHeaderCallback<T>(callback, message, start, serializer));
    }

    private void sendOneWayMessage(final MongoRequestMessage message, final AsyncCompletionHandler handler) {
        message.pipeAndClose(new AsyncWritableByteChannelAdapter(), handler);
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

    private void ensureOpen() {
        try {
            if (asynchronousSocketChannel == null) {
                asynchronousSocketChannel = AsynchronousSocketChannel.open();
                asynchronousSocketChannel.connect(address.getSocketAddress()).get();
            }
        } catch (IOException e) {
            throw new MongoSocketOpenException("Exception opening socket", address, e);
        } catch (InterruptedException e) {
            throw new MongoInterruptedException("Interrupted opening socket " + address, e);
        } catch (ExecutionException e) {
            throw new MongoInternalException("Exception from calling Future.get while opening socket to " + address, e.getCause());
        }
    }

    //CHECKSTYLE:OFF
    public void close() {
        try {
            if (asynchronousSocketChannel != null) {
                asynchronousSocketChannel.close();
                asynchronousSocketChannel = null;
            }
        } catch (IOException e) {
            // ignore
        }
    }
    //CHECKSTYLE:ON

    private class AsyncWritableByteChannelAdapter implements AsyncWritableByteChannel {

        @Override
        public void write(final ByteBuffer src, final AsyncCompletionHandler handler) {
            ensureOpen();
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
    }

    private class ReceiveMessageCompletionHandler<T> implements AsyncCompletionHandler {
        private final MongoRequestMessage message;
        private final Serializer<T> serializer;
        private final long start;
        private final SingleResultCallback<MongoReplyMessage<T>> callback;

        public ReceiveMessageCompletionHandler(final MongoRequestMessage message, final Serializer<T> serializer,
                                               final long start, final SingleResultCallback<MongoReplyMessage<T>> callback) {
            this.message = message;
            this.serializer = serializer;
            this.start = start;
            this.callback = callback;
        }

        @Override
        public void completed(final int bytesWritten) {
            receiveMessage(message, serializer, start, callback);
        }

        @Override
        public void failed(final Throwable t) {
            callback.onResult(null, new MongoException("", t));  // TODO
        }
    }

    private class ResponseHeaderCallback<T> implements SingleResultCallback<ByteBuffer> {
        private final SingleResultCallback<MongoReplyMessage<T>> callback;
        private final MongoRequestMessage message;
        private final long start;
        private final Serializer<T> serializer;

        public ResponseHeaderCallback(final SingleResultCallback<MongoReplyMessage<T>> callback,
                                      final MongoRequestMessage message, final long start,
                                      final Serializer<T> serializer) {
            this.callback = callback;
            this.message = message;
            this.start = start;
            this.serializer = serializer;
        }

        @Override
        public void onResult(final ByteBuffer result, final MongoException e) {
            if (e != null) {
                callback.onResult(null, e);
            }

            final InputBuffer headerInputBuffer = new BasicInputBuffer(result);

            final MongoReplyHeader replyHeader = new MongoReplyHeader(headerInputBuffer);

            pool.done(result);

            if (replyHeader.isCursorNotFound()) {
                callback.onResult(null, new MongoCursorNotFoundException(
                        new ServerCursor(((MongoGetMoreMessage) message).getCursorId(), address)));
            }
            else if (replyHeader.getNumberReturned() == 0) {
                callback.onResult(new MongoReplyMessage<T>(replyHeader, System.nanoTime() - start), null);
            }
            else {
                fillAndFlipBuffer(pool.get(replyHeader.getMessageLength() - REPLY_HEADER_LENGTH),
                        new ResponseBodyCallback(replyHeader));
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
                InputBuffer bodyInputBuffer = new BasicInputBuffer(result);
                if (replyHeader.isQueryFailure()) {
                    final Document errorDocument = new MongoReplyMessage<Document>(replyHeader, bodyInputBuffer,
                            errorSerializer, System.nanoTime() - start).getDocuments().get(0);
                    callback.onResult(null, new MongoQueryFailureException(address, errorDocument));
                }
                else {
                    try {
                        MongoReplyMessage<T> replyMessage =
                                new MongoReplyMessage<T>(replyHeader, bodyInputBuffer, serializer,
                                        System.nanoTime() - start);
                        callback.onResult(replyMessage, null);
                    } catch (Throwable t) {
                        callback.onResult(null, new MongoException("", t)); // TODO: proper subclass
                    } finally {
                        pool.done(result);
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
