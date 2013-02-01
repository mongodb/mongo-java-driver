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

import org.bson.io.ByteBufferInput;
import org.bson.io.InputBuffer;
import org.bson.io.async.AsyncCompletionHandler;
import org.bson.io.async.AsyncWritableByteChannel;
import org.bson.types.Document;
import org.bson.util.BufferPool;
import org.mongodb.MongoCursorNotFoundException;
import org.mongodb.MongoException;
import org.mongodb.MongoInternalException;
import org.mongodb.MongoInterruptedException;
import org.mongodb.MongoQueryFailureException;
import org.mongodb.ServerAddress;
import org.mongodb.async.SingleResultCallback;
import org.mongodb.io.MongoSocketOpenException;
import org.mongodb.io.MongoSocketReadException;
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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mongodb.protocol.MongoReplyHeader.REPLY_HEADER_LENGTH;

public class MongoAsynchronousChannel {
    private final ServerAddress address;
    private final BufferPool<ByteBuffer> pool;
    private final Serializer<Document> errorSerializer;
    private volatile AsynchronousSocketChannel asynchronousSocketChannel;

    public MongoAsynchronousChannel(final ServerAddress address, final BufferPool<ByteBuffer> pool,
                                    final Serializer<Document> errorSerializer) {
        this.address = address;
        this.pool = pool;
        this.errorSerializer = errorSerializer;
    }

    public ServerAddress getAddress() {
        return address;
    }

    public void sendMessage(final MongoRequestMessage message) {
        ensureOpen();
        sendOneWayMessage(message, new NoOpAsyncCompletionHandler());
    }

    public void sendMessage(final MongoRequestMessage message,
                            final SingleResultCallback<MongoReplyMessage<Document>> callback) {
        sendMessage(message, null, null, callback);
    }

    public void sendMessage(final MongoRequestMessage message, final MongoQueryMessage writeConcernMessage,
                            final Serializer<Document> serializer,
                            final SingleResultCallback<MongoReplyMessage<Document>> callback) {
        ensureOpen();
        sendOneWayMessage(message, new AsyncCompletionHandler() {
            @Override
            public void completed(final int bytesWritten) {
                if (writeConcernMessage != null) {
                    sendQueryMessage(writeConcernMessage, serializer, callback);
                } else {
                    callback.onResult(null, null);
                }
            }

            @Override
            public void failed(final Throwable t) {
                callback.onResult(null, new MongoException("", t));  // TODO
            }
        });
    }


    public <T> MongoReplyMessage<T> sendQueryMessage(final MongoQueryMessage message, final Serializer<T> serializer) {
        ensureOpen();
        long start = System.nanoTime();
        try {
            sendOneWayMessage(message).get();
            return receiveMessage(message, serializer, start);
        } catch (InterruptedException e) {
            throw new MongoInterruptedException("Interrupted while waiting for write to complete", e);
        } catch (ExecutionException e) {
            throw new MongoException("", e.getCause());  // TODO
        }
    }

    public <T> MongoReplyMessage<T> sendGetMoreMessage(final MongoGetMoreMessage message, final Serializer<T> serializer) {
        ensureOpen();
        long start = System.nanoTime();
        try {
            sendOneWayMessage(message).get();
            return receiveMessage(message, serializer, start);
        } catch (InterruptedException e) {
            throw new MongoInterruptedException("Interrupted while waiting for write to complete", e);
        } catch (ExecutionException e) {
            throw new MongoException("", e.getCause());  // TODO
        }
    }

    public void sendQueryMessage(final MongoQueryMessage message, final Serializer<Document> serializer,
                                 final SingleResultCallback<MongoReplyMessage<Document>> callback) {
        sendOneWayMessage(message, new ReceiveMessageCompletionHandler<Document>(message, serializer, System.nanoTime(), callback));
    }

    private <T> void receiveMessage(final MongoRequestMessage message, final Serializer<T> serializer, final long start,
                                    final SingleResultCallback<MongoReplyMessage<T>> callback) {
        fillAndFlipBuffer(pool.get(REPLY_HEADER_LENGTH), new SingleResultCallback<ByteBuffer>() {
            @Override
            public void onResult(final ByteBuffer result, final MongoException e) {
                if (e != null) {
                    callback.onResult(null, e);
                }

                final InputBuffer headerInputBuffer = new ByteBufferInput(result);

                final MongoReplyHeader replyHeader = new MongoReplyHeader(headerInputBuffer);

                pool.done(result);

                if (replyHeader.getNumberReturned() > 0) {
                    fillAndFlipBuffer(pool.get(replyHeader.getMessageLength() - REPLY_HEADER_LENGTH),
                            new SingleResultCallback<ByteBuffer>() {
                                @Override
                                public void onResult(final ByteBuffer result, final MongoException e) {
                                    if (e != null) {
                                        callback.onResult(null, e);
                                    }
                                    InputBuffer bodyInputBuffer = new ByteBufferInput(result);
                                    if (replyHeader.isCursorNotFound()) {
                                        callback.onResult(null, new MongoCursorNotFoundException(
                                                new ServerCursor(((MongoGetMoreMessage) message).getCursorId(), address)));
                                    } else if (replyHeader.isQueryFailure()) {
                                        final Document errorDocument = new MongoReplyMessage<Document>(replyHeader, bodyInputBuffer,
                                                errorSerializer, System.nanoTime() - start).getDocuments().get(0);
                                        callback.onResult(null, new MongoQueryFailureException(address, errorDocument));
                                    } else {
                                        MongoReplyMessage<T> replyMessage = null;
                                        try {
                                            replyMessage = new MongoReplyMessage<T>(replyHeader, bodyInputBuffer, serializer,
                                                    System.nanoTime() - start);
                                        } catch (Throwable t) {
                                            callback.onResult(null, new MongoException("", t)); // TODO: proper subclass
                                        } finally {
                                            pool.done(result);
                                        }
                                        if (replyMessage != null) {
                                            callback.onResult(replyMessage, null);
                                        }
                                    }
                                }
                            });
                }
            }
        });
    }

    private Future<Void> sendOneWayMessage(final MongoRequestMessage message) {
        final WriteFuture retVal = new WriteFuture();
        message.pipeAndClose(new Adapter(), new AsyncCompletionHandler() {
            @Override
            public void completed(final int bytesWritten) {
                retVal.completed();
            }

            @Override
            public void failed(final Throwable t) {
                retVal.failure(t);
            }
        });
        return retVal;
    }

    private void sendOneWayMessage(final MongoRequestMessage message, final AsyncCompletionHandler handler) {
        message.pipeAndClose(new Adapter(), handler);
    }

    private <T> MongoReplyMessage<T> receiveMessage(final MongoRequestMessage message, final Serializer<T> serializer, final long start) {
        ByteBuffer headerByteBuffer = null;
        ByteBuffer bodyByteBuffer = null;
        try {
            headerByteBuffer = pool.get(REPLY_HEADER_LENGTH);
            fillAndFlipBuffer(headerByteBuffer);
            final InputBuffer headerInputBuffer = new ByteBufferInput(headerByteBuffer);

            final MongoReplyHeader replyHeader = new MongoReplyHeader(headerInputBuffer);

            InputBuffer bodyInputBuffer = null;

            if (replyHeader.getNumberReturned() > 0) {
                bodyByteBuffer = pool.get(replyHeader.getMessageLength() - REPLY_HEADER_LENGTH);
                fillAndFlipBuffer(bodyByteBuffer);

                bodyInputBuffer = new ByteBufferInput(bodyByteBuffer);
            }

            if (replyHeader.isCursorNotFound()) {
                throw new MongoCursorNotFoundException(new ServerCursor(((MongoGetMoreMessage) message).getCursorId(),
                        address));
            } else if (replyHeader.isQueryFailure()) {
                final Document errorDocument = new MongoReplyMessage<Document>(replyHeader, bodyInputBuffer,
                        errorSerializer, System.nanoTime() - start).getDocuments().get(0);
                throw new MongoQueryFailureException(address, errorDocument);
            }
            return new MongoReplyMessage<T>(replyHeader, bodyInputBuffer, serializer, System.nanoTime() - start);
        } catch (InterruptedException e) {
            throw new MongoInterruptedException("Interrupted opening socket " + address, e);
        } catch (ExecutionException e) {
            throw new MongoInternalException("Exception from calling Future.get while opening socket to " + address, e.getCause());
        } finally {
            if (headerByteBuffer != null) {
                pool.done(headerByteBuffer);
            }
            if (bodyByteBuffer != null) {
                pool.done(bodyByteBuffer);
            }
        }
    }

    private void fillAndFlipBuffer(final ByteBuffer buffer) throws ExecutionException, InterruptedException {
        int totalBytesRead = 0;
        while (totalBytesRead < buffer.limit()) {
            final int bytesRead = asynchronousSocketChannel.read(buffer).get();
            if (bytesRead == -1) {
                throw new MongoSocketReadException("Prematurely reached end of stream", address);
            }
            totalBytesRead += bytesRead;
        }
        buffer.flip();
    }

    private void fillAndFlipBuffer(final ByteBuffer buffer, final SingleResultCallback<ByteBuffer> callback) {
        asynchronousSocketChannel.read(buffer, null, new BasicCompletionHandler(buffer, callback));
    }

    private static class WriteFuture implements Future<Void> {
        private volatile boolean done;
        private volatile Throwable exception;

        @Override
        public boolean cancel(final boolean mayInterruptIfRunning) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isCancelled() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isDone() {
            return done;
        }

        @Override
        public synchronized Void get() throws InterruptedException, ExecutionException {
            while (!done) {
                wait();
            }
            if (exception != null) {
                throw new ExecutionException(exception);
            }

            return null;
        }

        @Override
        public Void get(final long timeout, final TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            throw new UnsupportedOperationException();
        }

        synchronized void completed() {
            done = true;
            notify();
        }

        void failure(final Throwable t) {
            this.exception = t;
            completed();
        }
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
            } else {
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

    private class Adapter implements AsyncWritableByteChannel {

        @Override
        public void write(final ByteBuffer src, final org.bson.io.async.AsyncCompletionHandler handler) {
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

        @Override
        public Future<Integer> write(final ByteBuffer src) {
            return asynchronousSocketChannel.write(src);
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

    private class ReceiveMessageCompletionHandler<T> implements AsyncCompletionHandler {
        private final MongoQueryMessage message;
        private final Serializer<T> serializer;
        private final long start;
        private final SingleResultCallback<MongoReplyMessage<T>> callback;

        public ReceiveMessageCompletionHandler(final MongoQueryMessage message, final Serializer<T> serializer,
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
}
