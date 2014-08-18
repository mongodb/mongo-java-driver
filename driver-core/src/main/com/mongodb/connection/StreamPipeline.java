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

import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoSocketClosedException;
import com.mongodb.MongoSocketReadException;
import com.mongodb.MongoSocketReadTimeoutException;
import com.mongodb.MongoSocketWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.diagnostics.Loggers;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.event.ConnectionEvent;
import com.mongodb.event.ConnectionListener;
import com.mongodb.event.ConnectionMessageReceivedEvent;
import com.mongodb.event.ConnectionMessagesSentEvent;
import org.bson.ByteBuf;
import org.bson.io.BasicInputBuffer;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedByInterruptException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.ReplyHeader.REPLY_HEADER_LENGTH;

class StreamPipeline {

    private final String clusterId;
    private final Stream stream;
    private final ConnectionListener connectionListener;
    private final InternalConnection internalConnection;
    private volatile boolean initialized;

    private final LinkedList<SendMessageAsync> writeQueue = new LinkedList<SendMessageAsync>();
    private final ConcurrentHashMap<Integer, SingleResultCallback<ResponseBuffers>> readQueue =
        new ConcurrentHashMap<Integer, SingleResultCallback<ResponseBuffers>>();
    private final ConcurrentMap<Integer, Response> messages = new ConcurrentHashMap<Integer, Response>();

    private final Semaphore writing = new Semaphore(1);
    private final Semaphore reading = new Semaphore(1);

    static final Logger LOGGER = Loggers.getLogger("StreamPipeline");

    StreamPipeline(final String clusterId, final Stream stream, final ConnectionListener connectionListener,
                   final InternalConnection internalConnection) {
        this(clusterId, stream, connectionListener, internalConnection, false);
    }

    StreamPipeline(final String clusterId, final Stream stream, final ConnectionListener connectionListener,
                   final InternalConnection internalConnection, final boolean initialized) {
        this.clusterId = notNull("clusterId", clusterId);
        this.stream = notNull("stream", stream);
        this.connectionListener = notNull("connectionListener", connectionListener);
        this.internalConnection = notNull("internalConnection", internalConnection);
        this.initialized = initialized;
    }

    public boolean isInitialized() {
        return initialized;
    }

    public void initialized(final boolean initializedSucceed) {
        initialized = true;
        if (initializedSucceed) {
            try {
                connectionListener.connectionOpened(new ConnectionEvent(clusterId, stream.getAddress(), internalConnection.getId()));
            } catch (Throwable t) {
                LOGGER.warn("Exception when trying to signal connectionOpened to the connectionListener", t);
            }
        } else {
            close();
        }
        processPendingWrites();
    }
    
    void close() {
        internalConnection.close();
    }

    boolean isClosed() {
        return internalConnection.isClosed();
    }

    ServerAddress getServerAddress() {
        return stream.getAddress();
    }

    void sendMessage(final List<ByteBuf> byteBuffers, final int lastRequestId) {
        if (isClosed()) {
            throw new MongoSocketClosedException("Cannot write to a closed stream", getServerAddress());
        } else if (!isInitialized()) {
            throw new MongoException("Connection not initialized");
        } else {
            try {
                writing.acquire();
                stream.write(byteBuffers);
                try {
                    connectionListener.messagesSent(new ConnectionMessagesSentEvent(clusterId,
                                                                                    stream.getAddress(),
                                                                                    internalConnection.getId(),
                                                                                    lastRequestId,
                                                                                    getTotalRemaining(byteBuffers)));
                } catch (Throwable t) {
                    LOGGER.warn("Exception when trying to signal messagesSent to the connectionListener", t);
                }
            } catch (Exception e) {
                close();
                throw translateWriteException(e);
            } finally {
                writing.release();
            }
        }
    }

    ResponseBuffers receiveMessage(final int responseTo) {
        if (isClosed()) {
            throw new MongoSocketClosedException("Cannot read from a closed stream", getServerAddress());
        } else {
            while (!messages.containsKey(responseTo)) {
                try {
                    reading.acquire();
                    if (messages.containsKey(responseTo)) {
                        break;
                    }
                    ResponseBuffers responseBuffers = receiveResponseBuffers();
                    try {
                        connectionListener.messageReceived(new ConnectionMessageReceivedEvent(clusterId,
                                                                                              stream.getAddress(),
                                                                                              internalConnection.getId(),
                                                                                              responseBuffers.getReplyHeader()
                                                                                                             .getResponseTo(),
                                                                                              responseBuffers.getReplyHeader()
                                                                                                             .getMessageLength()));
                    } catch (Throwable t) {
                        LOGGER.warn("Exception when trying to signal messageReceived to the connectionListener", t);
                    }
                    messages.put(responseBuffers.getReplyHeader().getResponseTo(), new Response(responseBuffers, null));
                } catch (Exception e) {
                    close();
                    messages.put(responseTo, new Response(null, translateReadException(e)));
                } finally {
                    reading.release();
                }
            }
            final Response response = messages.remove(responseTo);
            if (response.hasError()) {
                throw response.getError();
            }
            return response.getResult();
        }
    }

    void sendMessageAsync(final List<ByteBuf> byteBuffers, final int lastRequestId, final SingleResultCallback<Void> callback) {
        writeQueue.add(new SendMessageAsync(byteBuffers, lastRequestId, callback));
        processPendingWrites();
    }

    void receiveMessageAsync(final int responseTo, final SingleResultCallback<ResponseBuffers> callback) {
        readQueue.put(responseTo, callback);
        processPendingReads();
    }

    private void fillAndFlipBuffer(final int numBytes, final SingleResultCallback<ByteBuf> callback) {
        if (isClosed()) {
            callback.onResult(null, new MongoSocketClosedException("Cannot read from a closed stream", getServerAddress()));
        } else {
            stream.readAsync(numBytes, new AsyncCompletionHandler<ByteBuf>() {
                @Override
                public void completed(final ByteBuf buffer) {
                    callback.onResult(buffer, null);
                }

                @Override
                public void failed(final Throwable t) {
                    close();
                    callback.onResult(null, translateReadException(t));
                }
            });
        }
    }

    private MongoException translateWriteException(final Throwable e) {
        if (e instanceof MongoException) {
            return (MongoException) e;
        } else if (e instanceof IOException) {
            return new MongoSocketWriteException("Exception sending message", getServerAddress(), e);
        } else if (e instanceof InterruptedException) {
            return new MongoInternalException("Thread interrupted exception", e);
        } else {
            return new MongoInternalException("Unexpected exception", e);
        }
    }

    private MongoException translateReadException(final Throwable e) {
        if (e instanceof SocketTimeoutException) {
            return new MongoSocketReadTimeoutException("Timeout while receiving message", getServerAddress(), e);
        } else if (e instanceof InterruptedIOException) {
            return new MongoInterruptedException("Interrupted while receiving message", (InterruptedIOException) e);
        } else if (e instanceof ClosedByInterruptException) {
            return new MongoInterruptedException("Interrupted while receiving message", (ClosedByInterruptException) e);
        } else if (e instanceof IOException) {
            return new MongoSocketReadException("Exception receiving message", getServerAddress(), e);
        } else if (e instanceof MongoException) {
            return (MongoException) e;
        } else if (e instanceof RuntimeException) {
            return new MongoInternalException("Unexpected runtime exception", e);
        } else if (e instanceof InterruptedException) {
            return new MongoInternalException("Interrupted exception", e);
        } else {
            return new MongoInternalException("Unexpected exception", e);
        }
    }

    private ResponseBuffers receiveResponseBuffers() throws IOException {
        ByteBuf headerByteBuffer = stream.read(REPLY_HEADER_LENGTH);
        ReplyHeader replyHeader;
        BasicInputBuffer headerInputBuffer = new BasicInputBuffer(headerByteBuffer);
        try {
            replyHeader = new ReplyHeader(headerInputBuffer);
        } finally {
            headerInputBuffer.close();
        }

        ByteBuf bodyByteBuffer = null;

        if (replyHeader.getNumberReturned() > 0) {
            bodyByteBuffer = stream.read(replyHeader.getMessageLength() - REPLY_HEADER_LENGTH);
        }
        return new ResponseBuffers(replyHeader, bodyByteBuffer);
    }

    private class ResponseHeaderCallback implements SingleResultCallback<ByteBuf> {
        private final SingleResultCallback<ResponseBuffers> callback;

        public ResponseHeaderCallback(final SingleResultCallback<ResponseBuffers> callback) {
            this.callback = callback;
        }

        @Override
        public void onResult(final ByteBuf result, final MongoException e) {
            if (e != null) {
                callback.onResult(null, e);
            } else {
                ReplyHeader replyHeader;
                BasicInputBuffer headerInputBuffer = new BasicInputBuffer(result);
                try {
                    replyHeader = new ReplyHeader(headerInputBuffer);
                } finally {
                    headerInputBuffer.close();
                }

                if (replyHeader.getMessageLength() == REPLY_HEADER_LENGTH) {
                    onSuccess(new ResponseBuffers(replyHeader, null));
                } else {
                    fillAndFlipBuffer(replyHeader.getMessageLength() - REPLY_HEADER_LENGTH,
                                      new ResponseBodyCallback(replyHeader));
                }
            }
        }

        private void onSuccess(final ResponseBuffers responseBuffers) {
            try {
                connectionListener.messageReceived(new ConnectionMessageReceivedEvent(clusterId,
                                                                                      stream.getAddress(),
                                                                                      internalConnection.getId(),
                                                                                      responseBuffers.getReplyHeader().getResponseTo(),
                                                                                      responseBuffers.getReplyHeader().getMessageLength()));
            } catch (Throwable t) {
                LOGGER.warn("Exception when trying to signal messageReceived to the connectionListener", t);
            }

            try {
                callback.onResult(responseBuffers, null);
            } catch (Throwable t) {
                LOGGER.warn("Exception calling callback", t);
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
                    try {
                        callback.onResult(new ResponseBuffers(replyHeader, result), e);
                    } catch (Throwable t) {
                        LOGGER.warn("Exception calling callback", t);
                    }
                } else {
                    onSuccess(new ResponseBuffers(replyHeader, result));
                }
            }
        }
    }

    private int getTotalRemaining(final List<ByteBuf> byteBuffers) {
        int messageSize = 0;
        for (final ByteBuf cur : byteBuffers) {
            messageSize += cur.remaining();
        }
        return messageSize;
    }

    private void processPendingReads() {
        if (reading.tryAcquire()) {
            processPendingResults();

            if (readQueue.isEmpty()) {
                reading.release();
                return;
            }

            if (isClosed()) {
                Iterator<Map.Entry<Integer, SingleResultCallback<ResponseBuffers>>> it = readQueue.entrySet().iterator();
                try {
                    while (it.hasNext()) {
                        Map.Entry<Integer, SingleResultCallback<ResponseBuffers>> pairs = it.next();
                        final SingleResultCallback<ResponseBuffers> callback = pairs.getValue();
                        it.remove();
                        try {
                            callback.onResult(null, new MongoSocketClosedException("Cannot read from a closed stream", getServerAddress()));
                        } catch (Throwable t) {
                            LOGGER.warn("Exception calling callback", t);
                        }
                    }
                } finally {
                    reading.release();
                }
            } else {
                fillAndFlipBuffer(REPLY_HEADER_LENGTH,
                                  new ResponseHeaderCallback(new SingleResultCallback<ResponseBuffers>() {
                                      @Override
                                      public void onResult(final ResponseBuffers result, final MongoException e) {
                                          if (result == null) {
                                              reading.release();
                                              processUnknownFailedRead(e);
                                          } else {
                                              reading.release();
                                              messages.put(result.getReplyHeader().getResponseTo(), new Response(result, e));
                                          }
                                          processPendingReads();
                                      }
                                  }));
            }
        }
    }

    private void processPendingWrites() {
        if (isInitialized() && writing.tryAcquire()) {
            if (writeQueue.isEmpty()) {
                writing.release();
                return;
            }
            if (isClosed()) {
                try {
                    while (!writeQueue.isEmpty()) {
                        final SendMessageAsync message = writeQueue.poll();
                        try {
                            message.callback.onResult(null, new MongoSocketClosedException("Cannot write to a closed stream",
                                                                                           getServerAddress()));
                        } catch (Throwable t) {
                            LOGGER.warn("Exception calling callback", t);
                        }
                    }
                } finally {
                    writing.release();
                }

            } else {
                final SendMessageAsync message = writeQueue.poll();
                stream.writeAsync(message.getByteBuffers(), new AsyncCompletionHandler<Void>() {
                    @Override
                    public void completed(final Void v) {
                        writing.release();
                        try {
                            connectionListener.messagesSent(new ConnectionMessagesSentEvent(clusterId, stream.getAddress(),
                                                                                            internalConnection.getId(),
                                                                                            message.getMessageId(),
                                                                                            getTotalRemaining(message.getByteBuffers())));
                        } catch (Throwable t) {
                            LOGGER.warn("Exception when trying to signal messagesSent to the connectionListener", t);
                        }
                        try {
                            message.getCallback().onResult(null, null);
                        } catch (Throwable t) {
                            LOGGER.warn("Exception calling callback", t);
                        }
                        processPendingWrites();
                    }

                    @Override
                    public void failed(final Throwable t) {
                        writing.release();
                        close();
                        try {
                            message.getCallback().onResult(null, translateWriteException(t));
                        } catch (Throwable e) {
                            LOGGER.warn("Exception calling callback", e);
                        }
                        processPendingWrites();
                    }
                });
            }
        }
    }

    private void processPendingResults() {
        Iterator<Map.Entry<Integer, Response>> it = messages.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, Response> pairs = it.next();
            final int messageId = pairs.getKey();
            final SingleResultCallback<ResponseBuffers> callback = readQueue.remove(messageId);
            if (callback != null) {
                if (pairs.getValue().getError() != null) {
                    try {
                        callback.onResult(null, pairs.getValue().getError());
                    } catch (Throwable t) {
                        LOGGER.warn("Exception calling callback", t);
                    }
                } else {
                    try {
                        callback.onResult(pairs.getValue().getResult(), null);
                    } catch (Throwable t) {
                        LOGGER.warn("Exception calling callback", t);
                    }
                }
                it.remove();
            }
        }
    }

    private void processUnknownFailedRead(final MongoException e) {
        processPendingResults();
        close();
        Iterator<Map.Entry<Integer, SingleResultCallback<ResponseBuffers>>> it = readQueue.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, SingleResultCallback<ResponseBuffers>> pairs = it.next();
            final SingleResultCallback<ResponseBuffers> callback = pairs.getValue();
            it.remove();
            try {
                callback.onResult(null, e);
            } catch (Throwable t) {
                LOGGER.warn("Exception calling callback", t);
            }
        }
    }

    private static class SendMessage {
        private final List<ByteBuf> byteBuffers;
        private final int messageId;

        SendMessage(final List<ByteBuf> byteBuffers, final int messageId) {
            this.byteBuffers = byteBuffers;
            this.messageId = messageId;
        }

        public List<ByteBuf> getByteBuffers() {
            return byteBuffers;
        }

        public int getMessageId() {
            return messageId;
        }
    }

    private static class SendMessageAsync extends SendMessage {
        private final SingleResultCallback<Void> callback;

        SendMessageAsync(final List<ByteBuf> byteBuffers, final int messageId, final SingleResultCallback<Void> callback) {
            super(byteBuffers, messageId);
            this.callback = callback;
        }

        public SingleResultCallback<Void> getCallback() {
            return callback;
        }
    }


    private static class Response {
        private final ResponseBuffers result;
        private final MongoException error;

        public Response(final ResponseBuffers result, final MongoException error) {
            this.result = result;
            this.error = error;
        }

        public ResponseBuffers getResult() {
            return result;
        }

        public MongoException getError() {
            return error;
        }

        public boolean hasError() {
            return error != null;
        }
    }
}
