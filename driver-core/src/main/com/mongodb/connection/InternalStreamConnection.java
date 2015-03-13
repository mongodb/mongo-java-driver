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
import com.mongodb.async.SingleResultCallback;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.event.ConnectionEvent;
import com.mongodb.event.ConnectionListener;
import com.mongodb.event.ConnectionMessageReceivedEvent;
import com.mongodb.event.ConnectionMessagesSentEvent;
import org.bson.ByteBuf;
import org.bson.io.ByteBufferBsonInput;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedByInterruptException;
import java.util.List;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.ReplyHeader.REPLY_HEADER_LENGTH;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static java.lang.String.format;

class InternalStreamConnection implements InternalConnection {
    private final ServerId serverId;
    private final StreamFactory streamFactory;
    private final InternalConnectionInitializer connectionInitializer;
    private final ConnectionListener connectionListener;

    private volatile ConnectionDescription description;
    private volatile Stream stream;
    private volatile boolean isClosed;
    private volatile boolean opened;

    static final Logger LOGGER = Loggers.getLogger("connection");

    InternalStreamConnection(final ServerId serverId, final StreamFactory streamFactory,
                             final InternalConnectionInitializer connectionInitializer,
                             final ConnectionListener connectionListener) {
        this.serverId = notNull("serverId", serverId);
        this.streamFactory = notNull("streamFactory", streamFactory);
        this.connectionInitializer = notNull("connectionInitializer", connectionInitializer);
        this.connectionListener = notNull("connectionListener", connectionListener);
        description = new ConnectionDescription(serverId);
    }

    @Override
    public ConnectionDescription getDescription() {
        return description;
    }

    @Override
    public void open() {
        isTrue("Open already called", stream == null);
        stream = streamFactory.create(serverId.getAddress());
        try {
            stream.open();
            description = connectionInitializer.initialize(this);
            LOGGER.info(format("Opened connection [%s] to %s", getId(), serverId.getAddress()));
            try {
                connectionListener.connectionOpened(new ConnectionEvent(getId()));
            } catch (Throwable t) {
                LOGGER.warn("Exception when trying to signal connectionOpened to the connectionListener", t);
            }
            opened = true;
        } catch (Throwable t) {
            close();
            if (t instanceof MongoException) {
                throw (MongoException) t;
            } else {
                throw new MongoException(t.toString(), t);
            }
        }
    }

    @Override
    public void openAsync(final SingleResultCallback<Void> callback) {
        isTrue("Open already called", stream == null);
        stream = streamFactory.create(serverId.getAddress());
        stream.openAsync(new AsyncCompletionHandler<Void>() {
            @Override
            public void completed(final Void aVoid) {
                connectionInitializer.initializeAsync(InternalStreamConnection.this, new SingleResultCallback<ConnectionDescription>() {
                    @Override
                    public void onResult(final ConnectionDescription result, final Throwable t) {
                        if (t != null) {
                            close();
                            callback.onResult(null, t);
                        } else {
                            description = result;
                            callback.onResult(null, null);
                            if (LOGGER.isInfoEnabled()) {
                                LOGGER.info(format("Opened connection [%s] to %s", getId(), serverId.getAddress()));
                            }
                            try {
                                connectionListener.connectionOpened(new ConnectionEvent(getId()));
                            } catch (Throwable tr) {
                                LOGGER.warn("Exception when trying to signal connectionOpened to the connectionListener", tr);
                            }
                            opened = true;
                        }
                    }
                });
            }

            @Override
            public void failed(final Throwable t) {
                callback.onResult(null, t);
            }
        });
    }

    @Override
    public void close() {
        if (stream != null) {
            stream.close();
        }
        isClosed = true;
        try {
            connectionListener.connectionClosed(new ConnectionEvent(getId()));
        } catch (Throwable t) {
            LOGGER.warn("Exception when trying to signal connectionClosed to the connectionListener", t);
        }
    }

    @Override
    public boolean opened() {
        return opened;
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public void sendMessage(final List<ByteBuf> byteBuffers, final int lastRequestId) {
        notNull("open", stream);
        if (isClosed()) {
            throw new MongoSocketClosedException("Cannot write to a closed stream", getServerAddress());
        } else {
            try {
                stream.write(byteBuffers);
                try {
                    connectionListener.messagesSent(new ConnectionMessagesSentEvent(getId(),
                            lastRequestId,
                            getTotalRemaining(byteBuffers)));
                } catch (Throwable t) {
                    LOGGER.warn("Exception when trying to signal messagesSent to the connectionListener", t);
                }
            } catch (Exception e) {
                close();
                throw translateWriteException(e);
            }
        }
    }

    @Override
    public ResponseBuffers receiveMessage(final int responseTo) {
        notNull("open", stream);
        if (isClosed()) {
            throw new MongoSocketClosedException("Cannot read from a closed stream", getServerAddress());
        } else {
            try {
                ResponseBuffers responseBuffers = receiveResponseBuffers();
                try {
                    connectionListener.messageReceived(new ConnectionMessageReceivedEvent(getId(),
                                                       responseBuffers.getReplyHeader().getResponseTo(),
                                                       responseBuffers.getReplyHeader().getMessageLength()));
                } catch (Throwable t) {
                    LOGGER.warn("Exception when trying to signal messageReceived to the connectionListener", t);
                }
                return responseBuffers;
            } catch (Exception e) {
                close();
                throw translateReadException(e);
            }
        }
    }

    @Override
    public void sendMessageAsync(final List<ByteBuf> byteBuffers, final int lastRequestId, final SingleResultCallback<Void> callback) {
        notNull("open", stream);
        notNull("callback", callback);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Send message async: %s", lastRequestId));
        }
        final SingleResultCallback<Void> safeCallback = errorHandlingCallback(callback, LOGGER);
        if (isClosed()) {
            try {
                safeCallback.onResult(null, new MongoSocketClosedException("Cannot write to a closed stream", getServerAddress()));
            } catch (Throwable t) {
                LOGGER.warn("Exception calling callback", t);
            }
        } else {
            stream.writeAsync(byteBuffers, new AsyncCompletionHandler<Void>() {
                @Override
                public void completed(final Void v) {
                    try {
                        connectionListener.messagesSent(new ConnectionMessagesSentEvent(getId(), lastRequestId,
                                getTotalRemaining(byteBuffers)));
                    } catch (Throwable t) {
                        LOGGER.warn("Exception when trying to signal messagesSent to the connectionListener", t);
                    }
                    safeCallback.onResult(null, null);
                }

                @Override
                public void failed(final Throwable t) {
                    close();
                    safeCallback.onResult(null, translateWriteException(t));
                }
            });
        }
    }

    @Override
    public void receiveMessageAsync(final int responseTo, final SingleResultCallback<ResponseBuffers> callback) {
        notNull("open", stream);
        notNull("callback", callback);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Receive message async: %s", responseTo));
        }
        final SingleResultCallback<ResponseBuffers> safeCallback = errorHandlingCallback(callback, LOGGER);
        if (isClosed()) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(format("Stream closed: %s", responseTo));
            }
            safeCallback.onResult(null, new MongoSocketClosedException("Cannot read from a closed stream", getServerAddress()));
        } else {
            fillAndFlipBuffer(REPLY_HEADER_LENGTH,
                    errorHandlingCallback(new ResponseHeaderCallback(new SingleResultCallback<ResponseBuffers>() {
                        @Override
                        public void onResult(final ResponseBuffers result, final Throwable t) {
                            if (LOGGER.isTraceEnabled()) {
                                LOGGER.trace(format("Received message: %s", responseTo));
                            }
                            safeCallback.onResult(result, t);
                        }
                    }), LOGGER));
        }
    }

    private ConnectionId getId() {
        return description.getConnectionId();
    }

    private ServerAddress getServerAddress() {
        return description.getServerAddress();
    }

    private void fillAndFlipBuffer(final int numBytes, final SingleResultCallback<ByteBuf> callback) {
        notNull("open", stream);
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
        if (e instanceof MongoException) {
            return (MongoException) e;
        } else if (e instanceof SocketTimeoutException) {
            return new MongoSocketReadTimeoutException("Timeout while receiving message", getServerAddress(), e);
        } else if (e instanceof InterruptedIOException) {
            return new MongoInterruptedException("Interrupted while receiving message", (InterruptedIOException) e);
        } else if (e instanceof ClosedByInterruptException) {
            return new MongoInterruptedException("Interrupted while receiving message", (ClosedByInterruptException) e);
        } else if (e instanceof IOException) {
            return new MongoSocketReadException("Exception receiving message", getServerAddress(), e);
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
        ByteBufferBsonInput headerInputBuffer = new ByteBufferBsonInput(headerByteBuffer);
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

    @Override
    public ByteBuf getBuffer(final int size) {
        notNull("open", stream);
        return stream.getBuffer(size);
    }

    private class ResponseHeaderCallback implements SingleResultCallback<ByteBuf> {
        private final SingleResultCallback<ResponseBuffers> callback;

        public ResponseHeaderCallback(final SingleResultCallback<ResponseBuffers> callback) {
            this.callback = callback;
        }

        @Override
        public void onResult(final ByteBuf result, final Throwable t) {
            if (t != null) {
                callback.onResult(null, t);
            } else {
                ReplyHeader replyHeader;
                ByteBufferBsonInput headerInputBuffer = new ByteBufferBsonInput(result);
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

            if (responseBuffers == null) {
                callback.onResult(null, new MongoException("Unexpected empty response buffers"));
                return;
            }

            try {
                connectionListener.messageReceived(new ConnectionMessageReceivedEvent(getId(),
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
            public void onResult(final ByteBuf result, final Throwable t) {
                if (t != null) {
                    try {
                        callback.onResult(new ResponseBuffers(replyHeader, result), t);
                    } catch (Throwable tr) {
                        LOGGER.warn("Exception calling callback", tr);
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
}
