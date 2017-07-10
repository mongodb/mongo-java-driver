/*
 * Copyright 2013-2016 MongoDB, Inc.
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
import org.bson.ByteBuf;
import org.bson.io.ByteBufferBsonInput;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedByInterruptException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.ReplyHeader.REPLY_HEADER_LENGTH;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static java.lang.String.format;

// This class is a bit strange currently.  It supports both concurrent synchronous and asynchronous send and receive, but for simplicity is
// designed to only handle concurrent synchronous OR concurrent asynchronous requests at any given time.  This works because
// Server#getConnection returns instances of the synchronous Connection class, which Server#getConnectionAsync returns instances of the
// asynchronous AsyncConnection class, so at any given time a client's view of a connection is either solely synchronous or solely
// asynchronous.
class InternalStreamConnection implements InternalConnection {
    private final ServerId serverId;
    private final StreamFactory streamFactory;
    private final InternalConnectionInitializer connectionInitializer;

    private final Lock writerLock = new ReentrantLock(false);
    private final Lock readerLock = new ReentrantLock(false);

    private final Deque<SendMessageRequest> writeQueue = new ArrayDeque<SendMessageRequest>();
    private final Map<Integer, SingleResultCallback<ResponseBuffers>> readQueue =
    new HashMap<Integer, SingleResultCallback<ResponseBuffers>>();
    private final Map<Integer, ResponseBuffers> messages = new ConcurrentHashMap<Integer, ResponseBuffers>();

    private boolean isWriting;
    private boolean isReading;

    private final AtomicReference<CountDownLatch> readingPhase = new AtomicReference<CountDownLatch>(new CountDownLatch(1));

    private volatile MongoException exceptionThatPrecededStreamClosing;

    private volatile ConnectionDescription description;
    private volatile Stream stream;

    private final AtomicBoolean isClosed = new AtomicBoolean();
    private final AtomicBoolean opened = new AtomicBoolean();

    static final Logger LOGGER = Loggers.getLogger("connection");

    InternalStreamConnection(final ServerId serverId, final StreamFactory streamFactory,
                             final InternalConnectionInitializer connectionInitializer) {
        this.serverId = notNull("serverId", serverId);
        this.streamFactory = notNull("streamFactory", streamFactory);
        this.connectionInitializer = notNull("connectionInitializer", connectionInitializer);
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
            opened.set(true);
            LOGGER.info(format("Opened connection [%s] to %s", getId(), serverId.getAddress()));
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
        isTrue("Open already called", stream == null, callback);
        try {
            stream = streamFactory.create(serverId.getAddress());
        } catch (Throwable t) {
            callback.onResult(null, t);
            return;
        }
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
                            opened.set(true);
                            if (LOGGER.isInfoEnabled()) {
                                LOGGER.info(format("Opened connection [%s] to %s", getId(), serverId.getAddress()));
                            }
                            callback.onResult(null, null);
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
        // All but the first call is a no-op
        if (!isClosed.getAndSet(true)) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(String.format("Closing connection %s", getId()));
            }
            if (stream != null) {
                stream.close();
            }
        }
    }

    @Override
    public boolean opened() {
        return opened.get();
    }

    @Override
    public boolean isClosed() {
        return isClosed.get();
    }

    @Override
    public void sendMessage(final List<ByteBuf> byteBuffers, final int lastRequestId) {
        notNull("stream is open", stream);

        if (isClosed()) {
            throw new MongoSocketClosedException("Cannot write to a closed stream", getServerAddress());
        }

        writerLock.lock();
        try {
            stream.write(byteBuffers);
        } catch (Exception e) {
            close();
            throw translateWriteException(e);
        } finally {
            writerLock.unlock();
        }
    }

    @Override
    public ResponseBuffers receiveMessage(final int responseTo) {
        notNull("stream is open", stream);
        if (isClosed()) {
            throw new MongoSocketClosedException("Cannot read from a closed stream", getServerAddress());
        }

        CountDownLatch localLatch = new CountDownLatch(1);
        readerLock.lock();
        try {
            ResponseBuffers responseBuffers = receiveResponseBuffers();
            messages.put(responseBuffers.getReplyHeader().getResponseTo(), responseBuffers);
            readingPhase.getAndSet(localLatch).countDown();
        } catch (Throwable t) {
            exceptionThatPrecededStreamClosing = translateReadException(t);
            close();
            readingPhase.getAndSet(localLatch).countDown();
        } finally {
            readerLock.unlock();
        }

        while (true) {
            if (isClosed()) {
                if (exceptionThatPrecededStreamClosing != null) {
                    throw exceptionThatPrecededStreamClosing;
                } else {
                    throw new MongoSocketClosedException("Socket has been closed", getServerAddress());
                }
            }
            ResponseBuffers myResponse = messages.remove(responseTo);
            if (myResponse != null) {
                return myResponse;
            }

            try {
                localLatch.await();
            } catch (InterruptedException e) {
                throw new MongoInterruptedException("Interrupted while reading from stream", e);
            }

            localLatch = readingPhase.get();
        }
    }

    @Override
    public void sendMessageAsync(final List<ByteBuf> byteBuffers, final int lastRequestId, final SingleResultCallback<Void> callback) {
        notNull("stream is open", stream, callback);

        if (isClosed()) {
            callback.onResult(null, new MongoSocketClosedException("Can not read from a closed socket", getServerAddress()));
            return;
        }

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Queuing send message: %s. ([%s])", lastRequestId, getId()));
        }

        SendMessageRequest sendMessageRequest = new SendMessageRequest(byteBuffers, lastRequestId, errorHandlingCallback(callback, LOGGER));

        boolean mustWrite = false;
        writerLock.lock();
        try {
            if (isWriting) {
                writeQueue.add(sendMessageRequest);
            } else {
                isWriting = true;
                mustWrite = true;
            }

        } finally {
            writerLock.unlock();
        }

        if (mustWrite) {
            writeAsync(sendMessageRequest);
        }
    }

    private void writeAsync(final SendMessageRequest request) {
        stream.writeAsync(request.getByteBuffers(), new AsyncCompletionHandler<Void>() {
            @Override
            public void completed(final Void v) {
                SendMessageRequest nextMessage = null;
                writerLock.lock();
                try {
                    nextMessage = writeQueue.poll();
                    if (nextMessage == null) {
                        isWriting = false;
                    }
                } finally {
                    writerLock.unlock();
                }
                request.getCallback().onResult(null, null);

                if (nextMessage != null) {
                    writeAsync(nextMessage);
                }
            }

            @Override
            public void failed(final Throwable t) {
                writerLock.lock();
                try {
                    MongoException translatedWriteException = translateWriteException(t);
                    request.getCallback().onResult(null, translatedWriteException);
                    SendMessageRequest nextMessage;
                    while ((nextMessage = writeQueue.poll()) != null) {
                        nextMessage.callback.onResult(null, translatedWriteException);
                    }
                    isWriting = false;
                    close();
                } finally {
                    writerLock.unlock();
                }
            }
        });
    }

    @Override
    public void receiveMessageAsync(final int responseTo, final SingleResultCallback<ResponseBuffers> callback) {
        isTrue("stream is open", stream != null, callback);

        if (isClosed()) {
            callback.onResult(null, new MongoSocketClosedException("Can not read from a closed socket", getServerAddress()));
            return;
        }

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Queuing read message: %s. ([%s])", responseTo, getId()));
        }

        ResponseBuffers response = null;
        readerLock.lock();
        boolean mustRead = false;
        try {
            response = messages.remove(responseTo);

            if (response == null) {
                readQueue.put(responseTo, callback);
            }

            if (!readQueue.isEmpty() && !isReading) {
                isReading = true;
                mustRead = true;
            }
        } finally {
            readerLock.unlock();
        }

        executeCallbackAndReceiveResponse(callback, response, mustRead);
    }

    private void executeCallbackAndReceiveResponse(final SingleResultCallback<ResponseBuffers> callback, final ResponseBuffers result,
                                                   final boolean mustRead) {
        if (callback != null && result != null) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(String.format("Executing callback for %s on %s", result.getReplyHeader().getResponseTo(), getId()));
            }
            callback.onResult(result, null);
        }

        if (mustRead) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(String.format("Start receiving response on %s", getId()));
            }
            receiveResponseAsync();
        }
    }

    private class ResponseBuffersCallback implements SingleResultCallback<ResponseBuffers> {
        @Override
        public void onResult(final ResponseBuffers result, final Throwable t) {
                SingleResultCallback<ResponseBuffers> callback = null;
                boolean mustRead = false;
                readerLock.lock();
                try {
                    if (t != null) {
                        failAllQueuedReads(t);
                        return;
                    }

                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace(String.format("Read response to message %s on %s", result.getReplyHeader().getResponseTo(), getId()));
                    }

                    callback = readQueue.remove(result.getReplyHeader().getResponseTo());

                    if (readQueue.isEmpty()) {
                        isReading = false;
                    } else {
                        mustRead = true;
                    }

                    if (callback == null) {
                        messages.put(result.getReplyHeader().getResponseTo(), result);
                    }
                } finally {
                    readerLock.unlock();
                }

                executeCallbackAndReceiveResponse(callback, result, mustRead);
            }
    }

    private ConnectionId getId() {
        return description.getConnectionId();
    }

    private ServerAddress getServerAddress() {
        return description.getServerAddress();
    }

    private void receiveResponseAsync() {
        readAsync(REPLY_HEADER_LENGTH,
                  errorHandlingCallback(new ResponseHeaderCallback(new ResponseBuffersCallback()), LOGGER));
    }

    private void readAsync(final int numBytes, final SingleResultCallback<ByteBuf> callback) {
        if (isClosed()) {
            callback.onResult(null, new MongoSocketClosedException("Cannot read from a closed stream", getServerAddress()));
            return;
        }

        try {
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
        } catch (Exception e) {
            callback.onResult(null, translateReadException(e));
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
            replyHeader = new ReplyHeader(headerInputBuffer, description.getMaxMessageSize());
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

        ResponseHeaderCallback(final SingleResultCallback<ResponseBuffers> callback) {
            this.callback = callback;
        }

        @Override
        public void onResult(final ByteBuf result, final Throwable throwableFromCallback) {
            if (throwableFromCallback != null) {
                callback.onResult(null, throwableFromCallback);
            } else {
                try {
                    ReplyHeader replyHeader;
                    ByteBufferBsonInput headerInputBuffer = new ByteBufferBsonInput(result);
                    try {
                        replyHeader = new ReplyHeader(headerInputBuffer, description.getMaxMessageSize());
                    } finally {
                        headerInputBuffer.close();
                    }

                    if (replyHeader.getMessageLength() == REPLY_HEADER_LENGTH) {
                        onSuccess(new ResponseBuffers(replyHeader, null));
                    } else {
                        readAsync(replyHeader.getMessageLength() - REPLY_HEADER_LENGTH,
                                  new ResponseBodyCallback(replyHeader));
                    }
                } catch (Throwable t) {
                    callback.onResult(null, t);
                }
            }
        }

        private void onSuccess(final ResponseBuffers responseBuffers) {

            if (responseBuffers == null) {
                callback.onResult(null, new MongoException("Unexpected empty response buffers"));
                return;
            }

            try {
                callback.onResult(responseBuffers, null);
            } catch (Throwable t) {
                LOGGER.warn("Exception calling callback", t);
            }
        }

        private class ResponseBodyCallback implements SingleResultCallback<ByteBuf> {
            private final ReplyHeader replyHeader;

            ResponseBodyCallback(final ReplyHeader replyHeader) {
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

    private void failAllQueuedReads(final Throwable t) {
        close();
        Iterator<Map.Entry<Integer, SingleResultCallback<ResponseBuffers>>> it = readQueue.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, SingleResultCallback<ResponseBuffers>> pairs = it.next();
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(format("Processing unknown failed message: %s. ([%s] %s)", pairs.getKey(), getId(), serverId));
            }
            SingleResultCallback<ResponseBuffers> callback = pairs.getValue();
            it.remove();
            try {
                callback.onResult(null, t);
            } catch (Throwable tr) {
                LOGGER.warn("Exception calling callback", tr);
            }
        }
    }

    private static class SendMessageRequest {
        private final SingleResultCallback<Void> callback;
        private final List<ByteBuf> byteBuffers;
        private final int messageId;

        SendMessageRequest(final List<ByteBuf> byteBuffers, final int messageId, final SingleResultCallback<Void> callback) {
            this.byteBuffers = byteBuffers;
            this.messageId = messageId;
            this.callback = callback;
        }

        public SingleResultCallback<Void> getCallback() {
            return callback;
        }

        public List<ByteBuf> getByteBuffers() {
            return byteBuffers;
        }

        public int getMessageId() {
            return messageId;
        }
    }
}
