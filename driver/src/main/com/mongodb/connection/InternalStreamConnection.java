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
import com.mongodb.MongoSocketReadException;
import com.mongodb.MongoSocketReadTimeoutException;
import com.mongodb.MongoSocketWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.event.ConnectionEvent;
import com.mongodb.event.ConnectionListener;
import com.mongodb.event.ConnectionMessageReceivedEvent;
import com.mongodb.event.ConnectionMessagesSentEvent;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.ByteBuf;
import org.bson.io.BasicInputBuffer;
import org.mongodb.MongoCredential;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedByInterruptException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.ReplyHeader.REPLY_HEADER_LENGTH;

class InternalStreamConnection implements InternalConnection {

    private final AtomicInteger incrementingId = new AtomicInteger();

    private final String clusterId;
    private final Stream stream;
    private final ConnectionListener eventPublisher;
    private final List<MongoCredential> credentialList;
    private volatile boolean isClosed;
    private String id;

    InternalStreamConnection(final String clusterId, final Stream stream, final List<MongoCredential> credentialList,
                             final ConnectionListener connectionListener) {
        this.clusterId = notNull("clusterId", clusterId);
        this.stream = notNull("stream", stream);
        this.eventPublisher = notNull("connectionListener", connectionListener);
        notNull("credentialList", credentialList);
        this.credentialList = new ArrayList<MongoCredential>(credentialList);
        initialize();
        connectionListener.connectionOpened(new ConnectionEvent(clusterId, stream.getAddress(), getId()));
    }

    @Override
    public void close() {
        isClosed = true;
        stream.close();
        eventPublisher.connectionClosed(new ConnectionEvent(clusterId, stream.getAddress(), getId()));
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    public ServerAddress getServerAddress() {
        return stream.getAddress();
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public ByteBuf getBuffer(final int size) {
        return stream.getBuffer(size);
    }

    public void sendMessage(final List<ByteBuf> byteBuffers, final int lastRequestId) {
        isTrue("open", !isClosed());
        try {
            stream.write(byteBuffers);
            eventPublisher.messagesSent(new ConnectionMessagesSentEvent(clusterId, stream.getAddress(), getId(), lastRequestId,
                                                                        getTotalRemaining(byteBuffers)));
        } catch (IOException e) {
            close();
            throw new MongoSocketWriteException("Exception sending message", getServerAddress(), e);
        }
    }

    @Override
    public ResponseBuffers receiveMessage() {
        isTrue("open", !isClosed());
        try {
            ResponseBuffers responseBuffers = receiveResponseBuffers();
            eventPublisher.messageReceived(new ConnectionMessageReceivedEvent(clusterId,
                                                                              stream.getAddress(),
                                                                              getId(),
                                                                              responseBuffers.getReplyHeader().getResponseTo(),
                                                                              responseBuffers.getReplyHeader().getMessageLength()));
            return responseBuffers;
        } catch (IOException e) {
            close();
            throw translateReadException(e);
        } catch (MongoException e) {
            close();
            throw e;
        } catch (RuntimeException e) {
            close();
            throw new MongoInternalException("Unexpected runtime exception", e);
        }
    }

    @Override
    public void sendMessageAsync(final List<ByteBuf> byteBuffers, final int lastRequestId, final SingleResultCallback<Void> callback) {
        isTrue("open", !isClosed());
        stream.writeAsync(byteBuffers, new AsyncCompletionHandler<Void>() {
            @Override
            public void completed(final Void t) {
                eventPublisher.messagesSent(new ConnectionMessagesSentEvent(clusterId, stream.getAddress(), getId(), lastRequestId,
                                                                            getTotalRemaining(byteBuffers)));
                callback.onResult(null, null);
            }

            @Override
            public void failed(final Throwable t) {
                if (t instanceof MongoException) {
                    callback.onResult(null, (MongoException) t);
                } else if (t instanceof IOException) {
                    callback.onResult(null, new MongoSocketWriteException("Exception writing to stream", getServerAddress(), t));
                } else {
                    callback.onResult(null, new MongoInternalException("Unexpected exception", t));
                }
            }
        });
    }

    @Override
    public void receiveMessageAsync(final SingleResultCallback<ResponseBuffers> callback) {
        fillAndFlipBuffer(REPLY_HEADER_LENGTH, new ResponseHeaderCallback(callback));
    }

    private void fillAndFlipBuffer(final int numBytes, final SingleResultCallback<ByteBuf> callback) {
        stream.readAsync(numBytes, new AsyncCompletionHandler<ByteBuf>() {
            @Override
            public void completed(final ByteBuf buffer) {
                callback.onResult(buffer, null);
            }

            @Override
            public void failed(final Throwable t) {
                if (t instanceof MongoException) {
                    callback.onResult(null, (MongoException) t);
                } else if (t instanceof IOException) {
                    callback.onResult(null, new MongoSocketReadException("Exception writing to stream", getServerAddress(), t));
                } else {
                    callback.onResult(null, new MongoInternalException("Unexpected exception", t));
                }
            }
        });
    }

    private MongoException translateReadException(final IOException e) {
        close();
        if (e instanceof SocketTimeoutException) {
            throw new MongoSocketReadTimeoutException("Timeout while receiving message", getServerAddress(), e);
        } else if (e instanceof InterruptedIOException || e instanceof ClosedByInterruptException) {
            throw new MongoInterruptedException("Interrupted while receiving message", e);
        } else {
            throw new MongoSocketReadException("Exception receiving message", getServerAddress(), e);
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

    private void initialize() {
        initializeConnectionId();
        authenticateAll();
        // try again if there was an exception calling getlasterror before authenticating
        if (id.contains("*")) {
            initializeConnectionId();
        }
    }

    private void initializeConnectionId() {
        BsonDocument response = CommandHelper.executeCommandWithoutCheckingForFailure("admin",
                                                                                      new BsonDocument("getlasterror", new BsonInt32(1)),
                                                                                      this);
        id = "conn" + (response.containsKey("connectionId")
                       ? response.getNumber("connectionId").intValue()
                       : "*" + incrementingId.incrementAndGet() + "*");
    }

    private void authenticateAll() {
        for (final MongoCredential cur : credentialList) {
            createAuthenticator(cur).authenticate();
        }
    }

    private Authenticator createAuthenticator(final MongoCredential credential) {
        switch (credential.getMechanism()) {
            case MONGODB_CR:
                return new NativeAuthenticator(credential, this);
            case GSSAPI:
                return new GSSAPIAuthenticator(credential, this);
            case PLAIN:
                return new PlainAuthenticator(credential, this);
            case MONGODB_X509:
                return new X509Authenticator(credential, this);
            default:
                throw new IllegalArgumentException("Unsupported authentication protocol: " + credential.getMechanism());
        }
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
            eventPublisher.messageReceived(new ConnectionMessageReceivedEvent(clusterId,
                                                                              stream.getAddress(),
                                                                              getId(),
                                                                              responseBuffers.getReplyHeader().getResponseTo(),
                                                                              responseBuffers.getReplyHeader().getMessageLength()));
            callback.onResult(responseBuffers, null);
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
