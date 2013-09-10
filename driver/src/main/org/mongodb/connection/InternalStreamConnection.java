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

import org.bson.ByteBuf;
import org.bson.io.BasicInputBuffer;
import org.mongodb.CommandResult;
import org.mongodb.Document;
import org.mongodb.MongoCommandFailureException;
import org.mongodb.MongoCredential;
import org.mongodb.MongoException;
import org.mongodb.MongoInternalException;
import org.mongodb.MongoInterruptedException;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.event.ConnectionListener;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedByInterruptException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.assertions.Assertions.notNull;
import static org.mongodb.connection.ReplyHeader.REPLY_HEADER_LENGTH;

class InternalStreamConnection implements InternalConnection {

    private final AtomicInteger incrementingId = new AtomicInteger();

    private final Stream stream;
    private final ConnectionListener eventPublisher;
    private List<MongoCredential> credentialList;
    private final BufferProvider bufferProvider;
    private volatile boolean isClosed;
    private String id;

    InternalStreamConnection(final Stream stream, final List<MongoCredential> credentialList, final BufferProvider bufferProvider,
                             final ConnectionListener eventPublisher) {
        this.stream = notNull("stream", stream);
        this.eventPublisher = notNull("eventPublisher", eventPublisher);
        notNull("credentialList", credentialList);
        this.credentialList = new ArrayList<MongoCredential>(credentialList);
        this.bufferProvider = notNull("bufferProvider", bufferProvider);
        initialize();
    }

    @Override
    public void close() {
        isClosed = true;
        stream.close();
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

    public void sendMessage(final List<ByteBuf> byteBuffers) {
        isTrue("open", !isClosed());
        try {
            stream.write(byteBuffers);
        } catch (IOException e) {
            close();
            throw new MongoSocketWriteException("Exception sending message", getServerAddress(), e);
        }
    }

    @Override
    public ResponseBuffers receiveMessage() {
        isTrue("open", !isClosed());
        try {
            return receiveMessage(System.nanoTime());
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
    public void sendMessageAsync(final List<ByteBuf> byteBuffers, final SingleResultCallback<Void> callback) {
        isTrue("open", !isClosed());
        stream.writeAsync(byteBuffers, new AsyncCompletionHandler() {
            @Override
            public void completed() {
                callback.onResult(null, null);
            }

            @Override
            public void failed(final Throwable t) {
                if (t instanceof MongoException) {
                    callback.onResult(null, (MongoException) t);
                }
                else if (t instanceof IOException) {
                    callback.onResult(null, new MongoSocketWriteException("Exception writing to stream", getServerAddress(),
                            (IOException) t));
                }
                else {
                    callback.onResult(null, new MongoInternalException("Unexpected exception", t));
                }
            }
        });
    }

    @Override
    public void receiveMessageAsync(final SingleResultCallback<ResponseBuffers> callback) {
        fillAndFlipBuffer(bufferProvider.get(REPLY_HEADER_LENGTH), new ResponseHeaderCallback(System.nanoTime(), callback));
    }

    private void fillAndFlipBuffer(final ByteBuf buffer, final SingleResultCallback<ByteBuf> callback) {
        stream.readAsync(buffer, new AsyncCompletionHandler() {
            @Override
            public void completed() {
                callback.onResult(buffer, null);
            }

            @Override
            public void failed(final Throwable t) {
                if (t instanceof MongoException) {
                    callback.onResult(null, (MongoException) t);
                }
                else if (t instanceof IOException) {
                    callback.onResult(null, new MongoSocketReadException("Exception writing to stream", getServerAddress(), t));
                }
                else {
                    callback.onResult(null, new MongoInternalException("Unexpected exception", t));
                }
            }
        });
    }

    private MongoException translateReadException(final IOException e) {
        close();
        if (e instanceof SocketTimeoutException) {
            throw new MongoSocketReadTimeoutException("Timeout while receiving message", getServerAddress(), (SocketTimeoutException) e);
        }
        else if (e instanceof InterruptedIOException || e instanceof ClosedByInterruptException) {
            throw new MongoInterruptedException("Interrupted while receiving message", e);
        }
        else {
            throw new MongoSocketReadException("Exception receiving message", getServerAddress(), e);
        }
    }

    private ResponseBuffers receiveMessage(final long start) throws IOException {
        ByteBuf headerByteBuffer = bufferProvider.get(REPLY_HEADER_LENGTH);

        final ReplyHeader replyHeader;
        stream.read(headerByteBuffer);
        BasicInputBuffer headerInputBuffer = new BasicInputBuffer(headerByteBuffer);
        try {
            replyHeader = new ReplyHeader(headerInputBuffer);
        } finally {
            headerInputBuffer.close();
        }

        ByteBuf bodyByteBuffer = null;

        if (replyHeader.getNumberReturned() > 0) {
            bodyByteBuffer = bufferProvider.get(replyHeader.getMessageLength() - REPLY_HEADER_LENGTH);
            stream.read(bodyByteBuffer);
        }

        return new ResponseBuffers(replyHeader, bodyByteBuffer, System.nanoTime() - start);
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
        try {
            int connectionIdFromServer;
            try {
                CommandResult result = CommandHelper.executeCommand("admin", new Document("getlasterror", 1), new DocumentCodec(), this,
                        bufferProvider);
                connectionIdFromServer = result.getResponse().getInteger("connectionId");

            } catch (MongoCommandFailureException e) {
                connectionIdFromServer = e.getCommandResult().getResponse().getInteger("connectionId");
            }
            id = "conn" + connectionIdFromServer;
        } catch (Exception e) {
            id = "conn*" + incrementingId.incrementAndGet() + "*";
        }
    }

    private void authenticateAll() {
        for (MongoCredential cur : credentialList) {
            createAuthenticator(cur).authenticate();
        }
    }

    private Authenticator createAuthenticator(final MongoCredential credential) {
        switch (credential.getMechanism()) {
            case MONGODB_CR:
                return new NativeAuthenticator(credential, this, bufferProvider);
            case GSSAPI:
                return new GSSAPIAuthenticator(credential, this, bufferProvider);
            case PLAIN:
                return new PlainAuthenticator(credential, this, bufferProvider);
            case MONGODB_X509:
                return new X509Authenticator(credential, this, bufferProvider);
            default:
                throw new IllegalArgumentException("Unsupported authentication protocol: " + credential.getMechanism());
        }
    }

    private class ResponseHeaderCallback implements SingleResultCallback<ByteBuf> {
        private final SingleResultCallback<ResponseBuffers> callback;
        private final long start;

        public ResponseHeaderCallback(final long start, final SingleResultCallback<ResponseBuffers> callback) {
            this.callback = callback;
            this.start = start;
        }

        @Override
        public void onResult(final ByteBuf result, final MongoException e) {
            if (e != null) {
                callback.onResult(null, e);
            }
            else {
                ReplyHeader replyHeader;
                final BasicInputBuffer headerInputBuffer = new BasicInputBuffer(result);
                try {
                    replyHeader = new ReplyHeader(headerInputBuffer);
                } finally {
                    headerInputBuffer.close();
                }

                if (replyHeader.getMessageLength() == REPLY_HEADER_LENGTH) {
                    callback.onResult(new ResponseBuffers(replyHeader, null, System.nanoTime() - start), null);
                }
                else {
                    fillAndFlipBuffer(bufferProvider.get(replyHeader.getMessageLength() - REPLY_HEADER_LENGTH),
                            new ResponseBodyCallback(replyHeader));
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
                }
                else {
                    callback.onResult(new ResponseBuffers(replyHeader, result, System.nanoTime() - start), null);
                }
            }
        }
    }
}
