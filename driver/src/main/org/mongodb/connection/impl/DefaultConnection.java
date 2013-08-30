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

import org.bson.ByteBuf;
import org.bson.io.BasicInputBuffer;
import org.mongodb.CommandResult;
import org.mongodb.Document;
import org.mongodb.MongoCredential;
import org.mongodb.MongoException;
import org.mongodb.MongoInternalException;
import org.mongodb.MongoInterruptedException;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.command.MongoCommandFailureException;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.Connection;
import org.mongodb.connection.MongoSocketReadException;
import org.mongodb.connection.MongoSocketReadTimeoutException;
import org.mongodb.connection.MongoSocketWriteException;
import org.mongodb.connection.ReplyHeader;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ServerAddress;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedByInterruptException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.assertions.Assertions.notNull;
import static org.mongodb.connection.ReplyHeader.REPLY_HEADER_LENGTH;

abstract class DefaultConnection implements Connection {

    private final AtomicInteger incrementingId = new AtomicInteger();

    private final ServerAddress serverAddress;
    private ConnectionSettings settings;
    private List<MongoCredential> credentialList;
    private final BufferProvider bufferProvider;
    private volatile boolean isClosed;
    private String id;

    DefaultConnection(final ServerAddress serverAddress, final ConnectionSettings settings, final List<MongoCredential> credentialList,
                      final BufferProvider bufferProvider) {
        this.serverAddress = notNull("serverAddress", serverAddress);
        this.settings = notNull("settings", settings);
        notNull("credentialList", credentialList);
        this.credentialList = new ArrayList<MongoCredential>(credentialList);
        this.bufferProvider = notNull("bufferProvider", bufferProvider);
    }

    @Override
    public void close() {
        isClosed = true;
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    public ServerAddress getServerAddress() {
        return serverAddress;
    }

    @Override
    public String getId() {
        return id;
    }

    public void sendMessage(final List<ByteBuf> byteBuffers) {
        isTrue("open", !isClosed());
        try {
            write(byteBuffers);
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

    protected abstract void write(final List<ByteBuf> buffers) throws IOException;

    protected abstract void read(final ByteBuf buffer) throws IOException;

    private MongoException translateReadException(final IOException e) {
        close();
        if (e instanceof SocketTimeoutException) {
            throw new MongoSocketReadTimeoutException("Timeout while receiving message", serverAddress, (SocketTimeoutException) e);
        }
        else if (e instanceof InterruptedIOException || e instanceof ClosedByInterruptException) {
            throw new MongoInterruptedException("Interrupted while receiving message", e);
        }
        else {
            throw new MongoSocketReadException("Exception receiving message", serverAddress, e);
        }
    }

    private ResponseBuffers receiveMessage(final long start) throws IOException {
        ByteBuf headerByteBuffer = bufferProvider.get(REPLY_HEADER_LENGTH);

        final ReplyHeader replyHeader;
        read(headerByteBuffer);
        BasicInputBuffer headerInputBuffer = new BasicInputBuffer(headerByteBuffer);
        try {
            replyHeader = new ReplyHeader(headerInputBuffer);
        } finally {
            headerInputBuffer.close();
        }

        ByteBuf bodyByteBuffer = null;

        if (replyHeader.getNumberReturned() > 0) {
            bodyByteBuffer = bufferProvider.get(replyHeader.getMessageLength() - REPLY_HEADER_LENGTH);
            read(bodyByteBuffer);
        }

        return new ResponseBuffers(replyHeader, bodyByteBuffer, System.nanoTime() - start);
    }

    protected final void initialize(final Socket socket) throws IOException {
        socket.setTcpNoDelay(true);
        socket.setSoTimeout(settings.getReadTimeoutMS());
        socket.setKeepAlive(settings.isKeepAlive());
        if (settings.getReceiveBufferSize() > 0) {
            socket.setReceiveBufferSize(settings.getReceiveBufferSize());
        }
        if (settings.getSendBufferSize() > 0) {
            socket.setSendBufferSize(settings.getSendBufferSize());
        }
        socket.connect(getServerAddress().getSocketAddress(), settings.getConnectTimeoutMS());
        initializeConnectionId();
        authenticateAll();
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
}
