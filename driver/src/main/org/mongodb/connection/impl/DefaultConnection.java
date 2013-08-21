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
import org.mongodb.MongoException;
import org.mongodb.MongoInternalException;
import org.mongodb.MongoInterruptedException;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.Connection;
import org.mongodb.connection.MongoSocketReadException;
import org.mongodb.connection.MongoSocketReadTimeoutException;
import org.mongodb.connection.MongoSocketWriteException;
import org.mongodb.connection.ReplyHeader;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ResponseSettings;
import org.mongodb.connection.ServerAddress;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedByInterruptException;
import java.util.List;

import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.connection.ReplyHeader.REPLY_HEADER_LENGTH;

abstract class DefaultConnection implements Connection {
    private final ServerAddress serverAddress;
    private ConnectionSettings settings;
    private final BufferProvider bufferProvider;
    private volatile boolean isClosed;

    DefaultConnection(final ServerAddress serverAddress, final ConnectionSettings settings, final BufferProvider bufferProvider) {
        this.serverAddress = serverAddress;
        this.settings = settings;
        this.bufferProvider = bufferProvider;
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
    public ResponseBuffers receiveMessage(final ResponseSettings responseSettings) {
        isTrue("open", !isClosed());
        try {
            return receiveMessage(responseSettings, System.nanoTime());
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

    private ResponseBuffers receiveMessage(final ResponseSettings responseSettings, final long start) throws IOException {
        ByteBuf headerByteBuffer = bufferProvider.get(REPLY_HEADER_LENGTH);

        final ReplyHeader replyHeader;
        read(headerByteBuffer);
        BasicInputBuffer headerInputBuffer = new BasicInputBuffer(headerByteBuffer);
        try {
            replyHeader = new ReplyHeader(headerInputBuffer);
        } finally {
            headerInputBuffer.close();
        }

        if (replyHeader.getResponseTo() != responseSettings.getResponseTo()) {
            throw new MongoInternalException(
                    String.format("The responseTo (%d) in the response does not match the requestId (%d) in the request",
                            replyHeader.getResponseTo(), responseSettings.getResponseTo()));
        }

        if (replyHeader.getMessageLength() > responseSettings.getMaxMessageSize()) {
            throw new MongoInternalException(String.format("Unexpectedly large message length of %d exceeds maximum of %d",
                    replyHeader.getMessageLength(), responseSettings.getMaxMessageSize()));
        }

        ByteBuf bodyByteBuffer = null;

        if (replyHeader.getNumberReturned() > 0) {
            bodyByteBuffer = bufferProvider.get(replyHeader.getMessageLength() - REPLY_HEADER_LENGTH);
            read(bodyByteBuffer);
        }

        return new ResponseBuffers(replyHeader, bodyByteBuffer, System.nanoTime() - start);
    }

    protected final void initializeSocket(final Socket socket) throws IOException {
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
    }
}
