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
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.MongoSocketOpenException;
import org.mongodb.connection.MongoSocketReadException;
import org.mongodb.connection.ServerAddress;

import javax.net.SocketFactory;
import java.io.IOException;
import java.net.Socket;
import java.util.List;

// TODO: migrate all the DBPort configuration
class DefaultSocketConnection extends DefaultConnection {
    private final Socket socket;

    public DefaultSocketConnection(final ServerAddress address, final ConnectionSettings settings,
                                   final BufferProvider bufferProvider, final SocketFactory socketFactory) {
        super(address, settings, bufferProvider);
        try {
            socket = socketFactory.createSocket();
            initializeSocket(socket);
        } catch (IOException e) {
            close();
            throw new MongoSocketOpenException("Exception opening socket", getServerAddress(), e);
        }
    }

    @Override
    protected void write(final List<ByteBuf> buffers) throws IOException {
        for (ByteBuf cur : buffers) {
            socket.getOutputStream().write(cur.array(), 0, cur.limit());
        }
    }

    protected void read(final ByteBuf buffer) throws IOException {
        int totalBytesRead = 0;
        byte[] bytes = buffer.array();
        while (totalBytesRead < buffer.limit()) {
            final int bytesRead = socket.getInputStream().read(bytes, totalBytesRead, buffer.limit() - totalBytesRead);
            if (bytesRead == -1) {
                throw new MongoSocketReadException("Prematurely reached end of stream", getServerAddress());
            }
            totalBytesRead += bytesRead;
        }
    }

    public void close() {
        try {
            if (socket != null) {
                socket.close();
            }
            super.close();
        } catch (IOException e) {
            // ignore
        }
    }
}