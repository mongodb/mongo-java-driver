/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.client.internal;

import com.mongodb.MongoSocketOpenException;
import com.mongodb.MongoSocketReadException;
import com.mongodb.MongoSocketWriteException;
import com.mongodb.ServerAddress;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;

class KeyManagementService {
    private final SSLContext sslContext;
    private final int port;
    private final int timeoutMillis;

    KeyManagementService(final SSLContext sslContext, final int port, final int timeoutMillis) {
        this.sslContext = sslContext;
        this.port = port;
        this.timeoutMillis = timeoutMillis;
    }

    public InputStream stream(final String host, final ByteBuffer message) {
        Socket socket;
        try {
            socket = sslContext.getSocketFactory().createSocket();
        } catch (IOException e) {
            throw new MongoSocketOpenException("Exception opening connection to Key Management Service", new ServerAddress(host, port), e);
        }

        try {
            socket.setSoTimeout(timeoutMillis);
            socket.connect(new InetSocketAddress(InetAddress.getByName(host), port), timeoutMillis);
        } catch (IOException e) {
            closeSocket(socket);
            throw new MongoSocketOpenException("Exception opening connection to Key Management Service", new ServerAddress(host, port), e);
        }

        try {
            OutputStream outputStream = socket.getOutputStream();

            byte[] bytes = new byte[message.remaining()];

            message.get(bytes);
            outputStream.write(bytes);
        } catch (IOException e) {
            closeSocket(socket);
            throw new MongoSocketWriteException("Exception sending message to Key Management Service",
                    new ServerAddress(host, port), e);
        }

        try {
            return socket.getInputStream();
        } catch (IOException e) {
            closeSocket(socket);
            throw new MongoSocketReadException("Exception receiving message from Key Management Service",
                    new ServerAddress(host, port), e);
        }
    }

    public int getPort() {
        return port;
    }

    private void closeSocket(final Socket socket) {
        try {
            socket.close();
        } catch (IOException e) {
            // ignore
        }
    }
}
