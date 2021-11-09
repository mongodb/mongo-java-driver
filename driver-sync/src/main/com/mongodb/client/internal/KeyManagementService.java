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

import com.mongodb.ServerAddress;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.internal.connection.SslHelper;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Map;

import static com.mongodb.assertions.Assertions.notNull;

class KeyManagementService {
    private static final Logger LOGGER = Loggers.getLogger("client");
    private final Map<String, SSLContext> kmsProviderSslContextMap;
    private final int timeoutMillis;

    KeyManagementService(final Map<String, SSLContext> kmsProviderSslContextMap, final int timeoutMillis) {
        this.kmsProviderSslContextMap = notNull("kmsProviderSslContextMap", kmsProviderSslContextMap);
        this.timeoutMillis = timeoutMillis;
    }

    public InputStream stream(final String kmsProvider, final String host, final ByteBuffer message) throws IOException {
        ServerAddress serverAddress = new ServerAddress(host);

        LOGGER.info("Connecting to KMS server at " + serverAddress);
        SSLContext sslContext = kmsProviderSslContextMap.get(kmsProvider);

        SocketFactory sslSocketFactory = sslContext == null
                    ? SSLSocketFactory.getDefault() : sslContext.getSocketFactory();
        SSLSocket socket = (SSLSocket) sslSocketFactory.createSocket();
        enableHostNameVerification(socket);

        try {
            socket.setSoTimeout(timeoutMillis);
            socket.connect(new InetSocketAddress(InetAddress.getByName(serverAddress.getHost()), serverAddress.getPort()), timeoutMillis);
        } catch (IOException e) {
            closeSocket(socket);
            throw e;
        }

        try {
            OutputStream outputStream = socket.getOutputStream();

            byte[] bytes = new byte[message.remaining()];

            message.get(bytes);
            outputStream.write(bytes);
        } catch (IOException e) {
            closeSocket(socket);
            throw e;
        }

        try {
            return socket.getInputStream();
        } catch (IOException e) {
            closeSocket(socket);
            throw e;
        }
    }

    private void enableHostNameVerification(final SSLSocket socket) {
        SSLParameters sslParameters = socket.getSSLParameters();
        if (sslParameters == null) {
            sslParameters = new SSLParameters();
        }
        SslHelper.enableHostNameVerification(sslParameters);
        socket.setSSLParameters(sslParameters);
    }

    private void closeSocket(final Socket socket) {
        try {
            socket.close();
        } catch (IOException e) {
            // ignore
        }
    }
}
