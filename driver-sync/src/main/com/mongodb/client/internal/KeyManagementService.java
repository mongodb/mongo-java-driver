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

import com.mongodb.ProxySettings;
import com.mongodb.ServerAddress;
import com.mongodb.internal.connection.SocksSocket;
import com.mongodb.internal.connection.SslHelper;
import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;
import com.mongodb.lang.Nullable;

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
    @Nullable
    private final ProxySettings proxySettings;

    KeyManagementService(final Map<String, SSLContext> kmsProviderSslContextMap, @Nullable final ProxySettings proxySettings,
                         final int timeoutMillis) {
        this.kmsProviderSslContextMap = notNull("kmsProviderSslContextMap", kmsProviderSslContextMap);
        this.timeoutMillis = timeoutMillis;
        this.proxySettings = proxySettings;
    }

    public InputStream stream(final String kmsProvider, final String host, final ByteBuffer message) throws IOException {
        ServerAddress serverAddress = new ServerAddress(host);

        LOGGER.info("Connecting to KMS server at " + serverAddress);
        SSLContext sslContext = kmsProviderSslContextMap.get(kmsProvider);
        SSLSocketFactory sslSocketFactory = sslContext == null
                ? (SSLSocketFactory) SSLSocketFactory.getDefault() : sslContext.getSocketFactory();

        Socket socket = null;
        try {
            if (isProxyEnabled()) {
                socket = new SocksSocket(proxySettings);
                socket.setSoTimeout(timeoutMillis);
                socket.connect(InetSocketAddress.createUnresolved(host, serverAddress.getPort()), timeoutMillis);
                socket = sslSocketFactory.createSocket(socket, host, serverAddress.getPort(), true);
            } else {
                socket = sslSocketFactory.createSocket();
                socket.connect(new InetSocketAddress(InetAddress.getByName(serverAddress.getHost()), serverAddress.getPort()),
                        timeoutMillis);
            }
            enableHostNameVerification((SSLSocket) socket);
        } catch (IOException e) {
            if (socket != null) {
                closeSocket(socket);
            }
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

    private boolean isProxyEnabled() {
        return proxySettings != null && proxySettings.getHost() != null;
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
