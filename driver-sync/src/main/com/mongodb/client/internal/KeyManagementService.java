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

import com.mongodb.KmsConnectCallback;
import com.mongodb.ServerAddress;
import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;
import com.mongodb.lang.Nullable;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Map;

import static com.mongodb.assertions.Assertions.notNull;

class KeyManagementService {
    private static final Logger LOGGER = Loggers.getLogger("client");
    private final Map<String, SSLContext> kmsProviderSslContextMap;
    @Nullable
    private final KmsConnectCallback kmsConnectCallback;
    private final int timeoutMillis;

    KeyManagementService(final Map<String, SSLContext> kmsProviderSslContextMap,
            @Nullable final KmsConnectCallback kmsConnectCallback, final int timeoutMillis) {
        this.kmsProviderSslContextMap = notNull("kmsProviderSslContextMap", kmsProviderSslContextMap);
        this.kmsConnectCallback = kmsConnectCallback;
        this.timeoutMillis = timeoutMillis;
    }

    public InputStream stream(final String kmsProvider, final String host, final ByteBuffer message) throws IOException {
        ServerAddress serverAddress = new ServerAddress(host);

        LOGGER.info("Connecting to KMS server at " + serverAddress);
        SSLContext sslContext = kmsProviderSslContextMap.get(kmsProvider);

        SSLSocket socket = KmsSocketConnector.connect(sslContext, kmsConnectCallback, serverAddress,
                timeoutMillis, remainingMillis());


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

    /**
     * Determines the time available for reaching the KMS server, which the specification requires to be the time
     * remaining in the operation when a client-side operation timeout is in use.
     *
     * <p>Visible for testing.</p>
     *
     * @return the remaining time available for connecting to the KMS server, in milliseconds, never larger than the
     * configured connect timeout.
     */
    long remainingMillis() {
        return timeoutMillis;
    }

    private void closeSocket(final Socket socket) {
        try {
            socket.close();
        } catch (IOException | RuntimeException e) {
            // ignore
        }
    }
}
