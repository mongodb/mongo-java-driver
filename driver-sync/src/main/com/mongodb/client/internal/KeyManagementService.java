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
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.connection.SslHelper;
import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;
import com.mongodb.internal.time.Timeout;
import com.mongodb.lang.Nullable;
import org.jetbrains.annotations.NotNull;

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
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.Map;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

class KeyManagementService {
    private static final Logger LOGGER = Loggers.getLogger("client");
    private final Map<String, SSLContext> kmsProviderSslContextMap;
    private final int timeoutMillis;

    KeyManagementService(final Map<String, SSLContext> kmsProviderSslContextMap, final int timeoutMillis) {
        this.kmsProviderSslContextMap = notNull("kmsProviderSslContextMap", kmsProviderSslContextMap);
        this.timeoutMillis = timeoutMillis;
    }

    public InputStream stream(final String kmsProvider, final String host, final ByteBuffer message, @Nullable final Timeout operationTimeout) throws IOException {
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
            return OperationTimeoutAwareInputStream.wrapIfNeeded(operationTimeout, socket);
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
        } catch (IOException | RuntimeException e) {
            // ignore
        }
    }

    private static final class OperationTimeoutAwareInputStream extends InputStream {
        private final Socket socket;
        private final Timeout operationTimeout;
        private final InputStream wrapped;

        /**
         * @param socket - socket to set timeout on.
         * @param operationTimeout - non-infinite timeout.
         */
        private OperationTimeoutAwareInputStream(final Socket socket, final Timeout operationTimeout) throws IOException {
            this.socket = socket;
            this.operationTimeout = operationTimeout;
            this.wrapped = socket.getInputStream();
        }

        public static InputStream wrapIfNeeded(@Nullable final Timeout operationTimeout, final SSLSocket socket) throws IOException {
            return Timeout.nullAsInfinite(operationTimeout).checkedCall(NANOSECONDS,
                    () -> socket.getInputStream(),
                    (ns) -> new OperationTimeoutAwareInputStream(socket, assertNotNull(operationTimeout)),
                    () -> new OperationTimeoutAwareInputStream(socket, assertNotNull(operationTimeout)));
        }

        private void setSocketSoTimeoutToOperationTimeout() throws SocketException {
            operationTimeout.checkedRun(MILLISECONDS,
                    () -> {
                        throw new AssertionError("operationTimeout cannot be infinite");
                    },
                    (ms) -> socket.setSoTimeout(Math.toIntExact(ms)),
                    () -> {
                        throw TimeoutContext.createMongoTimeoutException("Timeout has expired while reading from KMS server");
                    });
        }

        @Override
        public int read() throws IOException {
            setSocketSoTimeoutToOperationTimeout();
            return wrapped.read();
        }

        @Override
        public int read(@NotNull final byte[] b) throws IOException {
            setSocketSoTimeoutToOperationTimeout();
            return wrapped.read(b);
        }

        @Override
        public int read(@NotNull final byte[] b, final int off, final int len) throws IOException {
            setSocketSoTimeoutToOperationTimeout();
            return wrapped.read(b, off, len);
        }

        @Override
        public void close() throws IOException {
            wrapped.close();
        }

        @Override
        public long skip(final long n) throws IOException {
            setSocketSoTimeoutToOperationTimeout();
            return wrapped.skip(n);
        }

        @Override
        public int available() throws IOException {
            return wrapped.available();
        }

        @Override
        public synchronized void mark(final int readlimit) {
            wrapped.mark(readlimit);
        }

        @Override
        public synchronized void reset() throws IOException {
            wrapped.reset();
        }

        @Override
        public boolean markSupported() {
            return wrapped.markSupported();
        }
    }
}
