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
import com.mongodb.KmsConnectContext;
import com.mongodb.ServerAddress;
import com.mongodb.internal.connection.SslHelper;
import com.mongodb.lang.Nullable;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Establishes the TLS connection used for a Key Management Service (KMS) request.
 *
 * <p>When a {@link KmsConnectCallback} is configured, the callback establishes the underlying connection, which may be
 * to an intermediary such as an HTTP proxy, and TLS for the KMS host is layered on top of the socket it returns. TLS is
 * always negotiated end-to-end with the KMS host: Server Name Indication and certificate hostname verification are
 * configured from the KMS address rather than from wherever the socket is actually connected, so an intermediary
 * relays the session without being able to read it.</p>
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class KmsSocketConnector {

    /**
     * Connects to the KMS host and returns a socket with an established TLS session.
     *
     * @param sslContext the SSL context configured for the KMS provider, or null to use the default
     * @param kmsConnectCallback the callback that establishes the connection, or null to connect directly
     * @param kmsProvider the KMS provider that the request belongs to, passed on to {@code kmsConnectCallback}
     * @param kmsAddress the address of the KMS host
     * @param soTimeoutMillis the socket read timeout to apply
     * @param connectTimeoutMillis the time available to establish the connection, or 0 if no limit applies
     * @return a connected socket with an established TLS session with the KMS host
     * @throws IOException if the connection or the TLS handshake fails
     */
    public static SSLSocket connect(@Nullable final SSLContext sslContext,
            @Nullable final KmsConnectCallback kmsConnectCallback, final String kmsProvider,
            final ServerAddress kmsAddress, final int soTimeoutMillis, final long connectTimeoutMillis)
            throws IOException {
        SSLSocketFactory sslSocketFactory = sslContext == null
                ? (SSLSocketFactory) SSLSocketFactory.getDefault() : sslContext.getSocketFactory();

        if (kmsConnectCallback == null) {
            return connectDirectly(sslSocketFactory, kmsAddress, soTimeoutMillis, connectTimeoutMillis);
        }
        return connectViaCallback(sslSocketFactory, kmsConnectCallback, kmsProvider, kmsAddress, soTimeoutMillis,
                connectTimeoutMillis);
    }

    private static SSLSocket connectDirectly(final SSLSocketFactory sslSocketFactory, final ServerAddress kmsAddress,
            final int soTimeoutMillis, final long connectTimeoutMillis) throws IOException {
        SSLSocket socket = (SSLSocket) sslSocketFactory.createSocket();
        enableHostNameVerification(socket);
        try {
            socket.setSoTimeout(soTimeoutMillis);
            socket.connect(new InetSocketAddress(InetAddress.getByName(kmsAddress.getHost()), kmsAddress.getPort()),
                    Math.toIntExact(connectTimeoutMillis));
        } catch (IOException | RuntimeException e) {
            closeSocket(socket);
            throw e;
        }
        return socket;
    }

    /**
     * Obtains a socket from the callback, then layers TLS for the KMS host on top of it. The callback may have
     * connected to an intermediary, so Server Name Indication and hostname verification are configured from
     * {@code kmsAddress} rather than from the address the socket is actually connected to.
     */
    private static SSLSocket connectViaCallback(final SSLSocketFactory sslSocketFactory,
            final KmsConnectCallback kmsConnectCallback, final String kmsProvider, final ServerAddress kmsAddress,
            final int soTimeoutMillis, final long connectTimeoutMillis) throws IOException {
        Socket connectedSocket = notNull("socket returned by KmsConnectCallback",
                kmsConnectCallback.connect(new KmsConnectContext(kmsProvider, kmsAddress, connectTimeoutMillis)));

        SSLSocket socket;
        try {
            // Layers TLS over the already-connected socket. autoClose ensures that closing the returned socket also
            // closes the socket the callback established.
            socket = (SSLSocket) sslSocketFactory.createSocket(connectedSocket, kmsAddress.getHost(), kmsAddress.getPort(), true);
        } catch (IOException | RuntimeException e) {
            closeSocket(connectedSocket);
            throw e;
        }

        try {
            // Even though the callback's connection is already established, the TLS handshake has not been performed
            // yet, so SSL parameters can still be set. They target the KMS host, not any intermediary.
            SSLParameters sslParameters = socket.getSSLParameters();
            if (sslParameters == null) {
                sslParameters = new SSLParameters();
            }
            SslHelper.enableSni(kmsAddress.getHost(), sslParameters);
            SslHelper.enableHostNameVerification(sslParameters);
            socket.setSSLParameters(sslParameters);
            socket.setSoTimeout(soTimeoutMillis);
            // Handshake explicitly so that a TLS failure is reported here rather than on the first write.
            socket.startHandshake();
        } catch (IOException | RuntimeException e) {
            closeSocket(socket);
            throw e;
        }
        return socket;
    }

    private static void enableHostNameVerification(final SSLSocket socket) {
        SSLParameters sslParameters = socket.getSSLParameters();
        if (sslParameters == null) {
            sslParameters = new SSLParameters();
        }
        SslHelper.enableHostNameVerification(sslParameters);
        socket.setSSLParameters(sslParameters);
    }

    private static void closeSocket(final Socket socket) {
        try {
            socket.close();
        } catch (IOException | RuntimeException e) {
            // ignore
        }
    }

    private KmsSocketConnector() {
    }
}
