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

package com.mongodb.internal.capi;

import com.mongodb.MongoSocketException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ProxyProtocol;
import com.mongodb.connection.ProxySettings;
import com.mongodb.internal.connection.HttpProxyTunnel;
import com.mongodb.internal.connection.SocksSocket;
import com.mongodb.internal.connection.SslHelper;
import com.mongodb.lang.Nullable;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;

import static com.mongodb.assertions.Assertions.assertNotNull;

/**
 * Establishes the TLS connection used for a Key Management Service (KMS) request, optionally through a proxy.
 *
 * <p>When a proxy is configured, the connection to the KMS host is established through it first, and TLS is then
 * layered on top of the resulting socket. TLS is always negotiated end-to-end with the KMS host: Server Name Indication
 * and certificate hostname verification are configured from the KMS address rather than from the proxy, so a proxy
 * relays the session without being able to read it.</p>
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class KmsSocketConnector {

    /**
     * Connects to the KMS host and returns a socket with an established TLS session.
     *
     * @param sslContext the SSL context configured for the KMS provider, or null to use the default
     * @param proxySettings the proxy to reach the KMS host through, or null to connect directly
     * @param kmsAddress the address of the KMS host
     * @param soTimeoutMillis the socket read timeout to apply
     * @param connectTimeoutMillis the time available to establish the connection, or 0 if no limit applies
     * @return a connected socket with an established TLS session with the KMS host
     * @throws IOException if the connection, the proxy handshake, or the TLS handshake fails
     */
    public static SSLSocket connect(@Nullable final SSLContext sslContext,
            @Nullable final ProxySettings proxySettings, final ServerAddress kmsAddress,
            final int soTimeoutMillis, final long connectTimeoutMillis) throws IOException {
        SSLSocketFactory sslSocketFactory = sslContext == null
                ? (SSLSocketFactory) SSLSocketFactory.getDefault() : sslContext.getSocketFactory();

        if (proxySettings == null || !proxySettings.isProxyEnabled()) {
            return connectDirectly(sslSocketFactory, kmsAddress, soTimeoutMillis, connectTimeoutMillis);
        }
        return connectThroughProxy(sslSocketFactory, proxySettings, kmsAddress, soTimeoutMillis, connectTimeoutMillis);
    }

    private static SSLSocket connectDirectly(final SSLSocketFactory sslSocketFactory, final ServerAddress kmsAddress,
            final int soTimeoutMillis, final long connectTimeoutMillis) throws IOException {
        SSLSocket socket = (SSLSocket) sslSocketFactory.createSocket();
        SSLParameters sslParameters = socket.getSSLParameters();
        SslHelper.enableHostNameVerification(sslParameters);
        socket.setSSLParameters(sslParameters);
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
     * Reaches the KMS host through the configured proxy, then layers TLS for the KMS host on the resulting socket.
     */
    private static SSLSocket connectThroughProxy(final SSLSocketFactory sslSocketFactory,
            final ProxySettings proxySettings, final ServerAddress kmsAddress, final int soTimeoutMillis,
            final long connectTimeoutMillis) throws IOException {
        int connectTimeout = Math.toIntExact(connectTimeoutMillis);
        Socket proxySocket = proxySettings.getProtocol() == ProxyProtocol.SOCKS5
                ? connectSocksProxy(proxySettings, kmsAddress, soTimeoutMillis, connectTimeout)
                : connectHttpProxy(proxySettings, kmsAddress, soTimeoutMillis, connectTimeout);

        SSLSocket socket;
        try {
            // Layers TLS over the already-established tunnel. autoClose ensures that closing the returned socket also
            // closes the underlying proxy socket.
            socket = (SSLSocket) sslSocketFactory.createSocket(proxySocket, kmsAddress.getHost(), kmsAddress.getPort(), true);
        } catch (IOException | RuntimeException e) {
            closeSocket(proxySocket);
            throw e;
        }

        try {
            // Even though the proxy connection is already established, the TLS handshake has not been performed yet,
            // so SSL parameters can still be set. They target the KMS host, not the proxy.
            SSLParameters sslParameters = socket.getSSLParameters();
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

    /**
     * Opens a connection to an HTTP proxy and asks it to tunnel to the KMS host. When the protocol is
     * {@link ProxyProtocol#HTTPS} the connection to the proxy is itself protected with TLS, verified against the
     * proxy's own identity; the tunnel then carries a second, independent TLS session with the KMS host.
     */
    private static Socket connectHttpProxy(final ProxySettings proxySettings, final ServerAddress kmsAddress,
            final int soTimeoutMillis, final int connectTimeoutMillis) throws IOException {
        boolean useTls = proxySettings.getProtocol() == ProxyProtocol.HTTPS;
        String proxyHost = assertNotNull(proxySettings.getHost());
        int proxyPort = proxySettings.getPort();

        Socket proxySocket = useTls ? proxySslSocketFactory(proxySettings).createSocket() : new Socket();
        try {
            proxySocket.setSoTimeout(soTimeoutMillis);
            proxySocket.connect(new InetSocketAddress(proxyHost, proxyPort), connectTimeoutMillis);
            if (useTls) {
                SSLSocket proxyTlsSocket = (SSLSocket) proxySocket;
                SSLParameters sslParameters = proxyTlsSocket.getSSLParameters();
                SslHelper.enableSni(proxyHost, sslParameters);
                SslHelper.enableHostNameVerification(sslParameters);
                proxyTlsSocket.setSSLParameters(sslParameters);
                try {
                    proxyTlsSocket.startHandshake();
                } catch (SSLException e) {
                    // A plaintext proxy answering a TLS handshake produces an obscure JSSE error, so name the likely
                    // cause rather than letting it surface unexplained.
                    throw new MongoSocketException("TLS handshake with the " + ProxyProtocol.HTTPS + " proxy at "
                            + proxyHost + ":" + proxyPort + " failed. If the proxy does not use TLS, configure"
                            + " ProxySettings with " + ProxyProtocol.HTTP + " instead. Cause: " + e.getMessage(),
                            kmsAddress, e);
                }
            }
            HttpProxyTunnel.establishTunnel(proxySocket, kmsAddress, proxySettings);
        } catch (IOException | RuntimeException e) {
            closeSocket(proxySocket);
            throw e;
        }
        return proxySocket;
    }

    private static SSLSocketFactory proxySslSocketFactory(final ProxySettings proxySettings) {
        SSLContext proxySslContext = proxySettings.getSslContext();
        return proxySslContext == null
                ? (SSLSocketFactory) SSLSocketFactory.getDefault() : proxySslContext.getSocketFactory();
    }

    private static Socket connectSocksProxy(final ProxySettings proxySettings, final ServerAddress kmsAddress,
            final int soTimeoutMillis, final int connectTimeoutMillis) throws IOException {
        SocksSocket proxySocket = new SocksSocket(proxySettings);
        try {
            proxySocket.setSoTimeout(soTimeoutMillis);
            // Unresolved, so that the proxy resolves the KMS host rather than the client.
            proxySocket.connect(InetSocketAddress.createUnresolved(kmsAddress.getHost(), kmsAddress.getPort()),
                    connectTimeoutMillis);
        } catch (IOException | RuntimeException e) {
            closeSocket(proxySocket);
            throw e;
        }
        return proxySocket;
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
