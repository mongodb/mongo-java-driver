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
import com.mongodb.lang.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that the driver reaches a KMS host through a proxy and then negotiates TLS end-to-end with the KMS host over
 * the resulting tunnel.
 *
 * <p>Everything this test needs runs in-process: a minimal {@code HTTP CONNECT} proxy, a TLS server standing in for the
 * KMS host, and a keystore loaded from test resources. There is no dependency on a network, on credentials, or on any
 * external process, so it covers the tunneling behaviour that
 * {@code AbstractClientSideEncryptionKmsProxyProseTest} can only cover where real AWS credentials are available.</p>
 */
final class KmsSocketConnectorTunnelTest {

    private static final String KEYSTORE_RESOURCE = "/kms-tunnel-test.p12";
    private static final char[] KEYSTORE_PASSWORD = "changeit".toCharArray();

    /** The certificate in the keystore is issued for the IP address 127.0.0.1 and for no other name. */
    private static final String CERTIFIED_HOST = "127.0.0.1";

    private static final int TIMEOUT_MILLIS = 10_000;

    private final List<Closeable> toClose = new ArrayList<>();
    private final List<Thread> threads = new ArrayList<>();

    private FakeKmsServer kmsServer;

    @BeforeEach
    void setUp() throws Exception {
        kmsServer = new FakeKmsServer();
    }

    @AfterEach
    void tearDown() throws Exception {
        for (Closeable closeable : toClose) {
            try {
                closeable.close();
            } catch (IOException e) {
                // ignore
            }
        }
        for (Thread thread : threads) {
            thread.join(5_000);
        }
    }

    @Test
    void shouldConnectDirectlyWhenNoProxyIsConfigured() throws Exception {
        SSLSocket socket = connect(ProxySettings.builder().build());

        assertEquals("PONG", exchange(socket, "PING"));
        assertEquals("PING", kmsServer.received());
    }

    @Test
    void shouldTunnelThroughHttpProxy() throws Exception {
        ConnectProxy proxy = new ConnectProxy(false);

        SSLSocket socket = connect(proxy.settings(ProxyProtocol.HTTP).build());

        assertEquals("PONG", exchange(socket, "PING"));
        assertEquals("PING", kmsServer.received(), "the KMS host must receive what was written to the tunneled socket");
        assertEquals(1, proxy.connectCount(), "the connection must have been made through the proxy");
        assertNull(proxy.proxyAuthorization(), "no Proxy-Authorization header is expected without credentials");
    }

    @Test
    void shouldTunnelThroughHttpsProxy() throws Exception {
        // Two nested TLS sessions: the driver's session with the proxy, and its session with the KMS host carried
        // inside the tunnel.
        ConnectProxy proxy = new ConnectProxy(true);

        SSLSocket socket = connect(proxy.settings(ProxyProtocol.HTTPS)
                .sslContext(clientSslContext())
                .build());

        assertEquals("PONG", exchange(socket, "PING"));
        assertEquals("PING", kmsServer.received());
        assertEquals(1, proxy.connectCount());
    }

    @Test
    void shouldSendBasicProxyAuthorizationWhenCredentialsAreConfigured() throws Exception {
        ConnectProxy proxy = new ConnectProxy(false);

        SSLSocket socket = connect(proxy.settings(ProxyProtocol.HTTP)
                .username("user")
                .password("pass")
                .build());

        assertEquals("PONG", exchange(socket, "PING"));
        // "user:pass" base64-encoded
        assertEquals("Basic dXNlcjpwYXNz", proxy.proxyAuthorization());
    }

    @Test
    void shouldAcceptAStatusLineWithAnyHttpVersion() throws Exception {
        // Proxies differ in the HTTP version they reply with, so the status code alone must decide.
        ConnectProxy proxy = new ConnectProxy(false);
        proxy.respondWith("HTTP/1.0 200 Connection established");

        SSLSocket socket = connect(proxy.settings(ProxyProtocol.HTTP).build());

        assertEquals("PONG", exchange(socket, "PING"));
    }

    @Test
    void shouldVerifyCertificateAgainstKmsHostRatherThanProxy() throws Exception {
        ConnectProxy proxy = new ConnectProxy(false);

        // "localhost" resolves to the same server, but the KMS host's certificate is issued for the IP address only.
        // The handshake must therefore fail, proving that hostname verification is performed against the KMS address
        // rather than against the proxy the socket is actually connected to.
        ServerAddress unverifiableAddress = new ServerAddress("localhost", kmsServer.address().getPort());

        assertThrows(SSLHandshakeException.class, () -> KmsSocketConnector.connect(clientSslContext(),
                proxy.settings(ProxyProtocol.HTTP).build(), unverifiableAddress, TIMEOUT_MILLIS, TIMEOUT_MILLIS));
    }

    @Test
    void shouldFailWhenProxyRefusesToTunnel() throws Exception {
        ConnectProxy proxy = new ConnectProxy(false);
        proxy.respondWith("HTTP/1.1 403 Forbidden");

        MongoSocketException e = assertThrows(MongoSocketException.class,
                () -> connect(proxy.settings(ProxyProtocol.HTTP).build()));
        assertTrue(e.getMessage().contains("403"), () -> "unexpected failure: " + e.getMessage());
    }

    private SSLSocket connect(final ProxySettings proxySettings) throws Exception {
        return KmsSocketConnector.connect(clientSslContext(), proxySettings, kmsServer.address(),
                TIMEOUT_MILLIS, TIMEOUT_MILLIS);
    }

    private String exchange(final SSLSocket socket, final String request) throws IOException {
        try (SSLSocket tls = socket) {
            tls.getOutputStream().write(request.getBytes(StandardCharsets.UTF_8));
            tls.getOutputStream().flush();
            byte[] response = new byte[16];
            int read = tls.getInputStream().read(response);
            return new String(response, 0, read, StandardCharsets.UTF_8);
        }
    }

    // --- in-process HTTP CONNECT proxy -------------------------------------------------------------------------

    private final class ConnectProxy {
        private final ServerSocket serverSocket;
        private volatile String responseStatusLine = "HTTP/1.1 200 Connection Established";
        private volatile int connectCount;
        private final AtomicReference<String> proxyAuthorization = new AtomicReference<>();

        ConnectProxy(final boolean useTls) throws Exception {
            this.serverSocket = useTls
                    ? serverSslContext().getServerSocketFactory().createServerSocket(0, 1, loopback())
                    : new ServerSocket(0, 1, loopback());
            toClose.add(serverSocket);
            start(this::acceptLoop, "kms-connect-proxy");
        }

        ProxySettings.Builder settings(final ProxyProtocol protocol) {
            return ProxySettings.builder()
                    .protocol(protocol)
                    .host(CERTIFIED_HOST)
                    .port(serverSocket.getLocalPort());
        }

        void respondWith(final String statusLine) {
            this.responseStatusLine = statusLine;
        }

        int connectCount() {
            return connectCount;
        }

        @Nullable
        String proxyAuthorization() {
            return proxyAuthorization.get();
        }

        private void acceptLoop() {
            while (!serverSocket.isClosed()) {
                try {
                    Socket client = serverSocket.accept();
                    toClose.add(client);
                    handle(client);
                } catch (IOException e) {
                    return;
                }
            }
        }

        private void handle(final Socket client) throws IOException {
            List<String> headers = readHeaders(client.getInputStream());
            String requestLine = headers.get(0);
            for (String header : headers) {
                if (header.regionMatches(true, 0, "Proxy-Authorization:", 0, "Proxy-Authorization:".length())) {
                    proxyAuthorization.set(header.substring("Proxy-Authorization:".length()).trim());
                }
            }
            if (!requestLine.startsWith("CONNECT ") || !responseStatusLine.contains(" 2")) {
                client.getOutputStream().write((responseStatusLine + "\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
                client.getOutputStream().flush();
                return;
            }
            String[] hostAndPort = requestLine.split(" ")[1].split(":");
            Socket upstream = new Socket();
            upstream.connect(new InetSocketAddress(hostAndPort[0], Integer.parseInt(hostAndPort[1])),
                    TIMEOUT_MILLIS);
            toClose.add(upstream);
            connectCount++;
            client.getOutputStream().write((responseStatusLine + "\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
            client.getOutputStream().flush();
            // From here the proxy is a blind pipe, which is what makes end-to-end TLS with the KMS host possible.
            start(() -> pipe(client, upstream), "kms-proxy-pipe-out");
            start(() -> pipe(upstream, client), "kms-proxy-pipe-in");
        }

        private List<String> readHeaders(final InputStream inputStream) throws IOException {
            StringBuilder raw = new StringBuilder();
            while (!raw.toString().endsWith("\r\n\r\n")) {
                int b = inputStream.read();
                if (b == -1) {
                    throw new IOException("client closed before completing the request: " + raw);
                }
                raw.append((char) b);
            }
            List<String> headers = new ArrayList<>();
            for (String line : raw.toString().split("\r\n")) {
                if (!line.isEmpty()) {
                    headers.add(line);
                }
            }
            return headers;
        }

        private void pipe(final Socket from, final Socket to) {
            byte[] buffer = new byte[4096];
            try {
                InputStream in = from.getInputStream();
                OutputStream out = to.getOutputStream();
                int read;
                while ((read = in.read(buffer)) != -1) {
                    out.write(buffer, 0, read);
                    out.flush();
                }
            } catch (IOException e) {
                // the test is finished with this connection
            }
        }
    }

    // --- in-process stand-in for the KMS host -----------------------------------------------------------------

    private final class FakeKmsServer {
        private final SSLServerSocket serverSocket;
        private final AtomicReference<String> received = new AtomicReference<>();

        FakeKmsServer() throws Exception {
            serverSocket = (SSLServerSocket) serverSslContext().getServerSocketFactory()
                    .createServerSocket(0, 1, loopback());
            toClose.add(serverSocket);
            start(this::acceptLoop, "fake-kms-server");
        }

        ServerAddress address() {
            return new ServerAddress(CERTIFIED_HOST, serverSocket.getLocalPort());
        }

        String received() {
            return received.get();
        }

        private void acceptLoop() {
            while (!serverSocket.isClosed()) {
                try (SSLSocket accepted = (SSLSocket) serverSocket.accept()) {
                    byte[] buffer = new byte[16];
                    int read = accepted.getInputStream().read(buffer);
                    if (read > 0) {
                        received.set(new String(buffer, 0, read, StandardCharsets.UTF_8));
                        accepted.getOutputStream().write("PONG".getBytes(StandardCharsets.UTF_8));
                        accepted.getOutputStream().flush();
                    }
                } catch (IOException e) {
                    return;
                }
            }
        }
    }

    // --- shared helpers ---------------------------------------------------------------------------------------

    private void start(final Runnable body, final String name) {
        Thread thread = new Thread(body, name);
        thread.setDaemon(true);
        threads.add(thread);
        thread.start();
    }

    private static InetAddress loopback() {
        return InetAddress.getLoopbackAddress();
    }

    private static KeyStore keyStore() throws IOException, GeneralSecurityException {
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream in = KmsSocketConnectorTunnelTest.class.getResourceAsStream(KEYSTORE_RESOURCE)) {
            assertNotNull(in, KEYSTORE_RESOURCE + " is missing from the test resources");
            keyStore.load(in, KEYSTORE_PASSWORD);
        }
        return keyStore;
    }

    private static SSLContext serverSslContext() throws IOException, GeneralSecurityException {
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore(), KEYSTORE_PASSWORD);
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
        return sslContext;
    }

    private static SSLContext clientSslContext() throws IOException, GeneralSecurityException {
        TrustManagerFactory trustManagerFactory =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(keyStore());
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, trustManagerFactory.getTrustManagers(), null);
        return sslContext;
    }
}
