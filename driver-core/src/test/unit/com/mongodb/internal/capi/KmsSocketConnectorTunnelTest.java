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

import com.mongodb.KmsConnectCallback;
import com.mongodb.KmsConnectContext;
import com.mongodb.ServerAddress;
import com.mongodb.lang.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that the driver layers TLS for the KMS host on the socket a {@link KmsConnectCallback} returns, including
 * when that socket is tunneled to an intermediary.
 *
 * <p>The callback here performs the {@code HTTP CONNECT} exchange, which is what a user of the API is expected to
 * implement; the driver contributes only the TLS half.</p>
 *
 * <p>Everything this test needs runs in-process: a minimal {@code HTTP CONNECT} proxy, a TLS server standing in for the
 * KMS host, and a keystore loaded from test resources. There is no dependency on a network, on credentials, or on any
 * external process, so it covers the tunneling behaviour that
 * {@code AbstractClientSideEncryptionKmsConnectCallbackProseTest} can only cover where real AWS credentials are available.</p>
 */
final class KmsSocketConnectorTunnelTest {

    private static final char[] KEYSTORE_PASSWORD = "changeit".toCharArray();

    /**
     * Generated once per run rather than committed, so that no private key lives in the repository and nothing can
     * expire. The certificate is issued for the IP address 127.0.0.1 and for no other name, which
     * {@link #shouldVerifyCertificateAgainstKmsHostRatherThanProxy()} relies on.
     */
    private static Path keyStoreDir;
    private static Path keyStorePath;

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
    void shouldConnectDirectlyWhenNoCallbackIsConfigured() throws Exception {
        SSLSocket socket = connect(null);

        assertEquals("PONG", exchange(socket, "PING"));
        assertEquals("PING", kmsServer.received());
    }

    @Test
    void shouldLayerTlsOnASocketFromTheCallback() throws Exception {
        ConnectProxy proxy = new ConnectProxy(false);

        SSLSocket socket = connect(proxy.callback(false));

        assertEquals("PONG", exchange(socket, "PING"));
        assertEquals("PING", kmsServer.received(), "the KMS host must receive what was written to the tunneled socket");
        assertEquals(1, proxy.connectCount(), "the connection must have been made through the callback's proxy");
    }

    @Test
    void shouldLayerTlsOnATlsSocketFromTheCallback() throws Exception {
        // The callback returns an SSLSocket, so two independent TLS sessions are in play: the callback's own session
        // with the intermediary, and the driver's session with the KMS host carried inside it.
        ConnectProxy proxy = new ConnectProxy(true);

        SSLSocket socket = connect(proxy.callback(true));

        assertEquals("PONG", exchange(socket, "PING"));
        assertEquals("PING", kmsServer.received());
        assertEquals(1, proxy.connectCount());
    }

    @Test
    void shouldPassTheProviderAddressAndTimeoutToTheCallback() throws Exception {
        ConnectProxy proxy = new ConnectProxy(false);
        AtomicReference<KmsConnectContext> observed = new AtomicReference<>();
        KmsConnectCallback recording = context -> {
            observed.set(context);
            return proxy.callback(false).connect(context);
        };

        exchange(KmsSocketConnector.connect(clientSslContext(), recording, "aws:myname", kmsServer.address(),
                TIMEOUT_MILLIS, TIMEOUT_MILLIS), "PING");

        KmsConnectContext context = observed.get();
        assertNotNull(context);
        assertEquals("aws:myname", context.getKmsProvider());
        assertEquals(kmsServer.address(), context.getServerAddress(),
                "the callback must receive the KMS host address, not the address it connects to");
        assertEquals(TIMEOUT_MILLIS, context.getTimeoutMillis());
    }

    @Test
    void shouldVerifyCertificateAgainstKmsHostRatherThanTheAddressTheCallbackConnectedTo() throws Exception {
        ConnectProxy proxy = new ConnectProxy(false);

        // "localhost" resolves to the same server, but the KMS host's certificate is issued for the IP address only.
        // The handshake must therefore fail, proving that hostname verification is performed against the address the
        // driver was given rather than against whatever the callback connected to.
        ServerAddress unverifiableAddress = new ServerAddress("localhost", kmsServer.address().getPort());

        assertThrows(SSLHandshakeException.class, () -> KmsSocketConnector.connect(clientSslContext(),
                proxy.callback(false), "aws", unverifiableAddress, TIMEOUT_MILLIS, TIMEOUT_MILLIS));
    }

    @Test
    void shouldPropagateAnIOExceptionFromTheCallback() {
        IOException failure = new IOException("proxy refused CONNECT");

        IOException thrown = assertThrows(IOException.class, () -> connect(context -> {
            throw failure;
        }));

        assertSame(failure, thrown, "an IOException from the callback must be propagated unchanged");
    }

    @Test
    void shouldCloseTheCallbackSocketWhenTheHandshakeFails() throws Exception {
        // A server that accepts and immediately closes, so the KMS handshake over it fails promptly.
        ServerSocket unresponsive = new ServerSocket(0, 1, loopback());
        toClose.add(unresponsive);
        start(() -> {
            try (Socket accepted = unresponsive.accept()) {
                assertNotNull(accepted);
            } catch (IOException e) {
                // the test is finished with this connection
            }
        }, "unresponsive-server");

        AtomicReference<Socket> callbackSocket = new AtomicReference<>();
        KmsConnectCallback notTunneling = context -> {
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress(unresponsive.getInetAddress(), unresponsive.getLocalPort()),
                    TIMEOUT_MILLIS);
            socket.setSoTimeout(TIMEOUT_MILLIS);
            callbackSocket.set(socket);
            return socket;
        };

        assertThrows(IOException.class, () -> connect(notTunneling));

        assertTrue(callbackSocket.get().isClosed(),
                "the socket returned by the callback must be closed when the driver fails to establish TLS over it");
    }

    private SSLSocket connect(@Nullable final KmsConnectCallback callback) throws Exception {
        return KmsSocketConnector.connect(clientSslContext(), callback, "aws", kmsServer.address(),
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

        ConnectProxy(final boolean useTls) throws Exception {
            this.serverSocket = useTls
                    ? serverSslContext().getServerSocketFactory().createServerSocket(0, 1, loopback())
                    : new ServerSocket(0, 1, loopback());
            toClose.add(serverSocket);
            start(this::acceptLoop, "kms-connect-proxy");
        }

        /**
         * Returns a callback that tunnels to the KMS host through this proxy using {@code HTTP CONNECT}, which is what
         * a user of {@link KmsConnectCallback} is expected to implement.
         */
        KmsConnectCallback callback(final boolean useTls) {
            return context -> {
                Socket socket;
                try {
                    socket = useTls ? clientSslContext().getSocketFactory().createSocket() : new Socket();
                } catch (GeneralSecurityException e) {
                    throw new IOException("could not create the callback's socket", e);
                }
                try {
                    socket.connect(new InetSocketAddress(CERTIFIED_HOST, serverSocket.getLocalPort()), TIMEOUT_MILLIS);
                    socket.setSoTimeout(TIMEOUT_MILLIS);
                    ServerAddress target = context.getServerAddress();
                    String hostPort = target.getHost() + ":" + target.getPort();
                    socket.getOutputStream().write(("CONNECT " + hostPort + " HTTP/1.1\r\nHost: " + hostPort
                            + "\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
                    socket.getOutputStream().flush();
                    // One byte at a time, so that no byte of the driver's TLS handshake is consumed.
                    StringBuilder headers = new StringBuilder();
                    while (!headers.toString().endsWith("\r\n\r\n")) {
                        int b = socket.getInputStream().read();
                        if (b == -1) {
                            throw new IOException("proxy closed the connection: " + headers);
                        }
                        headers.append((char) b);
                    }
                    String statusLine = headers.substring(0, headers.indexOf("\r\n"));
                    if (!statusLine.contains(" 2")) {
                        throw new IOException("proxy refused the CONNECT request: " + statusLine);
                    }
                    return socket;
                } catch (IOException | RuntimeException e) {
                    socket.close();
                    throw e;
                }
            };
        }

        void respondWith(final String statusLine) {
            this.responseStatusLine = statusLine;
        }

        int connectCount() {
            return connectCount;
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

    @BeforeAll
    static void generateKeyStore() throws IOException, InterruptedException {
        keyStoreDir = Files.createTempDirectory("kms-tunnel-test");
        keyStorePath = keyStoreDir.resolve("kms-tunnel-test.p12");
        String executable = System.getProperty("os.name").toLowerCase(Locale.ROOT).startsWith("windows")
                ? "keytool.exe" : "keytool";
        Path keytool = Paths.get(System.getProperty("java.home"), "bin", executable);

        Process process = new ProcessBuilder(keytool.toString(),
                "-genkeypair", "-alias", "kms", "-keyalg", "RSA", "-keysize", "2048", "-validity", "1",
                "-storetype", "PKCS12", "-keystore", keyStorePath.toString(),
                "-storepass", new String(KEYSTORE_PASSWORD), "-keypass", new String(KEYSTORE_PASSWORD),
                "-dname", "CN=127.0.0.1", "-ext", "SAN=IP:127.0.0.1")
                .redirectErrorStream(true)
                .start();
        String output;
        try (InputStream in = process.getInputStream()) {
            ByteArrayOutputStream captured = new ByteArrayOutputStream();
            byte[] buffer = new byte[512];
            int read;
            while ((read = in.read(buffer)) != -1) {
                captured.write(buffer, 0, read);
            }
            output = new String(captured.toByteArray(), StandardCharsets.UTF_8);
        }
        assertTrue(process.waitFor(60, TimeUnit.SECONDS), "keytool did not finish in time");
        assertEquals(0, process.exitValue(), () -> "keytool failed: " + output);
    }

    @AfterAll
    static void deleteKeyStore() throws IOException {
        Files.deleteIfExists(keyStorePath);
        Files.deleteIfExists(keyStoreDir);
    }

    private static KeyStore keyStore() throws IOException, GeneralSecurityException {
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(keyStorePath)) {
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
