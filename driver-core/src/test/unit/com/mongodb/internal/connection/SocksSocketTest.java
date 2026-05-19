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

package com.mongodb.internal.connection;

import com.mongodb.MongoSocksProxyException;
import com.mongodb.MongoSocksProxyException.HandshakePhase;
import com.mongodb.connection.ProxySettings;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Verifies that SocksSocket tags each SOCKS5 protocol failure with the correct HandshakePhase
 * and, for CONNECT_RELAY failures, the correct RFC 1928 reply code.
 * Uses a local mini-server; no real SOCKS5 proxy required.
 */
class SocksSocketTest {

    private static final InetSocketAddress TARGET =
            InetSocketAddress.createUnresolved("mongo.example.com", 27017);

    private Exception connectWithMiniServer(final byte[] serverBytes, final boolean withCredentials)
            throws Exception {
        try (ServerSocket server = new ServerSocket(0)) {
            int port = server.getLocalPort();

            Thread t = new Thread(() -> {
                try (Socket client = server.accept()) {
                    OutputStream out = client.getOutputStream();
                    out.write(serverBytes);
                    out.flush();
                    // Drain anything the client writes (negotiation/auth/CONNECT bytes) until the
                    // client closes its end. This blocks the server thread so it does not tear
                    // down the socket while the client is still reading the canned bytes.
                    // Bounded by the client's natural close in the SocksSocket finally block.
                    // Plain read-loop (no transferTo/nullOutputStream) for Java 8 source compatibility.
                    InputStream in = client.getInputStream();
                    byte[] discard = new byte[1024];
                    while (in.read(discard) != -1) {
                        // discard
                    }
                } catch (Exception ignored) {
                }
            });
            t.setDaemon(true);
            t.start();

            SocksSocket socksSocket = new SocksSocket(buildProxySettings("127.0.0.1", port, withCredentials));
            try {
                socksSocket.connect(TARGET, 5000);
                return null;
            } catch (MongoSocksProxyException | IOException e) {
                return e;
            } finally {
                try {
                    socksSocket.close();
                } catch (Exception ignored) {
                }
                t.join(5000);
            }
        }
    }

    private static ProxySettings buildProxySettings(final String host, final int port, final boolean withCredentials) {
        ProxySettings.Builder b = ProxySettings.builder().host(host).port(port);
        if (withCredentials) {
            b.username("user").password("pass");
        }
        return b.build();
    }

    private static MongoSocksProxyException assertProxy(final Exception ex) {
        return assertInstanceOf(MongoSocksProxyException.class, ex,
                "Expected MongoSocksProxyException but got: " + (ex == null ? "null" : ex.getClass().getName()));
    }

    // -----------------------------------------------------------------------
    // CONNECT_RELAY — RFC 1928 server reply codes
    // -----------------------------------------------------------------------

    @Test
    void hostUnreachablePhaseConnectRelayCode4() throws Exception {
        byte[] bytes = {
                0x05, 0x00,                                 // negotiation OK, no auth
                0x05, 0x04, 0x00, 0x01, 0, 0, 0, 0, 0, 0   // HOST_UNREACHABLE
        };
        MongoSocksProxyException ex = assertProxy(connectWithMiniServer(bytes, false));
        Assertions.assertNotNull(ex);
        assertEquals(HandshakePhase.CONNECT_RELAY, ex.getHandshakePhase());
        assertEquals(4, ex.getProxyReplyCode());
    }

    @Test
    void connRefusedPhaseConnectRelayCode5() throws Exception {
        byte[] bytes = {
                0x05, 0x00,
                0x05, 0x05, 0x00, 0x01, 0, 0, 0, 0, 0, 0   // CONN_REFUSED
        };
        MongoSocksProxyException ex = assertProxy(connectWithMiniServer(bytes, false));
        Assertions.assertNotNull(ex);
        assertEquals(HandshakePhase.CONNECT_RELAY, ex.getHandshakePhase());
        assertEquals(5, ex.getProxyReplyCode());
    }

    @Test
    void notAllowedPhaseConnectRelayCode2() throws Exception {
        byte[] bytes = {
                0x05, 0x00,
                0x05, 0x02, 0x00, 0x01, 0, 0, 0, 0, 0, 0   // NOT_ALLOWED
        };
        MongoSocksProxyException ex = assertProxy(connectWithMiniServer(bytes, false));
        Assertions.assertNotNull(ex);
        assertEquals(HandshakePhase.CONNECT_RELAY, ex.getHandshakePhase());
        assertEquals(2, ex.getProxyReplyCode());
    }

    // -----------------------------------------------------------------------
    // AUTHENTICATION
    // -----------------------------------------------------------------------

    @Test
    void authRejectedPhaseAuthenticationNoReplyCode() throws Exception {
        byte[] bytes = {
                0x05, 0x02,   // negotiation OK, needs username/password
                0x01, 0x01    // auth rejected
        };
        MongoSocksProxyException ex = assertProxy(connectWithMiniServer(bytes, true));
        Assertions.assertNotNull(ex);
        assertEquals(HandshakePhase.AUTHENTICATION, ex.getHandshakePhase());
        assertNull(ex.getProxyReplyCode());
    }

    // -----------------------------------------------------------------------
    // NEGOTIATION
    // -----------------------------------------------------------------------

    @Test
    void noAcceptableMethodPhaseNegotiationNoReplyCode() throws Exception {
        byte[] bytes = {0x05, (byte) 0xFF};
        MongoSocksProxyException ex = assertProxy(connectWithMiniServer(bytes, false));
        Assertions.assertNotNull(ex);
        assertEquals(HandshakePhase.NEGOTIATION, ex.getHandshakePhase());
        assertNull(ex.getProxyReplyCode());
    }

    @Test
    void wrongSocksVersionPhaseNegotiationNoReplyCode() throws Exception {
        byte[] bytes = {0x04, 0x00};
        MongoSocksProxyException ex = assertProxy(connectWithMiniServer(bytes, false));
        Assertions.assertNotNull(ex);
        assertEquals(HandshakePhase.NEGOTIATION, ex.getHandshakePhase());
        assertNull(ex.getProxyReplyCode());
    }

    // -----------------------------------------------------------------------
    // IOException-during-handshake → tagged with the proper phase, not PROXY_TCP_CONNECT
    // -----------------------------------------------------------------------

    @Test
    void ioFailureDuringNegotiationTaggedAsNegotiation() throws Exception {
        // The mini-server's drain loop keeps the connection open while writing no method-selection
        // reply, so the client's readSocksReply blocks until the 5s socket timeout fires —
        // surfacing as a SocketTimeoutException inside performNegotiation. That IOException must
        // be wrapped as MongoSocksProxyException with phase=NEGOTIATION, not PROXY_TCP_CONNECT.
        byte[] noReply = new byte[0];
        MongoSocksProxyException ex = assertProxy(connectWithMiniServer(noReply, false));
        Assertions.assertNotNull(ex);
        assertEquals(HandshakePhase.NEGOTIATION, ex.getHandshakePhase());
        assertNull(ex.getProxyReplyCode());
    }

    @Test
    void unknownReplyCodeDuringConnectRelayTaggedAsConnectRelay() throws Exception {
        // Reply code 0x09 is not a known RFC 1928 code. ServerReply.of throws ConnectException
        // before the line that produces MongoSocksProxyException for known reply codes.
        // The fix must still tag this as CONNECT_RELAY (the phase we were in).
        byte[] bytes = {
                0x05, 0x00,                                 // negotiation OK
                0x05, 0x09, 0x00, 0x01, 0, 0, 0, 0, 0, 0   // unknown reply code 0x09
        };
        MongoSocksProxyException ex = assertProxy(connectWithMiniServer(bytes, false));
        Assertions.assertNotNull(ex);
        assertEquals(HandshakePhase.CONNECT_RELAY, ex.getHandshakePhase());
        assertNull(ex.getProxyReplyCode());
    }

    @Test
    void ioFailureDuringAuthenticationTaggedAsAuthentication() throws Exception {
        // Negotiation succeeds picking USERNAME_PASSWORD; the mini-server then writes nothing
        // further and the drain loop keeps the connection open, so the client's auth-read blocks
        // until SocketTimeoutException. The wrapper must tag the IOException as AUTHENTICATION.
        byte[] bytes = {0x05, 0x02};   // negotiation OK, picked username/password; then nothing
        MongoSocksProxyException ex = assertProxy(connectWithMiniServer(bytes, true));
        Assertions.assertNotNull(ex);
        assertEquals(HandshakePhase.AUTHENTICATION, ex.getHandshakePhase());
        assertNull(ex.getProxyReplyCode());
    }

    // -----------------------------------------------------------------------
    // PROXY_TCP_CONNECT — inferred at SocketStream boundary, not tagged here
    // -----------------------------------------------------------------------

    @Test
    void tcpConnectFailureNotMongoSocksProxyException() throws IOException {
        // Bind an ephemeral port then release it, so we have a port that is reliably closed
        // for the duration of this test. Using a hard-coded low port (e.g. 1) is unreliable
        // because some systems have services listening there.
        int closedPort;
        try (ServerSocket probe = new ServerSocket(0)) {
            closedPort = probe.getLocalPort();
        }
        try (SocksSocket s = new SocksSocket(buildProxySettings("127.0.0.1", closedPort, false))) {
            Throwable ex = assertThrows(Throwable.class, () -> s.connect(TARGET, 5000));
            assertFalse(ex instanceof MongoSocksProxyException, "TCP connect failure is tagged as PROXY_TCP_CONNECT at SocketStream, not here");
        }
    }

    @Test
    void constructorRejectsNullHandshakePhase() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new MongoSocksProxyException("m", new com.mongodb.ServerAddress(), null));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new MongoSocksProxyException("m", new com.mongodb.ServerAddress(),
                        new RuntimeException("c"), null));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new MongoSocksProxyException("m", new com.mongodb.ServerAddress(), null, 5));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new MongoSocksProxyException("m", new com.mongodb.ServerAddress(),
                        new RuntimeException("c"), null, 5));
    }
}
