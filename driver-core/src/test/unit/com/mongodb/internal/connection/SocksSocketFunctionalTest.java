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
import com.mongodb.connection.ProxySettings;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Verifies that SocksSocket surfaces each SOCKS5 protocol failure as a MongoSocksProxyException
 * and, for parsed non-success CONNECT replies, exposes the correct RFC 1928 reply code via
 * {@link MongoSocksProxyException#getProxyReplyCode()}. Uses a local mini-server; no real SOCKS5
 * proxy required.
 */
class SocksSocketFunctionalTest {

    private static final InetSocketAddress TARGET =
            InetSocketAddress.createUnresolved("mongo.example.com", 27017);

    private Exception connectWithMiniServer(final byte[] serverBytes, final boolean withCredentials)
            throws Exception {
        return connectWithMiniServer(serverBytes, withCredentials, false);
    }

    private Exception connectWithMiniServer(final byte[] serverBytes, final boolean withCredentials,
                                            final boolean eofAfterWrite)
            throws Exception {
        try (ServerSocket server = new ServerSocket(0)) {
            int port = server.getLocalPort();

            Thread t = new Thread(() -> {
                try (Socket client = server.accept()) {
                    OutputStream out = client.getOutputStream();
                    out.write(serverBytes);
                    out.flush();
                    if (eofAfterWrite) {
                        // Half-close: send TCP FIN so the client sees EOF (in.read() == -1) on its
                        // next read.
                        client.shutdownOutput();
                    }
                    // Drain anything the client writes (negotiation/auth/CONNECT bytes) until the
                    // client closes its end. This blocks the server thread so it does not tear
                    // down the socket while the client is still reading the canned bytes.
                    // Bounded by the client's natural close in the SocksSocket finally block.
                    // Plain read-loop (no transferTo/nullOutputStream) for Java 8 source compatibility.
                    InputStream in = client.getInputStream();
                    byte[] discard = new byte[1024];
                    //noinspection StatementWithEmptyBody
                    while (in.read(discard) != -1) {
                        // discard
                    }
                } catch (Exception ignored) {
                }
            });
            t.setDaemon(true);
            t.start();

            try (SocksSocket socksSocket = new SocksSocket(buildProxySettings("127.0.0.1", port, withCredentials))) {
                try {
                    socksSocket.connect(TARGET, 5000);
                    return null;
                } catch (MongoSocksProxyException e) {
                    return e;
                }
            } finally {
                try {
                    t.join(5000);
                } catch (InterruptedException ie) {
                    // Don't mask the primary exception (if any) with the join interruption;
                    // just preserve the thread's interrupt status and continue.
                    Thread.currentThread().interrupt();
                }
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
    void hostUnreachableCode4() throws Exception {
        byte[] bytes = {
                0x05, 0x00,                                 // negotiation OK, no auth
                0x05, 0x04, 0x00, 0x01, 0, 0, 0, 0, 0, 0   // HOST_UNREACHABLE
        };
        MongoSocksProxyException ex = assertProxy(connectWithMiniServer(bytes, false));
        Assertions.assertNotNull(ex);
        assertEquals(SocksSocket.ServerReply.HOST_UNREACHABLE.getReplyNumber(), ex.getProxyReplyCode());
    }

    @Test
    void connRefusedPhaseConnectRelayCode5() throws Exception {
        byte[] bytes = {
                0x05, 0x00,
                0x05, 0x05, 0x00, 0x01, 0, 0, 0, 0, 0, 0   // CONN_REFUSED
        };
        MongoSocksProxyException ex = assertProxy(connectWithMiniServer(bytes, false));
        Assertions.assertNotNull(ex);
        assertEquals(SocksSocket.ServerReply.CONN_REFUSED.getReplyNumber(), ex.getProxyReplyCode());
    }

    @Test
    void notAllowedPhaseConnectRelayCode2() throws Exception {
        byte[] bytes = {
                0x05, 0x00,
                0x05, 0x02, 0x00, 0x01, 0, 0, 0, 0, 0, 0   // NOT_ALLOWED
        };
        MongoSocksProxyException ex = assertProxy(connectWithMiniServer(bytes, false));
        Assertions.assertNotNull(ex);
        assertEquals(SocksSocket.ServerReply.NOT_ALLOWED.getReplyNumber(), ex.getProxyReplyCode());
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
        assertNull(ex.getProxyReplyCode());
    }

    // -----------------------------------------------------------------------
    // NEGOTIATION
    // -----------------------------------------------------------------------

    @Test
    void noAcceptableMethodNoReplyCode() throws Exception {
        byte[] bytes = {0x05, (byte) 0xFF};
        MongoSocksProxyException ex = assertProxy(connectWithMiniServer(bytes, false));
        Assertions.assertNotNull(ex);
        assertNull(ex.getProxyReplyCode());
    }

    @Test
    void wrongSocksVersionNoReplyCode() throws Exception {
        byte[] bytes = {0x04, 0x00};
        MongoSocksProxyException ex = assertProxy(connectWithMiniServer(bytes, false));
        Assertions.assertNotNull(ex);
        assertNull(ex.getProxyReplyCode());
    }

    // -----------------------------------------------------------------------
    // IOException-during-handshake → surfaces as MongoSocksProxyException with null replyCode
    // -----------------------------------------------------------------------

    @Test
    void ioFailureDuringNegotiationNoReplyCode() throws Exception {
        // Mini-server half-closes immediately after writing zero bytes of method-selection reply.
        // Client's readSocksReply sees EOF (in.read() == -1) and throws ConnectException("Malformed
        // reply..."). That IOException must be wrapped as MongoSocksProxyException with no reply
        // code (failure happened before any CONNECT reply was parsed).
        byte[] noReply = new byte[0];
        MongoSocksProxyException ex = assertProxy(connectWithMiniServer(noReply, false, true));
        Assertions.assertNotNull(ex);
        Assertions.assertTrue(ex.getMessage().contains("Malformed reply from SOCKS proxy server"));
        assertNull(ex.getProxyReplyCode());
    }

    @Test
    void unknownReplyCodeDuringConnectRelayNoReplyCode() throws Exception {
        // Reply code 0x09 is not a known RFC 1928 code — the parser rejects it before it can be
        // exposed via getProxyReplyCode().
        byte[] bytes = {
                0x05, 0x00,                                 // negotiation OK
                0x05, 0x09, 0x00, 0x01, 0, 0, 0, 0, 0, 0   // unknown reply code 0x09
        };
        MongoSocksProxyException ex = assertProxy(connectWithMiniServer(bytes, false));
        Assertions.assertNotNull(ex);
        assertNull(ex.getProxyReplyCode());
    }

    @Test
    void ioFailureDuringAuthenticationNoReplyCode() throws Exception {
        // Negotiation succeeds picking USERNAME_PASSWORD; mini-server then half-closes immediately,
        // so the client reads the 2 negotiation bytes successfully and then sees EOF on the
        // subsequent auth-result read. readSocksReply throws ConnectException("Malformed reply...")
        // from inside authenticate(). The wrapper must surface this as MongoSocksProxyException
        // with no reply code.
        byte[] bytes = {0x05, 0x02};   // negotiation OK, picked username/password; then EOF
        MongoSocksProxyException ex = assertProxy(connectWithMiniServer(bytes, true, true));
        Assertions.assertNotNull(ex);
        Assertions.assertTrue(ex.getMessage().contains("Malformed reply from SOCKS proxy server"));
        assertNull(ex.getProxyReplyCode());
    }
}
