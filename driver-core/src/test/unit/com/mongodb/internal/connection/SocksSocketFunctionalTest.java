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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that SocksSocket surfaces each SOCKS5 protocol failure as a MongoSocksProxyException
 * and, for parsed non-success CONNECT replies, exposes the correct RFC 1928 reply code via
 * {@link MongoSocksProxyException#getProxyReplyCode()}. Uses a local mini-server; no real SOCKS5
 * proxy required.
 */
class SocksSocketFunctionalTest {

    private static final InetSocketAddress TARGET =
            InetSocketAddress.createUnresolved("mongo.example.com", 27017);
    private static final int CONNECT_TIMEOUT_MS = 5000;

    private void connectWithMiniServer(final byte[] serverBytes, final boolean withCredentials)
            throws Exception {
        connectWithMiniServer(serverBytes, withCredentials, false);
    }

    private void connectWithMiniServer(final byte[] serverBytes, final boolean withCredentials,
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
                        // Half-close: send TCP FIN so the client sees EOF on its next read.
                        client.shutdownOutput();
                    }
                    // Drain anything the client writes until it closes its end, so the server thread
                    // does not tear down the socket while the client is still reading canned bytes.
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
                socksSocket.connect(TARGET, CONNECT_TIMEOUT_MS);
            } finally {
                try {
                    t.join(CONNECT_TIMEOUT_MS);
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

    // -----------------------------------------------------------------------
    // CONNECT relay — RFC 1928 server reply codes
    // -----------------------------------------------------------------------

    static Stream<SocksSocket.ServerReply> connectRelayReplyCodes() {
        return Stream.of(
                SocksSocket.ServerReply.GENERAL_FAILURE,
                SocksSocket.ServerReply.NOT_ALLOWED,
                SocksSocket.ServerReply.NET_UNREACHABLE,
                SocksSocket.ServerReply.HOST_UNREACHABLE,
                SocksSocket.ServerReply.CONN_REFUSED,
                SocksSocket.ServerReply.TTL_EXPIRED,
                SocksSocket.ServerReply.CMD_NOT_SUPPORTED,
                SocksSocket.ServerReply.ADDR_TYPE_NOT_SUP
        );
    }

    @ParameterizedTest
    @MethodSource
    void connectRelayReplyCodes(final SocksSocket.ServerReply reply) {
        byte[] bytes = {
                0x05, 0x00,                                                        // negotiation OK, no auth
                0x05, (byte) reply.getReplyNumber(), 0x00, 0x01, 0, 0, 0, 0, 0, 0  // CONNECT reply
        };
        MongoSocksProxyException ex = assertThrows(MongoSocksProxyException.class,
                () -> connectWithMiniServer(bytes, false));
        assertEquals(reply.getReplyNumber(), ex.getProxyReplyCode());
    }

    // -----------------------------------------------------------------------
    // Authentication
    // -----------------------------------------------------------------------

    @Test
    void authRejectedNoReplyCode() {
        byte[] bytes = {
                0x05, 0x02,   // negotiation OK, needs username/password
                0x01, 0x01    // auth rejected
        };
        MongoSocksProxyException ex = assertThrows(MongoSocksProxyException.class,
                () -> connectWithMiniServer(bytes, true));
        assertNull(ex.getProxyReplyCode());
    }

    // -----------------------------------------------------------------------
    // Negotiation
    // -----------------------------------------------------------------------

    @Test
    void noAcceptableMethodNoReplyCode() {
        byte[] bytes = {0x05, (byte) 0xFF};
        MongoSocksProxyException ex = assertThrows(MongoSocksProxyException.class,
                () -> connectWithMiniServer(bytes, false));
        assertNull(ex.getProxyReplyCode());
    }

    @Test
    void wrongSocksVersionNoReplyCode() {
        byte[] bytes = {0x04, 0x00};
        MongoSocksProxyException ex = assertThrows(MongoSocksProxyException.class,
                () -> connectWithMiniServer(bytes, false));
        assertNull(ex.getProxyReplyCode());
    }

    // -----------------------------------------------------------------------
    // IO failure mid-handshake → surfaces as MongoSocksProxyException with null replyCode
    // -----------------------------------------------------------------------

    @Test
    void ioFailureDuringNegotiationNoReplyCode() {
        // Mini-server half-closes before sending any method-selection bytes, so the client sees
        // EOF while reading the negotiation reply.
        byte[] noReply = new byte[0];
        MongoSocksProxyException ex = assertThrows(MongoSocksProxyException.class,
                () -> connectWithMiniServer(noReply, false, true));
        assertTrue(ex.getMessage().contains("Malformed reply from SOCKS proxy server"));
        assertNull(ex.getProxyReplyCode());
    }

    @Test
    void unknownReplyCodeDuringConnectRelayNoReplyCode() {
        byte[] bytes = {
                0x05, 0x00,                                 // negotiation OK
                0x05, 0x09, 0x00, 0x01, 0, 0, 0, 0, 0, 0   // reply code 0x09 is not a known RFC 1928 code
        };
        MongoSocksProxyException ex = assertThrows(MongoSocksProxyException.class,
                () -> connectWithMiniServer(bytes, false));
        assertNull(ex.getProxyReplyCode());
    }

    @Test
    void ioFailureDuringAuthenticationNoReplyCode() {
        byte[] bytes = {0x05, 0x02};
        MongoSocksProxyException ex = assertThrows(MongoSocksProxyException.class,
                () -> connectWithMiniServer(bytes, true, true));
        assertTrue(ex.getMessage().contains("Malformed reply from SOCKS proxy server"));
        assertNull(ex.getProxyReplyCode());
    }
}
