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

import com.mongodb.MongoSocketException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ProxySettings;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static com.mongodb.assertions.Assertions.assertNotNull;

/**
 * Establishes a tunnel to a target host through an HTTP proxy using the {@code CONNECT} method, as described by
 * <a href="https://www.rfc-editor.org/rfc/rfc9110#name-connect">RFC 9110</a>.
 *
 * <p>Once the tunnel is established the proxy relays bytes without interpreting them, so TLS can be negotiated
 * end-to-end with the target host over the same socket.</p>
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class HttpProxyTunnel {

    /**
     * Bounds the response so that a proxy which never sends the end of the header block cannot cause an unbounded read.
     */
    private static final int MAX_RESPONSE_BYTES = 8192;

    private static final String CRLF = "\r\n";

    /**
     * Sends a {@code CONNECT} request for {@code target} over {@code socket} and consumes the response.
     *
     * <p>The target is sent as a host name, so that the proxy resolves it. This matters when the client cannot resolve
     * the target itself, which is common in the networks where a proxy is mandatory.</p>
     *
     * <p>On return, the socket carries a transparent tunnel to {@code target} and TLS may be negotiated over it.</p>
     *
     * @param socket a connected socket to the proxy
     * @param target the host the tunnel should reach
     * @param proxySettings the proxy settings, used for optional {@code Basic} authentication
     * @throws IOException if the request cannot be written, the response cannot be read, or the proxy declines
     */
    public static void establishTunnel(final Socket socket, final ServerAddress target,
            final ProxySettings proxySettings) throws IOException {
        writeConnectRequest(socket.getOutputStream(), target, proxySettings);
        String statusLine = readResponse(socket.getInputStream(), target);
        int statusCode = parseStatusCode(statusLine, target);
        if (statusCode / 100 != 2) {
            throw new MongoSocketException("HTTP proxy " + proxySettings.getHost() + ":" + proxySettings.getPort()
                    + " refused to establish a tunnel to " + target + ": " + statusLine, target);
        }
    }

    private static void writeConnectRequest(final OutputStream outputStream, final ServerAddress target,
            final ProxySettings proxySettings) throws IOException {
        String hostPort = target.getHost() + ":" + target.getPort();
        StringBuilder request = new StringBuilder()
                .append("CONNECT ").append(hostPort).append(" HTTP/1.1").append(CRLF)
                .append("Host: ").append(hostPort).append(CRLF);
        String username = proxySettings.getUsername();
        if (username != null) {
            String credentials = username + ":" + assertNotNull(proxySettings.getPassword());
            String encoded = Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
            request.append("Proxy-Authorization: Basic ").append(encoded).append(CRLF);
        }
        request.append(CRLF);
        outputStream.write(request.toString().getBytes(StandardCharsets.US_ASCII));
        outputStream.flush();
    }

    /**
     * Reads the response one byte at a time, stopping at the end of the header block. Buffering would risk consuming
     * bytes of the TLS handshake that is subsequently performed over this same socket.
     *
     * @return the status line
     */
    private static String readResponse(final InputStream inputStream, final ServerAddress target) throws IOException {
        StringBuilder response = new StringBuilder();
        while (!endsWithBlankLine(response)) {
            int b = inputStream.read();
            if (b == -1) {
                throw new MongoSocketException(
                        "HTTP proxy closed the connection before completing the tunnel to " + target, target);
            }
            response.append((char) b);
            if (response.length() > MAX_RESPONSE_BYTES) {
                throw new MongoSocketException("HTTP proxy response exceeded " + MAX_RESPONSE_BYTES
                        + " bytes while establishing a tunnel to " + target, target);
            }
        }
        int endOfStatusLine = response.indexOf(CRLF);
        return response.substring(0, endOfStatusLine);
    }

    /**
     * Extracts the status code, tolerating any HTTP version, as proxies differ in the version they reply with.
     */
    private static int parseStatusCode(final String statusLine, final ServerAddress target) throws IOException {
        int firstSpace = statusLine.indexOf(' ');
        if (firstSpace < 0 || !statusLine.startsWith("HTTP/")) {
            throw new MongoSocketException(
                    "HTTP proxy returned a malformed response while establishing a tunnel to " + target
                            + ": " + statusLine, target);
        }
        int secondSpace = statusLine.indexOf(' ', firstSpace + 1);
        String code = secondSpace < 0
                ? statusLine.substring(firstSpace + 1)
                : statusLine.substring(firstSpace + 1, secondSpace);
        try {
            return Integer.parseInt(code.trim());
        } catch (NumberFormatException e) {
            throw new MongoSocketException(
                    "HTTP proxy returned an unparseable status while establishing a tunnel to " + target
                            + ": " + statusLine, target);
        }
    }

    private static boolean endsWithBlankLine(final CharSequence response) {
        int length = response.length();
        return length >= 4
                && response.charAt(length - 4) == '\r' && response.charAt(length - 3) == '\n'
                && response.charAt(length - 2) == '\r' && response.charAt(length - 1) == '\n';
    }

    private HttpProxyTunnel() {
    }
}
