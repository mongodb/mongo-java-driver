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

package com.mongodb;

import com.mongodb.annotations.ThreadSafe;

import java.io.IOException;
import java.net.Socket;

/**
 * A callback that establishes the connection used for a Key Management Service (KMS) request made by in-use encryption
 * (client-side field level encryption or queryable encryption).
 *
 * <p>When a callback is configured, the driver invokes it instead of connecting to the KMS host itself, and uses the
 * socket it returns for the KMS request. This enables routing KMS requests through an intermediary, most commonly an
 * HTTP proxy via the {@code HTTP CONNECT} method.</p>
 *
 * <p>The driver always negotiates TLS with the KMS host itself over the returned socket, using the
 * {@link javax.net.ssl.SSLContext} configured for the KMS provider. Server Name Indication and certificate hostname
 * verification target {@link KmsConnectContext#getServerAddress()}, not the address the callback actually connected to.
 * Implementations therefore MUST NOT negotiate TLS with the KMS host themselves; they must return a socket over which
 * a TLS handshake with the KMS host can be performed. An implementation may use TLS for its own connection to an
 * intermediary, in which case it returns an {@link javax.net.ssl.SSLSocket} and the driver layers the KMS host's TLS
 * session on top of it.</p>
 *
 * <p>An {@link IOException} thrown by an implementation is treated as a transient network error.</p>
 *
 * <p>Authenticating to an intermediary is the responsibility of the implementation. For a proxy requiring HTTP Basic
 * authentication, for example, the implementation adds a {@code Proxy-Authorization} header to the {@code CONNECT}
 * request.</p>
 *
 * <p>Example of an implementation that tunnels through an HTTP proxy:</p>
 * <pre>{@code
 * KmsConnectCallback callback = context -> {
 *     Socket socket = new Socket();
 *     int timeout = (int) context.getTimeoutMillis();
 *     socket.connect(new InetSocketAddress("proxy.example.com", 8080), timeout);
 *     socket.setSoTimeout(timeout);
 *
 *     String target = context.getServerAddress().getHost() + ":" + context.getServerAddress().getPort();
 *     socket.getOutputStream().write(
 *             ("CONNECT " + target + " HTTP/1.1\r\nHost: " + target + "\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
 *
 *     // Read the status line and confirm a 2xx status, throwing an IOException otherwise. Match the status code
 *     // rather than the whole status line, as proxies differ in the HTTP version they reply with. Read the response
 *     // one byte at a time, up to the end of the header block, so that no byte of the TLS handshake that the driver
 *     // performs over this socket is consumed.
 *     readAndCheckProxyResponse(socket.getInputStream());
 *
 *     return socket;
 * };
 * }</pre>
 *
 * @see ClientEncryptionSettings.Builder#kmsConnectCallback(KmsConnectCallback)
 * @see AutoEncryptionSettings.Builder#kmsConnectCallback(KmsConnectCallback)
 * @since 5.11
 */
@ThreadSafe
@FunctionalInterface
public interface KmsConnectCallback {
    /**
     * Returns a socket connected such that a TLS handshake with the KMS host can be performed over it.
     *
     * <p>Ownership of the returned socket passes to the driver, which closes it once the KMS request completes.</p>
     *
     * @param context the details of the connection to establish, which may gain further properties in future releases
     * @return a connected socket, which must not have an established TLS session with the KMS host
     * @throws IOException if the connection cannot be established. This is treated as a transient network error.
     */
    Socket connect(KmsConnectContext context) throws IOException;
}
