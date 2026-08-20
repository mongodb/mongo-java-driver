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

package com.mongodb.connection;

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.ClientEncryptionSettings;

/**
 * The protocol spoken to a proxy server.
 *
 * @see ProxySettings.Builder#protocol(ProxyProtocol)
 * @since 5.11
 */
public enum ProxyProtocol {
    /**
     * SOCKS5, as specified by <a href="https://www.rfc-editor.org/rfc/rfc1928">RFC 1928</a>.
     *
     * <p>This is the default, and is the only protocol supported for connections to a MongoDB server.</p>
     */
    SOCKS5,

    /**
     * HTTP, using the {@code CONNECT} method to establish a tunnel to the target host.
     *
     * <p>This protocol is currently supported only for Key Management Service (KMS) requests made by in-use
     * encryption, configured via {@link ClientEncryptionSettings.Builder#proxySettings(ProxySettings)} or
     * {@link AutoEncryptionSettings.Builder#proxySettings(ProxySettings)}. Configuring it for connections to a
     * MongoDB server is rejected when the client is created.</p>
     *
     * <p>A port must be specified explicitly with {@link ProxySettings.Builder#port(int)}, as there is no
     * standard port for an HTTP proxy.</p>
     */
    HTTP,

    /**
     * HTTP over TLS, using the {@code CONNECT} method to establish a tunnel to the target host.
     *
     * <p>The connection to the proxy itself is protected with TLS, configured by
     * {@link ProxySettings.Builder#sslContext(javax.net.ssl.SSLContext)}. The tunnel then carries a second, independent
     * TLS session negotiated end-to-end with the target host, so the proxy cannot read it.</p>
     *
     * <p>As with {@link #HTTP}, this is supported only for Key Management Service (KMS) requests, and a port must be
     * specified explicitly.</p>
     */
    HTTPS
}
