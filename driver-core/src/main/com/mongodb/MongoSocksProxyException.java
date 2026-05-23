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

import com.mongodb.lang.Nullable;

/**
 * Thrown when an error occurs while connecting via a SOCKS5 proxy.
 *
 * <p>{@link #getProxyReplyCode()} returns the RFC 1928 reply code sent by the proxy when a
 * non-success CONNECT reply was successfully parsed; it returns {@code null} otherwise
 * (including for any failure that did not produce a parsed CONNECT reply, e.g. proxy TCP
 * connect failure, negotiation failure, authentication failure, failure connecting through
 * the proxy to the target server that did not yield a parsed reply, or an I/O error
 * mid-CONNECT).
 *
 * <p>RFC 1928 reply codes: 1=general failure, 2=connection not allowed by ruleset,
 * 3=network unreachable, 4=host unreachable, 5=connection refused, 6=TTL expired,
 * 7=command not supported, 8=address type not supported.
 *
 * @since 5.8
 */
public class MongoSocksProxyException extends MongoSocketOpenException {
    private static final long serialVersionUID = 1L;

    @Nullable
    private final Integer proxyReplyCode;

    /**
     * Construct an instance with no cause and no RFC 1928 reply code.
     *
     * @param message        the message
     * @param serverAddress  the server address
     */
    public MongoSocksProxyException(final String message, final ServerAddress serverAddress) {
        this(message, serverAddress, null, null);
    }

    /**
     * Construct an instance with a cause and no RFC 1928 reply code.
     *
     * @param message        the message
     * @param serverAddress  the server address
     * @param cause          the cause
     */
    public MongoSocksProxyException(final String message, final ServerAddress serverAddress, final Throwable cause) {
        this(message, serverAddress, cause, null);
    }

    /**
     * Construct an instance with no cause and an RFC 1928 reply code from a parsed non-success
     * CONNECT reply.
     *
     * @param message        the message
     * @param serverAddress  the server address
     * @param proxyReplyCode the RFC 1928 reply code, or {@code null}
     */
    public MongoSocksProxyException(final String message, final ServerAddress serverAddress,
                                    @Nullable final Integer proxyReplyCode) {
        super(message, serverAddress);
        this.proxyReplyCode = proxyReplyCode;
    }

    /**
     * Construct an instance with a cause and an optional RFC 1928 reply code.
     *
     * @param message        the message
     * @param serverAddress  the server address
     * @param cause          the cause, may be {@code null}
     * @param proxyReplyCode the RFC 1928 reply code, or {@code null}
     */
    public MongoSocksProxyException(final String message, final ServerAddress serverAddress,
                                    @Nullable final Throwable cause, @Nullable final Integer proxyReplyCode) {
        super(message, serverAddress);
        this.proxyReplyCode = proxyReplyCode;
        if (cause != null) {
            initCause(cause);
        }
    }

    /**
     * Returns the RFC 1928 reply code sent by the SOCKS5 proxy when a non-success CONNECT
     * reply was successfully parsed, or {@code null} otherwise.
     *
     * @return the RFC 1928 proxy reply code, or {@code null}
     */
    @Nullable
    public Integer getProxyReplyCode() {
        return proxyReplyCode;
    }
}
