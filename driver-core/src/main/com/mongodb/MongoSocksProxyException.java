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

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Thrown when an error occurs while establishing a connection to a SOCKS5 proxy.
 *
 * <p>Per the CMAP specification, post-TCP SOCKS5 failures
 * ({@link HandshakePhase#NEGOTIATION}, {@link HandshakePhase#AUTHENTICATION},
 * {@link HandshakePhase#CONNECT_RELAY}) are excluded from backpressure error labels
 * ({@link MongoException#SYSTEM_OVERLOADED_ERROR_LABEL},
 * {@link MongoException#RETRYABLE_ERROR_LABEL}). Failures in the
 * {@link HandshakePhase#PROXY_TCP_CONNECT} phase are plain TCP-level reach failures
 * to the proxy host and continue to receive these labels like any other
 * socket-open failure.
 *
 * <p>The {@link #getHandshakePhase()} identifies which phase of the SOCKS5 handshake failed.
 * {@link #getProxyReplyCode()} returns the RFC 1928 reply code sent by the proxy when a
 * non-success CONNECT reply was successfully parsed; it returns {@code null} otherwise
 * (including for {@link HandshakePhase#CONNECT_RELAY} failures caused by an I/O error or
 * an unrecognised reply field).
 *
 * <p>RFC 1928 reply codes: 1=general failure, 2=connection not allowed by ruleset,
 * 3=network unreachable, 4=host unreachable, 5=connection refused, 6=TTL expired,
 * 7=command not supported, 8=address type not supported.
 *
 * @since 5.8
 */
public class MongoSocksProxyException extends MongoSocketOpenException {
    private static final long serialVersionUID = 1L;

    /**
     * The phase of the SOCKS5 handshake at which the failure occurred.
     *
     * @since 5.8
     */
    public enum HandshakePhase {
        /**
         * TCP connection to the proxy host itself failed before any SOCKS5 exchange.
         * The proxy may be temporarily unreachable.
         */
        PROXY_TCP_CONNECT,

        /**
         * SOCKS5 method-selection exchange failed: the proxy version is incompatible,
         * no common authentication method was found, or the proxy returned an
         * unrecognised method. This is always a configuration error.
         */
        NEGOTIATION,

        /**
         * Credential verification with the proxy failed. This is always a
         * configuration error (wrong username or password).
         */
        AUTHENTICATION,

        /**
         * The proxy processed the CONNECT command for the target host and returned
         * a non-success reply code. See {@link MongoSocksProxyException#getProxyReplyCode()}
         * for the specific RFC 1928 reply code.
         */
        CONNECT_RELAY
    }

    private final HandshakePhase handshakePhase;

    @Nullable
    private final Integer proxyReplyCode;

    /**
     * Construct an instance for failures that have no RFC 1928 reply code and no cause
     * ({@link HandshakePhase#PROXY_TCP_CONNECT}, {@link HandshakePhase#NEGOTIATION},
     * {@link HandshakePhase#AUTHENTICATION}).
     *
     * @param message        the message
     * @param serverAddress  the server address
     * @param handshakePhase the phase at which the failure occurred
     */
    public MongoSocksProxyException(final String message, final ServerAddress serverAddress, final HandshakePhase handshakePhase) {
        this(message, serverAddress, notNull("handshakePhase", handshakePhase), null);
    }

    /**
     * Construct an instance for failures that have no RFC 1928 reply code
     * ({@link HandshakePhase#PROXY_TCP_CONNECT}, {@link HandshakePhase#NEGOTIATION},
     * {@link HandshakePhase#AUTHENTICATION}).
     *
     * @param message        the message
     * @param address        the server address
     * @param cause          the cause
     * @param handshakePhase the phase at which the failure occurred
     */
    public MongoSocksProxyException(final String message, final ServerAddress address,
                                    final Throwable cause, final HandshakePhase handshakePhase) {
        this(message, address, cause, notNull("handshakePhase", handshakePhase), null);
    }

    /**
     * Construct an instance with an optional RFC 1928 reply code.
     * Use {@code null} for phases that do not carry a reply code
     * ({@link HandshakePhase#PROXY_TCP_CONNECT}, {@link HandshakePhase#NEGOTIATION},
     * {@link HandshakePhase#AUTHENTICATION}).
     *
     * @param message        the message
     * @param address        the server address
     * @param handshakePhase the phase at which the failure occurred
     * @param proxyReplyCode the RFC 1928 reply code, or {@code null}
     */
    public MongoSocksProxyException(final String message, final ServerAddress address, final HandshakePhase handshakePhase,
            @Nullable final Integer proxyReplyCode) {
        super(message, address);
        this.handshakePhase = notNull("handshakePhase", handshakePhase);
        this.proxyReplyCode = proxyReplyCode;
    }

    /**
     * Construct an instance with an optional RFC 1928 reply code.
     * Use {@code null} for phases that do not carry a reply code
     * ({@link HandshakePhase#PROXY_TCP_CONNECT}, {@link HandshakePhase#NEGOTIATION},
     * {@link HandshakePhase#AUTHENTICATION}).
     *
     * @param message        the message
     * @param address        the server address
     * @param cause          the cause
     * @param handshakePhase the phase at which the failure occurred
     * @param proxyReplyCode the RFC 1928 reply code, or {@code null}
     */
    public MongoSocksProxyException(final String message, final ServerAddress address,
                                    final Throwable cause, final HandshakePhase handshakePhase,
                                    @Nullable final Integer proxyReplyCode) {
        super(message, address, cause);
        this.handshakePhase = notNull("handshakePhase", handshakePhase);
        this.proxyReplyCode = proxyReplyCode;
    }

    /**
     * Returns the phase of the SOCKS5 handshake at which the failure occurred.
     *
     * @return the handshake phase, never {@code null}
     */
    public HandshakePhase getHandshakePhase() {
        return handshakePhase;
    }

    /**
     * Returns the RFC 1928 reply code sent by the SOCKS5 proxy in response to a CONNECT request,
     * or {@code null} if the failure occurred before the proxy sent a CONNECT response
     * (i.e. phase is not {@link HandshakePhase#CONNECT_RELAY}).
     *
     * @return the RFC 1928 proxy reply code, or {@code null}
     */
    @Nullable
    public Integer getProxyReplyCode() {
        return proxyReplyCode;
    }
}
