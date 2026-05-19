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
         * The SOCKS5 method-selection exchange failed. Causes include: incompatible
         * proxy version, no common authentication method, an unrecognised method, or
         * an I/O failure (EOF, timeout, broken pipe) while sending the method-selection
         * request or reading its reply.
         */
        NEGOTIATION,

        /**
         * Username/password sub-negotiation with the proxy failed. Causes include:
         * the proxy rejecting the credentials (typically wrong username/password),
         * or an I/O failure (EOF, timeout, broken pipe) while sending credentials
         * or reading the auth result.
         */
        AUTHENTICATION,

        /**
         * A failure occurred while sending the CONNECT request to the proxy or
         * reading/parsing its reply. Causes include: a parsed non-success RFC 1928
         * reply (in which case {@link MongoSocksProxyException#getProxyReplyCode()}
         * carries the code), an unrecognised reply field or address type, or an
         * I/O failure (EOF, timeout, broken pipe) on the CONNECT exchange.
         */
        CONNECT_RELAY
    }

    private final HandshakePhase handshakePhase;

    @Nullable
    private final Integer proxyReplyCode;

    /**
     * Construct an instance with no RFC 1928 reply code and no cause. Suitable for any phase
     * whose failure does not carry a parsed reply code: {@link HandshakePhase#PROXY_TCP_CONNECT},
     * {@link HandshakePhase#NEGOTIATION}, {@link HandshakePhase#AUTHENTICATION}, and the
     * {@link HandshakePhase#CONNECT_RELAY} sub-cases driven by an I/O failure or an unrecognised
     * reply field.
     *
     * @param message        the message
     * @param serverAddress  the server address
     * @param handshakePhase the phase at which the failure occurred
     */
    public MongoSocksProxyException(final String message, final ServerAddress serverAddress, final HandshakePhase handshakePhase) {
        this(message, serverAddress, notNull("handshakePhase", handshakePhase), null);
    }

    /**
     * Construct an instance with no RFC 1928 reply code. Suitable for any phase whose failure
     * does not carry a parsed reply code: {@link HandshakePhase#PROXY_TCP_CONNECT},
     * {@link HandshakePhase#NEGOTIATION}, {@link HandshakePhase#AUTHENTICATION}, and the
     * {@link HandshakePhase#CONNECT_RELAY} sub-cases driven by an I/O failure or an unrecognised
     * reply field.
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
     * Construct an instance with an optional RFC 1928 reply code. A non-{@code null}
     * {@code proxyReplyCode} should only accompany {@link HandshakePhase#CONNECT_RELAY} and
     * indicates a successfully parsed non-success reply from the proxy. Use {@code null} in
     * all other cases — including {@link HandshakePhase#CONNECT_RELAY} failures caused by an
     * I/O error or an unrecognised reply field.
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
     * Construct an instance with an optional RFC 1928 reply code. A non-{@code null}
     * {@code proxyReplyCode} should only accompany {@link HandshakePhase#CONNECT_RELAY} and
     * indicates a successfully parsed non-success reply from the proxy. Use {@code null} in
     * all other cases — including {@link HandshakePhase#CONNECT_RELAY} failures caused by an
     * I/O error or an unrecognised reply field.
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
