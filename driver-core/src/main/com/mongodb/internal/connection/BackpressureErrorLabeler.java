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

import com.mongodb.MongoException;
import com.mongodb.MongoSocketException;
import com.mongodb.lang.Nullable;

import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLProtocolException;
import java.net.UnknownHostException;
import java.security.cert.CertPathBuilderException;
import java.security.cert.CertPathValidatorException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Attaches {@link MongoException#SYSTEM_OVERLOADED_ERROR_LABEL} and
 * {@link MongoException#RETRYABLE_ERROR_LABEL} to network errors encountered during connection
 * establishment or the hello message, per the CMAP specification.
 *
 * <p>This is topology-agnostic: it must be invoked from the connection-establishment path so that
 * both default SDAM and load-balanced modes are covered.
 */
final class BackpressureErrorLabeler {

    /**
     * BouncyCastle TLS fatal-alert exception types resolved at class-load time. If BC isn't on the
     * classpath the list is empty and {@link #isBouncyCastleTlsError(Throwable)} short-circuits to false.
     */
    private static final List<Class<?>> BOUNCY_CASTLE_TLS_FATAL_TYPES = Stream.of(
                    "org.bouncycastle.tls.TlsFatalAlert",
                    "org.bouncycastle.tls.TlsFatalAlertReceived",
                    "org.bouncycastle.tls.crypto.TlsCryptoException")
            .map(BackpressureErrorLabeler::loadClassOrNull)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    /**
     * RFC 5246 / RFC 8446 alert descriptions that surface in BouncyCastle TLS exception messages.
     * See <a href="https://github.com/bcgit/bc-java/blob/main/tls/src/main/java/org/bouncycastle/tls/AlertDescription.java">AlertDescription.java</a>.
     * See <a href="https://datatracker.ietf.org/doc/html/rfc5246#section-7.2">(TLS) Protocol Version 1.2 - Alert Protocol</a>.
     */
    private static final Set<String> BOUNCY_CASTLE_TLS_ALERT_DESCRIPTIONS = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(
                    "close_notify", "unexpected_message", "bad_record_mac", "decryption_failed",
                    "record_overflow", "decompression_failure", "handshake_failure", "no_certificate",
                    "bad_certificate", "unsupported_certificate", "certificate_revoked", "certificate_expired",
                    "certificate_unknown", "illegal_parameter", "unknown_ca", "access_denied",
                    "decode_error", "decrypt_error", "export_restriction", "protocol_version",
                    "insufficient_security", "internal_error", "no_renegotiation", "unsupported_extension",
                    "certificate_unobtainable", "unrecognized_name", "bad_certificate_status_response",
                    "bad_certificate_hash_value", "unknown_psk_identity", "no_application_protocol",
                    "inappropriate_fallback", "missing_extension", "certificate_required")));

    private BackpressureErrorLabeler() {
    }

    static void applyLabelsIfEligible(final Throwable t) {
        if (!(t instanceof MongoSocketException)) {
            return;
        }
        MongoSocketException socketException = (MongoSocketException) t;
        if (isDnsLookupFailure(socketException)) {
            return;
        }
        if (isTlsConfigurationError(socketException)) {
            return;
        }
        // TODO-BACKPRESSURE Nabil - SOCKS5 Revisit alongside JAVA-5205 (SOCKS5 in async) so both sync and
        // async proxy error surfaces can be handled together — likely via a dedicated internal
        // exception thrown from the proxy code path.
        socketException.addLabel(MongoException.SYSTEM_OVERLOADED_ERROR_LABEL);
        socketException.addLabel(MongoException.RETRYABLE_ERROR_LABEL);
    }

    private static boolean isDnsLookupFailure(final MongoSocketException t) {
        Throwable cause = t.getCause();
        while (cause != null) {
            if (cause instanceof UnknownHostException) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    private static boolean isTlsConfigurationError(final MongoSocketException t) {
        Throwable cause = t.getCause();
        while (cause != null) {
            if (cause instanceof CertificateException
                    || cause instanceof CertPathBuilderException
                    || cause instanceof CertPathValidatorException
                    || cause instanceof SSLPeerUnverifiedException
                    || cause instanceof SSLProtocolException) {
                return true;
            }
            if (cause instanceof SSLHandshakeException) {
                String message = cause.getMessage();
                if (message != null) {
                    String lowerMessage = message.toLowerCase(Locale.ROOT);
                    if (lowerMessage.contains("verify")
                            || lowerMessage.contains("protocol")
                            || lowerMessage.contains("cipher")
                            || lowerMessage.contains("received fatal alert")) {
                        return true;
                    }
                }
            }
            if (isBouncyCastleTlsError(cause)) {
                return true;
            }

            cause = cause.getCause();
        }
        return false;
    }

    private static boolean isBouncyCastleTlsError(final Throwable cause) {
        boolean isBcType = false;
        for (Class<?> bcType : BOUNCY_CASTLE_TLS_FATAL_TYPES) {
            if (bcType.isInstance(cause)) {
                isBcType = true;
                break;
            }
        }
        if (!isBcType) {
            return false;
        }
        String message = cause.getMessage();
        if (message == null) {
            return false;
        }
        String description = message.toLowerCase(Locale.ROOT);
        for (String alertName : BOUNCY_CASTLE_TLS_ALERT_DESCRIPTIONS) {
            if (description.contains(alertName)) {
                return true;
            }
        }
        return false;
    }

    @Nullable
    private static Class<?> loadClassOrNull(final String fqn) {
        try {
            return Class.forName(fqn);
        } catch (ClassNotFoundException e) {
            return null;
        }
    }
}
