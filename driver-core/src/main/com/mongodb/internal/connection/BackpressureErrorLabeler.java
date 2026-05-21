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
import com.mongodb.MongoSocksProxyException;

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
import java.util.Locale;
import java.util.Set;

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
     * BouncyCastle TLS fatal-alert exception type names.
     */
    private static final Set<String> BOUNCY_CASTLE_TLS_FATAL_TYPE_NAMES = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(
                    "org.bouncycastle.tls.TlsFatalAlert",
                    "org.bouncycastle.tls.TlsFatalAlertReceived",
                    "org.bouncycastle.tls.crypto.TlsCryptoException")));

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
        if (isSocksFailure(socketException)) {
            return;
        }
        if (isDnsLookupFailure(socketException)) {
            return;
        }
        if (isTlsConfigurationError(socketException)) {
            return;
        }
        socketException.addLabel(MongoException.SYSTEM_OVERLOADED_ERROR_LABEL);
        socketException.addLabel(MongoException.RETRYABLE_ERROR_LABEL);
    }

    private static boolean isSocksFailure(final MongoSocketException t) {
        if (!(t instanceof MongoSocksProxyException)) {
            return false;
        }
        Integer replyCode = ((MongoSocksProxyException) t).getProxyReplyCode();
        if (replyCode == null) {
            return true;
        }
        return replyCode != SocksSocket.ServerReply.NET_UNREACHABLE.getReplyNumber()
                && replyCode != SocksSocket.ServerReply.HOST_UNREACHABLE.getReplyNumber()
                && replyCode != SocksSocket.ServerReply.CONN_REFUSED.getReplyNumber();
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
        if (!isBouncyCastleTlsFatalType(cause.getClass())) {
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

    /**
     * Walks the class hierarchy comparing fully qualified names so a subclass of a known BC type
     * still matches.
     */
    private static boolean isBouncyCastleTlsFatalType(final Class<?> exceptionClass) {
        Class<?> cls = exceptionClass;
        while (cls != null) {
            if (BOUNCY_CASTLE_TLS_FATAL_TYPE_NAMES.contains(cls.getName())) {
                return true;
            }
            cls = cls.getSuperclass();
        }
        return false;
    }
}
