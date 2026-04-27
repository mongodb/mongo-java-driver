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

import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLProtocolException;
import java.net.UnknownHostException;
import java.security.cert.CertPathBuilderException;
import java.security.cert.CertPathValidatorException;
import java.security.cert.CertificateException;
import java.util.Locale;

/**
 * Attaches {@link MongoException#SYSTEM_OVERLOADED_ERROR_LABEL} and
 * {@link MongoException#RETRYABLE_ERROR_LABEL} to network errors encountered during connection
 * establishment or the hello message, per the CMAP specification.
 *
 * <p>This is topology-agnostic: it must be invoked from the connection-establishment path so that
 * both default SDAM and load-balanced modes are covered.
 */
final class BackpressureErrorLabeler {

    private BackpressureErrorLabeler() {
    }

    static void applyLabelsIfEligible(final Throwable t) {
        if (!(t instanceof MongoSocketException)) {
            return;
        }
        if (isDnsLookupFailure(t)) {
            return;
        }
        if (isTlsConfigurationError(t)) {
            return;
        }
        // TODO-BACKPRESSURE Nabil - SOCKS5 Revisit alongside JAVA-5205 (SOCKS5 in async) so both sync and
        // async proxy error surfaces can be handled together — likely via a dedicated internal
        // exception thrown from the proxy code path.
        MongoException mongoException = (MongoException) t;
        mongoException.addLabel(MongoException.SYSTEM_OVERLOADED_ERROR_LABEL);
        mongoException.addLabel(MongoException.RETRYABLE_ERROR_LABEL);
    }

    private static boolean isDnsLookupFailure(final Throwable t) {
        Throwable cause = t.getCause();
        while (cause != null) {
            if (cause instanceof UnknownHostException) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    private static boolean isTlsConfigurationError(final Throwable t) {
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
                    if (lowerMessage.contains("certificate")
                            || lowerMessage.contains("verify")
                            || lowerMessage.contains("trust")
                            || lowerMessage.contains("hostname")
                            || lowerMessage.contains("protocol")
                            || lowerMessage.contains("cipher")
                            || lowerMessage.contains("handshake_failure")) {
                        return true;
                    }
                }
            }
            cause = cause.getCause();
        }
        return false;
    }
}
