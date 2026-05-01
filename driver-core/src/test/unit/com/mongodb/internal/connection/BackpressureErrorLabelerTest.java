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

import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.MongoSecurityException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoSocketOpenException;
import com.mongodb.MongoSocketReadTimeoutException;
import com.mongodb.ServerAddress;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLProtocolException;
import java.io.EOFException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.security.cert.CertPathBuilderException;
import java.security.cert.CertPathValidatorException;
import java.security.cert.CertificateException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BackpressureErrorLabelerTest {

    private static final ServerAddress ADDRESS = new ServerAddress();

    static Stream<Named<MongoSocketException>> networkErrorShouldBeLabeled() {
        return Stream.of(
                named(new MongoSocketException("boom", ADDRESS)),
                named(new MongoSocketReadTimeoutException("slow", ADDRESS, new IOException("read timed out"))),
                named(new MongoSocketOpenException("open failed", ADDRESS, new IOException("connection refused"))),
                // FIN-during-handshake: server closed the TCP connection while the client was mid-handshake
                // (no protocol-level alert). I/O failure → must be labeled per CMAP "I/O error during TLS handshake".
                named(new MongoSocketException("tls", ADDRESS, initCause(
                        new SSLHandshakeException("Remote host terminated the handshake"),
                        new EOFException("SSL peer shut down incorrectly"))))
        );
    }

    @ParameterizedTest
    @MethodSource
    void networkErrorShouldBeLabeled(final MongoSocketException e) {
        BackpressureErrorLabeler.applyLabelsIfEligible(e);
        assertHasBackpressureLabels(e);
    }

    static Stream<Named<MongoSocketException>> dnsFailureShouldNotBeLabeled() {
        return Stream.of(
                named(new MongoSocketException("lookup failed", ADDRESS, new UnknownHostException("nope"))),
                named(new MongoSocketException("wrap", ADDRESS, new IOException("wrap", new UnknownHostException("nope"))))
        );
    }

    @ParameterizedTest
    @MethodSource
    void dnsFailureShouldNotBeLabeled(final MongoSocketException e) {
        BackpressureErrorLabeler.applyLabelsIfEligible(e);
        assertLacksBackpressureLabels(e);
    }

    static Stream<Named<Throwable>> localTlsConfigErrorShouldNotBeLabeled() {
        return Stream.of(
                named(new CertificateException("bad cert")),
                named(new CertPathBuilderException("path build failed")),
                named(new CertPathValidatorException("validation failed")),
                named(new SSLPeerUnverifiedException("peer not verified")),
                named(new SSLProtocolException("protocol error")),
                named(new SSLHandshakeException("PKIX path building failed: sun.security.provider.certpath.SunCertPathBuilderException: "
                        + "unable to find valid certification path to requested target"))
        );
    }

    @ParameterizedTest
    @MethodSource
    void localTlsConfigErrorShouldNotBeLabeled(final Throwable cause) {
        MongoSocketException e = new MongoSocketException("tls", ADDRESS, cause);
        BackpressureErrorLabeler.applyLabelsIfEligible(e);
        assertLacksBackpressureLabels(e);
    }

    /**
     * "Received fatal alert: <description>" means the peer actively answered with a TLS protocol
     * error — definitively a config/protocol issue, not an overload signal. Covers all 25
     * handshake-only RFC alert descriptions emitted by OpenJDK's JSSE provider.
     */
    @ParameterizedTest(name = "Received fatal alert: {0}")
    @ValueSource(strings = {
            "handshake_failure",
            "no_certificate",
            "bad_certificate",
            "unsupported_certificate",
            "certificate_revoked",
            "certificate_expired",
            "certificate_unknown",
            "illegal_parameter",
            "unknown_ca",
            "access_denied",
            "decode_error",
            "decrypt_error",
            "export_restriction",
            "protocol_version",
            "insufficient_security",
            "no_renegotiation",
            "missing_extension",
            "unsupported_extension",
            "certificate_unobtainable",
            "unrecognized_name",
            "bad_certificate_status_response",
            "bad_certificate_hash_value",
            "unknown_psk_identity",
            "certificate_required",
            "no_application_protocol"
    })
    void receivedTlsAlertShouldNotBeLabeled(final String alertDescription) {
        SSLHandshakeException tls = new SSLHandshakeException("Received fatal alert: " + alertDescription);
        MongoSocketException e = new MongoSocketException("tls", ADDRESS, tls);
        BackpressureErrorLabeler.applyLabelsIfEligible(e);
        assertLacksBackpressureLabels(e);
    }

    static Stream<Named<Throwable>> nonSocketErrorShouldNotBeLabeled() {
        return Stream.of(
                named(new MongoSecurityException(
                        MongoCredential.createCredential("user", "db", "pwd".toCharArray()), "auth failed")),
                named(new MongoException(42, "some command error")),
                named(new IOException("raw"))
        );
    }

    @ParameterizedTest
    @MethodSource
    void nonSocketErrorShouldNotBeLabeled(final Throwable e) {
        BackpressureErrorLabeler.applyLabelsIfEligible(e);
        if (e instanceof MongoException) {
            assertLacksBackpressureLabels((MongoException) e);
        }
    }

    private static <T extends Throwable> Named<T> named(final T e) {
        return Named.of(e.getClass().getSimpleName(), e);
    }

    private static <T extends Throwable> T initCause(final T exception, final Throwable cause) {
        exception.initCause(cause);
        return exception;
    }

    private static void assertHasBackpressureLabels(final MongoException e) {
        assertTrue(e.hasErrorLabel(MongoException.SYSTEM_OVERLOADED_ERROR_LABEL),
                "expected SystemOverloadedError label");
        assertTrue(e.hasErrorLabel(MongoException.RETRYABLE_ERROR_LABEL),
                "expected RetryableError label");
    }

    private static void assertLacksBackpressureLabels(final MongoException e) {
        assertFalse(e.hasErrorLabel(MongoException.SYSTEM_OVERLOADED_ERROR_LABEL),
                "unexpected SystemOverloadedError label");
        assertFalse(e.hasErrorLabel(MongoException.RETRYABLE_ERROR_LABEL),
                "unexpected RetryableError label");
    }
}
