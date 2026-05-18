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
import com.mongodb.MongoSocksProxyException;
import com.mongodb.ServerAddress;
import net.bytebuddy.ByteBuddy;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
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

    static Stream<Named<MongoSocketException>> socksProxyPostTcpPhaseShouldNotBeLabeled() {
        // NEGOTIATION / AUTHENTICATION / CONNECT_RELAY are configuration/protocol-level errors
        // surfaced after the TCP connection to the proxy succeeded. They are not overload signals.
        return Stream.of(
                named(new MongoSocksProxyException("negotiation failed", ADDRESS,
                        MongoSocksProxyException.HandshakePhase.NEGOTIATION)),
                named(new MongoSocksProxyException("auth failed", ADDRESS,
                        MongoSocksProxyException.HandshakePhase.AUTHENTICATION)),
                named(new MongoSocksProxyException("connect relay failed", ADDRESS,
                        MongoSocksProxyException.HandshakePhase.CONNECT_RELAY, 5))
        );
    }

    @ParameterizedTest
    @MethodSource
    void socksProxyPostTcpPhaseShouldNotBeLabeled(final MongoSocketException e) {
        BackpressureErrorLabeler.applyLabelsIfEligible(e);
        assertLacksBackpressureLabels(e);
    }

    @Test
    void socksProxyTcpConnectPhaseShouldBeLabeled() {
        // PROXY_TCP_CONNECT is a plain TCP-level failure reaching the proxy host — structurally
        // identical to any other socket-open failure (proxy may be transiently unreachable or
        // overloaded). It must still receive backpressure labels.
        MongoSocksProxyException e = new MongoSocksProxyException(
                "tcp connect to proxy failed", ADDRESS,
                MongoSocksProxyException.HandshakePhase.PROXY_TCP_CONNECT);
        BackpressureErrorLabeler.applyLabelsIfEligible(e);
        assertHasBackpressureLabels(e);
    }

    static Stream<Named<Throwable>> localTlsConfigErrorShouldNotBeLabeled() {
        return Stream.of(
                named(new CertificateException("bad cert")),
                named(new CertPathBuilderException("path build failed")),
                named(new CertPathValidatorException("validation failed")),
                named(new SSLPeerUnverifiedException("peer not verified")),
                named(new SSLProtocolException("protocol error")),
                named(initCause(
                        new SSLHandshakeException("SSLHandshakeException invoking https://1.2.3.4:8443/api/methodName: "
                                + "sun.security.validator.ValidatorException: PKIX path building failed"),
                        initCause(
                                new SSLHandshakeException("sun.security.validator.ValidatorException: "
                                        + "PKIX path building failed: "
                                        + "sun.security.provider.certpath.SunCertPathBuilderException: "
                                        + "unable to find valid certification path to requested target"),
                                new CertPathBuilderException(
                                        "unable to find valid certification path to requested target"))))
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

    /**
     * BouncyCastle isn't on the test classpath, so we use ByteBuddy to synthesise classes with the
     * exact FQCNs the labeler matches against. This exercises the FQCN-walk in
     * {@code isBouncyCastleTlsFatalType} without taking a compile- or runtime dependency on BC.
     */
    @ParameterizedTest(name = "{0} with alert {1}")
    @MethodSource
    void bouncyCastleTlsFatalAlertShouldNotBeLabeled(final String bcFqcn, final String alertMessage) throws Exception {
        Throwable bcCause = newBouncyCastleStub(bcFqcn, alertMessage);
        MongoSocketException e = new MongoSocketException("tls", ADDRESS, bcCause);
        BackpressureErrorLabeler.applyLabelsIfEligible(e);
        assertLacksBackpressureLabels(e);
    }

    static Stream<Arguments> bouncyCastleTlsFatalAlertShouldNotBeLabeled() {
        return Stream.of(
                Arguments.of("org.bouncycastle.tls.TlsFatalAlert", "handshake_failure(40)"),
                Arguments.of("org.bouncycastle.tls.TlsFatalAlertReceived", "unknown_ca(48)"),
                Arguments.of("org.bouncycastle.tls.crypto.TlsCryptoException", "bad_certificate(42)"));
    }

    /**
     * Subclasses of known BC types must still match — the labeler walks the superclass chain.
     */
    @Test
    void bouncyCastleSubclassWithAlertShouldNotBeLabeled() throws Exception {
        Class<?> bcParent = new ByteBuddy()
                .subclass(Exception.class)
                .name("org.bouncycastle.tls.TlsFatalAlert")
                .make()
                .load(BackpressureErrorLabelerTest.class.getClassLoader())
                .getLoaded();
        Class<?> bcSubclass = new ByteBuddy()
                .subclass(bcParent)
                .name("com.example.CustomBcSubclass")
                .make()
                .load(bcParent.getClassLoader())
                .getLoaded();
        Throwable cause = (Throwable) bcSubclass.getConstructor(String.class).newInstance("handshake_failure(40)");
        MongoSocketException e = new MongoSocketException("tls", ADDRESS, cause);
        BackpressureErrorLabeler.applyLabelsIfEligible(e);
        assertLacksBackpressureLabels(e);
    }

    /**
     * BC type but the message has no recognised alert description — the alert-keyword filter
     * rejects it, so the labeler falls through and applies backpressure labels.
     */
    @Test
    void bouncyCastleTypeWithoutAlertKeywordShouldBeLabeled() throws Exception {
        Throwable bcCause = newBouncyCastleStub("org.bouncycastle.tls.TlsFatalAlert", "something unrelated");
        MongoSocketException e = new MongoSocketException("tls", ADDRESS, bcCause);
        BackpressureErrorLabeler.applyLabelsIfEligible(e);
        assertHasBackpressureLabels(e);
    }

    private static Throwable newBouncyCastleStub(final String fqcn, final String message) throws Exception {
        Class<?> cls = new ByteBuddy()
                .subclass(Exception.class)
                .name(fqcn)
                .make()
                .load(BackpressureErrorLabelerTest.class.getClassLoader())
                .getLoaded();
        return (Throwable) cls.getConstructor(String.class).newInstance(message);
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
