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
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLPeerUnverifiedException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.security.cert.CertificateException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BackpressureErrorLabelerTest {

    private static final ServerAddress ADDRESS = new ServerAddress();

    @Test
    void mongoSocketExceptionIsLabeled() {
        MongoSocketException e = new MongoSocketException("boom", ADDRESS);
        BackpressureErrorLabeler.applyLabelsIfEligible(e);
        assertHasBackpressureLabels(e);
    }

    @Test
    void mongoSocketReadTimeoutIsLabeled() {
        MongoSocketReadTimeoutException e = new MongoSocketReadTimeoutException("slow", ADDRESS, new IOException("read timed out"));
        BackpressureErrorLabeler.applyLabelsIfEligible(e);
        assertHasBackpressureLabels(e);
    }

    @Test
    void mongoSocketOpenExceptionIsLabeled() {
        MongoSocketOpenException e = new MongoSocketOpenException("open failed", ADDRESS, new IOException("connection refused"));
        BackpressureErrorLabeler.applyLabelsIfEligible(e);
        assertHasBackpressureLabels(e);
    }

    @Test
    void dnsFailureIsNotLabeled() {
        MongoSocketException e = new MongoSocketException("lookup failed", ADDRESS, new UnknownHostException("nope"));
        BackpressureErrorLabeler.applyLabelsIfEligible(e);
        assertLacksBackpressureLabels(e);
    }

    @Test
    void dnsFailureDeepInCauseChainIsNotLabeled() {
        Throwable dns = new UnknownHostException("nope");
        IOException wrap1 = new IOException("wrap1", dns);
        MongoSocketException e = new MongoSocketException("wrap2", ADDRESS, wrap1);
        BackpressureErrorLabeler.applyLabelsIfEligible(e);
        assertLacksBackpressureLabels(e);
    }

    @Test
    void tlsCertificateErrorIsNotLabeled() {
        CertificateException cert = new CertificateException("bad cert");
        MongoSocketException e = new MongoSocketException("tls", ADDRESS, cert);
        BackpressureErrorLabeler.applyLabelsIfEligible(e);
        assertLacksBackpressureLabels(e);
    }

    @Test
    void tlsPeerUnverifiedIsNotLabeled() {
        SSLPeerUnverifiedException tls = new SSLPeerUnverifiedException("peer not verified");
        MongoSocketException e = new MongoSocketException("tls", ADDRESS, tls);
        BackpressureErrorLabeler.applyLabelsIfEligible(e);
        assertLacksBackpressureLabels(e);
    }

    @Test
    void sslHandshakeWithCertificateMessageIsNotLabeled() {
        SSLHandshakeException tls = new SSLHandshakeException("PKIX path building failed: certificate chain invalid");
        MongoSocketException e = new MongoSocketException("tls", ADDRESS, tls);
        BackpressureErrorLabeler.applyLabelsIfEligible(e);
        assertLacksBackpressureLabels(e);
    }

    @Test
    void sslHandshakeWithoutConfigKeywordIsLabeled() {
        SSLHandshakeException tls = new SSLHandshakeException("remote host closed connection during handshake");
        MongoSocketException e = new MongoSocketException("tls", ADDRESS, tls);
        BackpressureErrorLabeler.applyLabelsIfEligible(e);
        assertHasBackpressureLabels(e);
    }

    @Test
    void authErrorIsNotLabeled() {
        MongoCredential credential = MongoCredential.createCredential("user", "db", "pwd".toCharArray());
        MongoSecurityException e = new MongoSecurityException(credential, "auth failed");
        BackpressureErrorLabeler.applyLabelsIfEligible(e);
        assertLacksBackpressureLabels(e);
    }

    @Test
    void commandExceptionIsNotLabeled() {
        MongoException e = new MongoException(42, "some command error");
        BackpressureErrorLabeler.applyLabelsIfEligible(e);
        assertLacksBackpressureLabels(e);
    }

    @Test
    void nonMongoThrowableIsNotLabeled() {
        IOException e = new IOException("raw");
        BackpressureErrorLabeler.applyLabelsIfEligible(e);
    }

    @Test
    void sdamIssueTlsCheckDelegatesToHelper() {
        MongoSocketException tlsException = new MongoSocketException("tls", ADDRESS, new CertificateException("bad cert"));
        assertTrue(BackpressureErrorLabeler.isTlsConfigurationError(tlsException));

        MongoSocketException plainException = new MongoSocketException("plain", ADDRESS);
        assertFalse(BackpressureErrorLabeler.isTlsConfigurationError(plainException));

        assertFalse(BackpressureErrorLabeler.isTlsConfigurationError(new MongoException(0, "not socket", new BsonDocument())));
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
