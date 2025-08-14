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

package com.mongodb.client.auth;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoSecurityException;
import com.mongodb.client.Fixture;
import com.mongodb.client.MongoClient;
import com.mongodb.connection.NettyTransportSettings;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.stream.Stream;

import static com.mongodb.AuthenticationMechanism.MONGODB_X509;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(AbstractX509AuthenticationTest.X509AuthenticationPropertyCondition.class)
public abstract class AbstractX509AuthenticationTest {

    private static final String KEYSTORE_PASSWORD = "test";
    protected abstract MongoClient createMongoClient(MongoClientSettings mongoClientSettings);

    private static Stream<Arguments> shouldAuthenticateWithClientCertificate() throws Exception {
        String keystoreFileName = "existing_user.p12";
        return getArgumentForKeystore(keystoreFileName);
    }

    @ParameterizedTest(name = "should authenticate with client certificate. MongoClientSettings: {0}")
    @MethodSource
    public void shouldAuthenticateWithClientCertificate(final MongoClientSettings mongoClientSettings) {
        //given
        try (MongoClient client = createMongoClient(mongoClientSettings)) {

            //when & then command completes successfully with x509 authentication
            client.getDatabase("test").getCollection("test").estimatedDocumentCount();
        }
    }

    private static Stream<Arguments> shouldPassMutualTLSWithClientCertificateAndFailAuthenticateWithAbsentUser() throws Exception {
        String keystoreFileName = "non_existing_user.p12";
        return getArgumentForKeystore(keystoreFileName);
    }

    @ParameterizedTest(name = "should pass mutual TLS with client certificate and fail authenticate with absent user. "
            + "MongoClientSettings: {0}")
    @MethodSource
    public void shouldPassMutualTLSWithClientCertificateAndFailAuthenticateWithAbsentUser(final MongoClientSettings mongoClientSettings) {
        // given
        try (MongoClient client = createMongoClient(mongoClientSettings)) {

            // when & then
            MongoSecurityException mongoSecurityException = assertThrows(MongoSecurityException.class,
                    () -> client.getDatabase("test").getCollection("test").estimatedDocumentCount());

            assertTrue(mongoSecurityException.getMessage().contains("Exception authenticating"));
            MongoCommandException mongoCommandException = (MongoCommandException) mongoSecurityException.getCause();

            assertTrue(mongoCommandException.getMessage().contains("Could not find user"));
            assertEquals(11, mongoCommandException.getCode());
        }
    }

    private static Stream<Arguments> getArgumentForKeystore(final String keystoreFileName) throws Exception {
        SSLContext context = buildSslContextFromKeyStore(keystoreFileName);
        MongoClientSettings.Builder mongoClientSettingsBuilder = Fixture.getMongoClientSettingsBuilder();
        verifyX509AuthenticationIsRequired(mongoClientSettingsBuilder);

        return Stream.of(
                Arguments.of(mongoClientSettingsBuilder
                        .applyToSslSettings(builder -> builder.context(context))
                        .build()),

                Arguments.of(mongoClientSettingsBuilder
                        .applyToSslSettings(builder -> builder.context(context))
                        .transportSettings(NettyTransportSettings.nettyBuilder()
                                .sslContext(SslContextBuilder.forClient()
                                        .sslProvider(SslProvider.JDK)
                                        .keyManager(getKeyManagerFactory(keystoreFileName))
                                        .build())
                                .build())
                        .build()),

                Arguments.of(mongoClientSettingsBuilder
                        .applyToSslSettings(builder -> builder.context(context))
                        .transportSettings(NettyTransportSettings.nettyBuilder()
                                .sslContext(SslContextBuilder.forClient()
                                        .sslProvider(SslProvider.OPENSSL)
                                        .keyManager(getKeyManagerFactory(keystoreFileName))
                                        .build())
                                .build())
                        .build())
        );
    }

    private static SSLContext buildSslContextFromKeyStore(final String keystoreFileName) throws Exception {
        KeyManagerFactory keyManagerFactory = getKeyManagerFactory(keystoreFileName);
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
        return sslContext;
    }

    private static KeyManagerFactory getKeyManagerFactory(final String keystoreFileName)
            throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException, UnrecoverableKeyException {
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (FileInputStream fis = new FileInputStream(getKeystoreLocation() + File.separator + keystoreFileName)) {
            ks.load(fis, KEYSTORE_PASSWORD.toCharArray());
        }
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(
                KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(ks, KEYSTORE_PASSWORD.toCharArray());
        return keyManagerFactory;
    }

    private static String getKeystoreLocation() {
        return System.getProperty("org.mongodb.test.x509.auth.keystore.location");
    }

    /**
     * The connection string is sourced from an environment variable populated from Secret Storage.
     * We verify it still requires X.509 authentication before running these tests to ensure test invariants.
     */
    private static void verifyX509AuthenticationIsRequired(final MongoClientSettings.Builder mongoClientSettingsBuilder) {
        com.mongodb.assertions.Assertions.assertTrue(
                com.mongodb.assertions.Assertions.assertNotNull(mongoClientSettingsBuilder.build().getCredential())
                        .getAuthenticationMechanism() == MONGODB_X509);
    }

    /**
       This condition allows to skip initialization of method sources and test execution.
        - @EnableIf on the class, assumeTrue in the constructor - do not block method source initialization.
        - assumeTrue in the static block - fails the test.
     **/
    public static class X509AuthenticationPropertyCondition implements ExecutionCondition {
        @Override
        public ConditionEvaluationResult evaluateExecutionCondition(final ExtensionContext context) {
            if (isX509TestsEnabled()) {
                return ConditionEvaluationResult.enabled("Test is enabled because x509 auth configuration exists");
            } else {
                return ConditionEvaluationResult.disabled("Test is disabled because x509 auth configuration is missing");
            }
        }
    }

    private static boolean isX509TestsEnabled() {
        return Boolean.parseBoolean(System.getProperty("org.mongodb.test.x509.auth.enabled"));
    }
}
