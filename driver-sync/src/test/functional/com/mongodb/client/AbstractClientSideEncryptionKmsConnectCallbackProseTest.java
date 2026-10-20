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

package com.mongodb.client;

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.ClientEncryptionSettings;
import com.mongodb.MongoClientSettings;
import com.mongodb.KmsConnectCallback;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.lang.Nullable;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.mongodb.ClusterFixture.isClientSideEncryptionTest;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Prose test 28, "KMS Connect Callback".
 *
 * <p>All cases require real AWS KMS credentials and are skipped when they are not available. The KMS HTTP proxy is
 * started by {@code drivers-evergreen-tools}: {@code .evergreen/csfle/start-servers.sh} runs {@code kms_http_proxy.py}
 * on port 9004 in plain HTTP mode and on port 9005 in HTTPS mode. Cases are skipped when the proxy is unreachable, so
 * that this test does not fail when run outside that environment.</p>
 *
 * <p>Case 6, "Retry", is not implemented: it is to be skipped by drivers that do not implement DRIVERS-1541, and this
 * driver does not retry KMS requests. TODO-JAVA-5391</p>
 *
 * @see <a href="https://github.com/mongodb/specifications/blob/master/source/client-side-encryption/tests/README.md#28-kms-connect-callback">
 * Prose test 28</a>
 */
public abstract class AbstractClientSideEncryptionKmsConnectCallbackProseTest {

    private static final String PROXY_HOST = "127.0.0.1";
    private static final int HTTP_PROXY_PORT = 9004;
    private static final int HTTPS_PROXY_PORT = 9005;
    private static final int PROXY_TIMEOUT_MILLIS = 10000;

    private static final String KEY_VAULT_NAMESPACE = "keyvault.datakeys";
    private static final String MASTER_KEY = "{"
            + "region: \"us-east-1\", "
            + "key: \"arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0\"}";

    private static final Pattern CONNECT_COUNT_PATTERN = Pattern.compile("connect_count (\\d+)");

    protected abstract ClientEncryption createClientEncryption(ClientEncryptionSettings settings);

    protected abstract MongoClient createMongoClient(MongoClientSettings settings);


    @Test
    @DisplayName("Case 1: plain HTTP proxy")
    void testPlainHttpProxy() throws IOException {
        assumeTrue(isClientSideEncryptionTest(), "Requires AWS KMS credentials");
        assumeProxyIsRunning(false);
        resetMetrics(false);

        try (ClientEncryption clientEncryption = createClientEncryption(clientEncryptionSettingsBuilder()
                .kmsConnectCallback(proxyConnectCallback(false))
                .build())) {
            assertNotNull(createDataKey(clientEncryption));
        }

        assertTrue(getConnectCount(false) >= 1, "expected the KMS request to be routed through the proxy");
    }

    @Test
    @DisplayName("Case 2: HTTPS proxy")
    void testHttpsProxy() throws IOException {
        assumeTrue(isClientSideEncryptionTest(), "Requires AWS KMS credentials");
        assumeTrue(caFile() != null, "Requires the proxy's CA file, e.g. $DRIVERS_TOOLS/.evergreen/x509gen/ca.pem");
        assumeProxyIsRunning(true);
        resetMetrics(true);

        // Two independent TLS layers are in play here: the driver's connection to the proxy, verified against the
        // proxy's CA, and its connection to the KMS host, carried end-to-end through the CONNECT tunnel and verified
        // against the real KMS host's certificate. Creating the data key confirms the driver verified the KMS host's
        // identity rather than the proxy's.
        try (ClientEncryption clientEncryption = createClientEncryption(clientEncryptionSettingsBuilder()
                .kmsConnectCallback(proxyConnectCallback(true))
                .build())) {
            assertNotNull(createDataKey(clientEncryption));
        }

        assertTrue(getConnectCount(true) >= 1, "expected the KMS request to be routed through the proxy");
    }

    @Test
    @DisplayName("Case 3: full auto encryption pipeline via proxy")
    void testAutoEncryptionPipelineViaProxy() throws IOException {
        assumeTrue(isClientSideEncryptionTest(), "Requires AWS KMS credentials");
        assumeProxyIsRunning(false);

        try (MongoClient client = createMongoClient(getMongoClientSettingsBuilder().build())) {
            client.getDatabase("keyvault").getCollection("datakeys").drop();
            client.getDatabase("db").getCollection("coll").drop();

            BsonBinary dataKeyId;
            try (ClientEncryption clientEncryption = createClientEncryption(clientEncryptionSettingsBuilder()
                    .kmsConnectCallback(proxyConnectCallback(false))
                    .build())) {
                dataKeyId = createDataKey(clientEncryption);
                assertNotNull(dataKeyId);
            }

            Map<String, BsonDocument> schemaMap = new HashMap<>();
            schemaMap.put("db.coll", schemaForDataKey(dataKeyId));

            resetMetrics(false);

            AutoEncryptionSettings autoEncryptionSettings = AutoEncryptionSettings.builder()
                    .keyVaultNamespace(KEY_VAULT_NAMESPACE)
                    .kmsProviders(awsKmsProviders())
                    .schemaMap(schemaMap)
                    .kmsConnectCallback(proxyConnectCallback(false))
                    .build();

            try (MongoClient encryptedClient = createMongoClient(getMongoClientSettingsBuilder()
                    .autoEncryptionSettings(autoEncryptionSettings)
                    .build())) {
                MongoCollection<Document> encryptedColl = encryptedClient.getDatabase("db").getCollection("coll");
                encryptedColl.insertOne(new Document("_id", 1).append("encrypted_string", "hello"));

                Document decrypted = encryptedColl.find(Filters.eq("_id", 1)).first();
                assertNotNull(decrypted);
                assertEquals("hello", decrypted.get("encrypted_string"));
            }

            // read with the unencrypted client to confirm the value is stored encrypted
            Document stored = client.getDatabase("db").getCollection("coll").find(Filters.eq("_id", 1)).first();
            assertNotNull(stored);
            assertInstanceOf(org.bson.types.Binary.class, stored.get("encrypted_string"));
        }

        // Exactly one KMS request is expected since the decrypted key is cached; more than one would indicate a
        // data encryption key caching regression.
        assertEquals(1, getConnectCount(false), "expected exactly one KMS request through the proxy");
    }

    @Test
    @DisplayName("Case 4: Error")
    void testCallbackError() {
        // The callback fails before any KMS request is made, so no real credentials are needed and this case runs
        // wherever a MongoDB deployment is available.
        String failureMessage = "Proxy refused the CONNECT request";
        KmsConnectCallback failingCallback = context -> {
            throw new IOException(failureMessage);
        };

        try (ClientEncryption clientEncryption = createClientEncryption(clientEncryptionSettingsBuilder()
                .kmsProviders(placeholderAwsKmsProviders())
                .kmsConnectCallback(failingCallback)
                .build())) {
            RuntimeException e = assertThrows(RuntimeException.class, () -> createDataKey(clientEncryption));
            assertTrue(causeChainContains(e, failureMessage),
                    () -> "expected the callback's failure to be reported, but got: " + e);
        }
    }

    // Case 5 (callback receives timeout) is omitted because the callback is not given a deadline: the driver applies
    // the operation's remaining time to its own connect and TLS handshake instead of exposing it to the callback.

    /**
     * Returns a callback that tunnels to the KMS host through the proxy using HTTP CONNECT, as described in the Setup
     * section of prose test 28:
     * <ol>
     *     <li>Accept {@code (host, port)} from the driver.</li>
     *     <li>Open a connection to the proxy — plain TCP on port 9004, or TLS verified with {@code ca.pem} on 9005.</li>
     *     <li>Send {@code CONNECT <host>:<port> HTTP/1.1\r\nHost: <host>:<port>\r\n\r\n}.</li>
     *     <li>Read the response and verify it begins with {@code HTTP/1.1 200}.</li>
     *     <li>Return the socket, over which the driver negotiates TLS with the KMS host.</li>
     * </ol>
     */
    private KmsConnectCallback proxyConnectCallback(final boolean useTls) {
        return context -> {
            // The context carries no deadline, so the callback chooses its own timeout for reaching the proxy.
            int timeout = PROXY_TIMEOUT_MILLIS;
            Socket socket = useTls
                    ? proxySslContext().getSocketFactory().createSocket()
                    : new Socket();
            try {
                socket.connect(new InetSocketAddress(PROXY_HOST, useTls ? HTTPS_PROXY_PORT : HTTP_PROXY_PORT), timeout);
                socket.setSoTimeout(timeout);
                String target = context.getHost() + ":" + context.getPort();
                socket.getOutputStream().write(("CONNECT " + target + " HTTP/1.1\r\nHost: " + target + "\r\n\r\n")
                        .getBytes(StandardCharsets.US_ASCII));
                socket.getOutputStream().flush();
                // One byte at a time, so that no byte of the driver's TLS handshake is consumed.
                StringBuilder headers = new StringBuilder();
                while (!headers.toString().endsWith("\r\n\r\n")) {
                    int b = socket.getInputStream().read();
                    if (b == -1) {
                        throw new IOException("Proxy closed the connection before completing CONNECT: " + headers);
                    }
                    headers.append((char) b);
                }
                String statusLine = headers.substring(0, headers.indexOf("\r\n"));
                if (!statusLine.startsWith("HTTP/1.1 200")) {
                    throw new IOException("Proxy refused the CONNECT request: " + statusLine);
                }
                return socket;
            } catch (IOException | RuntimeException e) {
                socket.close();
                throw e;
            }
        };
    }

    private static boolean causeChainContains(final Throwable throwable, final String message) {
        for (Throwable t = throwable; t != null; t = t.getCause()) {
            if (t.getMessage() != null && t.getMessage().contains(message)) {
                return true;
            }
        }
        return false;
    }

    private ClientEncryptionSettings.Builder clientEncryptionSettingsBuilder() {
        return ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(getMongoClientSettingsBuilder().build())
                .keyVaultNamespace(KEY_VAULT_NAMESPACE)
                .kmsProviders(awsKmsProviders());
    }

    /**
     * Credentials that are never used, for the case whose callback fails before any KMS request is made.
     */
    private static Map<String, Map<String, Object>> placeholderAwsKmsProviders() {
        Map<String, Object> aws = new HashMap<>();
        aws.put("accessKeyId", "fakeAccessKeyId");
        aws.put("secretAccessKey", "fakeSecretAccessKey");
        Map<String, Map<String, Object>> kmsProviders = new HashMap<>();
        kmsProviders.put("aws", aws);
        return kmsProviders;
    }

    private static Map<String, Map<String, Object>> awsKmsProviders() {
        Map<String, Object> aws = new HashMap<>();
        aws.put("accessKeyId", System.getenv("AWS_ACCESS_KEY_ID"));
        aws.put("secretAccessKey", System.getenv("AWS_SECRET_ACCESS_KEY"));
        Map<String, Map<String, Object>> kmsProviders = new HashMap<>();
        kmsProviders.put("aws", aws);
        return kmsProviders;
    }

    private static BsonBinary createDataKey(final ClientEncryption clientEncryption) {
        return clientEncryption.createDataKey("aws", new DataKeyOptions().masterKey(BsonDocument.parse(MASTER_KEY)));
    }

    private static BsonDocument schemaForDataKey(final BsonBinary dataKeyId) {
        String base64DataKeyId = Base64.getEncoder().encodeToString(dataKeyId.getData());
        return BsonDocument.parse("{"
                + "  bsonType: \"object\","
                + "  properties: {"
                + "    encrypted_string: {"
                + "      encrypt: {"
                + "        keyId: [{\"$binary\": {\"base64\": \"" + base64DataKeyId + "\", \"subType\": \"04\"}}],"
                + "        bsonType: \"string\","
                + "        algorithm: \"AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic\""
                + "      }"
                + "    }"
                + "  }"
                + "}");
    }

    // --- the proxy's control endpoints -------------------------------------------------------------------------

    private void assumeProxyIsRunning(final boolean useTls) {
        try {
            getConnectCount(useTls);
        } catch (IOException e) {
            assumeTrue(false, "KMS HTTP proxy is not running on port "
                    + (useTls ? HTTPS_PROXY_PORT : HTTP_PROXY_PORT) + ": " + e.getMessage());
        }
    }

    private void resetMetrics(final boolean useTls) throws IOException {
        readControlResponse("/reset", "POST", useTls);
    }

    private int getConnectCount(final boolean useTls) throws IOException {
        String body = readControlResponse("/metrics", "GET", useTls);
        Matcher matcher = CONNECT_COUNT_PATTERN.matcher(body);
        if (!matcher.find()) {
            throw new IOException("Could not find connect_count in the proxy's metrics response: " + body);
        }
        return Integer.parseInt(matcher.group(1));
    }

    private String readControlResponse(final String path, final String method, final boolean useTls) throws IOException {
        int port = useTls ? HTTPS_PROXY_PORT : HTTP_PROXY_PORT;
        URL url = new URL((useTls ? "https" : "http") + "://" + PROXY_HOST + ":" + port + path);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        if (useTls) {
            HttpsURLConnection httpsConnection = (HttpsURLConnection) connection;
            httpsConnection.setSSLSocketFactory(proxySslContext().getSocketFactory());
            // The proxy's certificate is verified against its CA above. Its subject does not necessarily match the
            // loopback address that the control endpoints are reached on, which is immaterial for these tests.
            httpsConnection.setHostnameVerifier((hostname, session) -> PROXY_HOST.equals(hostname));
        }
        connection.setRequestMethod(method);
        connection.setConnectTimeout(5000);
        connection.setReadTimeout(5000);
        try (InputStream inputStream = connection.getInputStream()) {
            ByteArrayOutputStream body = new ByteArrayOutputStream();
            byte[] buffer = new byte[512];
            int read;
            while ((read = inputStream.read(buffer)) != -1) {
                body.write(buffer, 0, read);
            }
            return new String(body.toByteArray(), StandardCharsets.UTF_8);
        } finally {
            connection.disconnect();
        }
    }

    // --- the proxy's CA ----------------------------------------------------------------------------------------

    private static SSLContext proxySslContext;

    private static SSLContext proxySslContext() {
        if (proxySslContext == null) {
            proxySslContext = buildProxySslContext();
        }
        return proxySslContext;
    }

    private static SSLContext buildProxySslContext() {
        String caFile = caFile();
        assertNotNull(caFile, "the proxy's CA file could not be located");
        try (InputStream caStream = Files.newInputStream(Paths.get(caFile))) {
            X509Certificate ca = (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(caStream);
            KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
            trustStore.load(null, null);
            trustStore.setCertificateEntry("csfle-proxy-ca", ca);
            TrustManagerFactory trustManagerFactory =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(trustStore);
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, trustManagerFactory.getTrustManagers(), null);
            return sslContext;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Could not build an SSLContext trusting the proxy's CA", e);
        }
    }

    /**
     * @return the path to the CA certificate that signed the HTTPS proxy's certificate, or null if it cannot be found,
     * in which case the HTTPS proxy case is skipped.
     */
    @Nullable
    private static String caFile() {
        String caFile = System.getProperty("org.mongodb.test.csfle.tls.ca.file");
        if (caFile == null) {
            caFile = System.getenv("CSFLE_TLS_CA_FILE");
        }
        if (caFile == null) {
            String driversTools = System.getenv("DRIVERS_TOOLS");
            if (driversTools != null) {
                caFile = driversTools + "/.evergreen/x509gen/ca.pem";
            }
        }
        return caFile != null && new File(caFile).isFile() ? caFile : null;
    }
}
