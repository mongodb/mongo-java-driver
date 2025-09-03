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
import com.mongodb.MongoConfigurationException;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.crypt.capi.MongoCryptException;
import com.mongodb.lang.NonNull;
import com.mongodb.lang.Nullable;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.getEnv;
import static com.mongodb.ClusterFixture.isClientSideEncryptionTest;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public abstract class AbstractClientSideEncryptionAwsCredentialFromEnvironmentTest {

    private static final String MASTER_KEY = "{"
            + "region: \"us-east-1\", "
            + "key: \"arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0\"}";

    protected abstract ClientEncryption createClientEncryption(ClientEncryptionSettings settings);

    protected abstract MongoClient createMongoClient(MongoClientSettings settings);

    @Test
    public void testGetCredentialsFromEnvironment() {
        assumeTrue(System.getenv().containsKey("AWS_ACCESS_KEY_ID"));

        Map<String, Map<String, Object>> kmsProviders = new HashMap<String, Map<String, Object>>() {{
            put("aws", new HashMap<>());
        }};

        try (ClientEncryption clientEncryption = createClientEncryption(ClientEncryptionSettings.builder()
                .keyVaultNamespace("test.datakeys")
                .kmsProviders(kmsProviders)
                .keyVaultMongoClientSettings(Fixture.getMongoClientSettings())
                .build())) {

            // If this succeeds, then it means credentials have been fetched from the environment as expected
            BsonBinary dataKeyId = clientEncryption.createDataKey("aws", new DataKeyOptions().masterKey(
                    BsonDocument.parse(MASTER_KEY)));

            String base64DataKeyId = Base64.getEncoder().encodeToString(dataKeyId.getData());

            Map<String, BsonDocument> schemaMap = new HashMap<>();
            schemaMap.put("test.coll", getSchema(base64DataKeyId));
            AutoEncryptionSettings autoEncryptionSettings = AutoEncryptionSettings.builder()
                    .kmsProviders(kmsProviders)
                    .keyVaultNamespace("test.datakeys")
                    .schemaMap(schemaMap)
                    .build();
            try (MongoClient client = createMongoClient(getMongoClientSettingsBuilder()
                    .autoEncryptionSettings(autoEncryptionSettings)
                    .build())) {
                // If this succeeds, then it means credentials have been fetched from the environment as expected
                client.getDatabase("test").getCollection("coll")
                        .insertOne(new Document("encryptedField", "encryptMe"));
            }
        }
    }
    @Test
    public void testGetCredentialsFromSupplier() {
        assumeFalse(System.getenv().containsKey("AWS_ACCESS_KEY_ID"));
        assumeTrue(isClientSideEncryptionTest());

        Map<String, Map<String, Object>> kmsProviders = new HashMap<String, Map<String, Object>>() {{
            put("aws", new HashMap<>());
        }};

        Map<String, Supplier<Map<String, Object>>> kmsProviderPropertySuppliers = new HashMap<String, Supplier<Map<String, Object>>>() {{
            put("aws", () -> new HashMap<String, Object>() {{
                put("accessKeyId", getEnv("AWS_ACCESS_KEY_ID"));
                put("secretAccessKey", getEnv("AWS_SECRET_ACCESS_KEY"));
            }});
        }};

        try (ClientEncryption clientEncryption = createClientEncryption(ClientEncryptionSettings.builder()
                .keyVaultNamespace("test.datakeys")
                .kmsProviders(kmsProviders)
                .kmsProviderPropertySuppliers(kmsProviderPropertySuppliers)
                .keyVaultMongoClientSettings(Fixture.getMongoClientSettings())
                .build())) {

            // If this succeeds, then it means credentials have been fetched from the supplier as expected
            BsonBinary dataKeyId = clientEncryption.createDataKey("aws", new DataKeyOptions().masterKey(
                    BsonDocument.parse(MASTER_KEY)));

            String base64DataKeyId = Base64.getEncoder().encodeToString(dataKeyId.getData());

            Map<String, BsonDocument> schemaMap = new HashMap<>();
            schemaMap.put("test.coll", getSchema(base64DataKeyId));
            AutoEncryptionSettings autoEncryptionSettings = AutoEncryptionSettings.builder()
                    .kmsProviders(kmsProviders)
                    .kmsProviderPropertySuppliers(kmsProviderPropertySuppliers)
                    .keyVaultNamespace("test.datakeys")
                    .schemaMap(schemaMap)
                    .build();
            try (MongoClient client = createMongoClient(getMongoClientSettingsBuilder()
                    .autoEncryptionSettings(autoEncryptionSettings)
                    .build())) {
                // If this succeeds, then it means credentials have been fetched from the supplier as expected
                client.getDatabase("test").getCollection("coll")
                        .insertOne(new Document("encryptedField", "encryptMe"));
            }
        }
    }

    public static Stream<Arguments> createUnexpectedSupplierArguments() {
        return Stream.of(
                Arguments.of("ThrowsAnException", (Supplier<Map<String, Object>>) () -> {
                    throw new RuntimeException();
                }, "", RuntimeException.class),
                Arguments.of("ReturnsNull", (Supplier<Map<String, Object>>) () -> null, " The returned value is null.", null),
                Arguments.of("ReturnsEmptyMap", (Supplier<Map<String, Object>>) Collections::emptyMap, " The returned value is empty.",
                        null)
        );
    }

    @ParameterizedTest(name = "shouldThrowMongoConfigurationIfSupplier{0}")
    @MethodSource("createUnexpectedSupplierArguments")
    public void shouldThrowMongoConfigurationIfSupplierReturnsDoesSomethingUnexpected(final String testNameSuffix,
            final Supplier<Map<String, Object>> awsProviderPropertySupplier, final String exceptionMessageSuffix,
            @Nullable final Class<?> exceptionCauseType) {
        assumeFalse(System.getenv().containsKey("AWS_ACCESS_KEY_ID"));
        assumeTrue(isClientSideEncryptionTest());

        Map<String, Map<String, Object>> kmsProviders = new HashMap<String, Map<String, Object>>() {{
            put("aws", new HashMap<>());
        }};

        Map<String, Supplier<Map<String, Object>>> kmsProviderPropertySuppliers = new HashMap<String, Supplier<Map<String, Object>>>() {{
            put("aws", awsProviderPropertySupplier);
        }};

        try (ClientEncryption clientEncryption = createClientEncryption(ClientEncryptionSettings.builder()
                .keyVaultNamespace("test.datakeys")
                .kmsProviders(kmsProviders)
                .kmsProviderPropertySuppliers(kmsProviderPropertySuppliers)
                .keyVaultMongoClientSettings(Fixture.getMongoClientSettings())
                .build())) {
            MongoConfigurationException e = assertThrows(MongoConfigurationException.class, () ->
                    clientEncryption.createDataKey("aws", new DataKeyOptions().masterKey(
                            BsonDocument.parse(MASTER_KEY))));
            assertEquals("Exception getting credential for kms provider aws from configured Supplier."
                    + exceptionMessageSuffix, e.getMessage());
            if (exceptionCauseType == null) {
                assertNull(e.getCause());
            } else {
                assertEquals(exceptionCauseType, e.getCause().getClass());
            }
        }
    }


    /**
     * This is a custom prose tests to enhance coverage.
     * <p>
     * This test specifically verifies the following part of the specification:
     * <ul>
     *     <li>KMS providers that include a name (e.g., "aws:myname") do not support automatic credentials.</li>
     *     <li>Configuring a named KMS provider for automatic credentials will result in a runtime error from libmongocrypt.</li>
     * </ul>
     * <p>
     * Detailed specification reference:
     * <a href="https://github.com/mongodb/specifications/blob/master/source/client-side-encryption/client-side-encryption.md#automatic-credentials">Client-Side Encryption Spec</a>
     */
    @Test
    @DisplayName("Throw MongoCryptException when configured for automatic/on-demand credentials in ClientEncryptionSettings")
    void shouldThrowMongoCryptExceptionWhenNamedKMSProviderUsesEmptyOnDemandCredentialsWithEncryptionSettings() {
        assumeTrue(isClientSideEncryptionTest());

        Map<String, Map<String, Object>> kmsProviders = new HashMap<String, Map<String, Object>>() {{
            put("aws:name", new HashMap<>());
        }};

        Map<String, Supplier<Map<String, Object>>> kmsProviderPropertySuppliers = new HashMap<>();
        kmsProviderPropertySuppliers.put("aws:name", () -> Assertions.fail("Supplier should not be called"));

        ClientEncryptionSettings settings = ClientEncryptionSettings.builder()
                .keyVaultNamespace("test.datakeys")
                .kmsProviders(kmsProviders)
                .kmsProviderPropertySuppliers(kmsProviderPropertySuppliers)
                .keyVaultMongoClientSettings(Fixture.getMongoClientSettings())
                .build();

        MongoCryptException e = assertThrows(MongoCryptException.class, () -> {
            createClientEncryption(settings).close();
        });
        assertTrue(e.getMessage().contains("On-demand credentials are not supported for named KMS providers."));
    }

    /**
     * This is a custom prose tests to enhance coverage.
     * <p>
     * This test specifically verifies the following part of the specification:
     * <ul>
     *     <li>KMS providers that include a name (e.g., "aws:myname") do not support automatic credentials.</li>
     *     <li>Configuring a named KMS provider for automatic credentials will result in a runtime error from libmongocrypt.</li>
     * </ul>
     * <p>
     * Detailed specification reference:
     * <a href="https://github.com/mongodb/specifications/blob/master/source/client-side-encryption/client-side-encryption.md#automatic-credentials">Client-Side Encryption Spec</a>
     */
    @Test
    @DisplayName("Throw MongoCryptException when configured for automatic/on-demand credentials in AutoEncryptionSettings")
    public void shouldThrowMongoCryptExceptionWhenNamedKMSProviderUsesEmptyOnDemandCredentialsWithAutoEncryptionSettings() {
        assumeTrue(isClientSideEncryptionTest());

        Map<String, Map<String, Object>> kmsProviders = new HashMap<String, Map<String, Object>>() {{
            put("aws:name", new HashMap<>());
        }};

        Map<String, Supplier<Map<String, Object>>> kmsProviderPropertySuppliers = new HashMap<>();
        kmsProviderPropertySuppliers.put("aws:name", () -> Assertions.fail("Supplier should not be called"));

        AutoEncryptionSettings autoEncryptionSettings = AutoEncryptionSettings.builder()
                .kmsProviders(kmsProviders)
                .keyVaultNamespace("test.datakeys")
                .build();

        MongoCryptException e = assertThrows(MongoCryptException.class, () -> {
            createMongoClient(getMongoClientSettingsBuilder()
                    .autoEncryptionSettings(autoEncryptionSettings)
                    .build()).close();
        });
        assertTrue(e.getMessage().contains("On-demand credentials are not supported for named KMS providers."));
    }



    @Test
    public void shouldIgnoreSupplierIfKmsProviderMapValueIsNotEmpty() {
        assumeFalse(System.getenv().containsKey("AWS_ACCESS_KEY_ID"));
        assumeTrue(isClientSideEncryptionTest());

        Map<String, Map<String, Object>> kmsProviders = new HashMap<String, Map<String, Object>>() {{
            put("aws", new HashMap<String, Object>() {{
                put("accessKeyId", getEnv("AWS_ACCESS_KEY_ID"));
                put("secretAccessKey", getEnv("AWS_SECRET_ACCESS_KEY"));
            }});
        }};

        Map<String, Supplier<Map<String, Object>>> kmsProviderPropertySuppliers = new HashMap<String, Supplier<Map<String, Object>>>() {{
            put("aws", () -> null);  // if Supplier was actually used, an exception would be thrown because it's returning null
        }};

        try (ClientEncryption clientEncryption = createClientEncryption(ClientEncryptionSettings.builder()
                .keyVaultNamespace("test.datakeys")
                .kmsProviders(kmsProviders)
                .kmsProviderPropertySuppliers(kmsProviderPropertySuppliers)
                .keyVaultMongoClientSettings(Fixture.getMongoClientSettings())
                .build())) {
            assertDoesNotThrow(() ->
                    clientEncryption.createDataKey("aws", new DataKeyOptions().masterKey(BsonDocument.parse(MASTER_KEY))));
        }
    }

    @NonNull
    private static BsonDocument getSchema(final String base64DataKeyId) {
        return BsonDocument.parse("{"
                + "  properties: {"
                + "    encryptedField: {"
                + "      encrypt: {"
                + "        keyId: [{"
                + "          \"$binary\": {"
                + "            \"base64\": \"" + base64DataKeyId + "\","
                + "            \"subType\": \"04\""
                + "          }"
                + "        }],"
                + "        bsonType: \"string\","
                + "        algorithm: \"AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic\""
                + "      }"
                + "    }"
                + "  },"
                + "  \"bsonType\": \"object\""
                + "}");
    }
}
