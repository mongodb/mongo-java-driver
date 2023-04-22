/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */

package com.mongodb.client;

import com.mongodb.ClientEncryptionSettings;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoConfigurationException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoWriteException;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.CreateEncryptedCollectionParams;
import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.client.vault.ClientEncryption;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Base64;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.isServerlessTest;
import static com.mongodb.ClusterFixture.isStandalone;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.Fixture.getMongoClientSettings;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.StreamSupport.stream;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.params.ParameterizedTest.DISPLAY_NAME_PLACEHOLDER;

/**
 * See <a href="https://github.com/mongodb/specifications/tree/master/source/client-side-encryption/tests#automatic-data-encryption-keys">
 * 21. Automatic Data Encryption Keys</a>.
 */
public abstract class AbstractClientSideEncryptionAutoDataKeysTest {
    private static final String COLL_NAME = "testing1";
    private static final MongoNamespace KEY_VAULT_NAMESPACE = new MongoNamespace("keyvault", "datakeys");

    private MongoClient client;
    private MongoDatabase db;
    private ClientEncryption clientEncryption;

    @BeforeEach
    public void setUp() {
        assumeTrue(serverVersionAtLeast(7, 0));
        assumeFalse(isStandalone());
        assumeFalse(isServerlessTest());

        client = createMongoClient(getMongoClientSettings());
        Set<KmsProvider> kmsProviders = KmsProvider.detect();
        clientEncryption = createClientEncryption(ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(getMongoClientSettings())
                .keyVaultNamespace(KEY_VAULT_NAMESPACE.getFullName())
                .kmsProviders(kmsProviders.stream().collect(toMap(
                        provider -> provider.name, provider -> emptyMap())))
                .kmsProviderPropertySuppliers(kmsProviders.stream().collect(toMap(
                        provider -> provider.name, provider -> provider.propertiesSupplier)))
                .build());
        client.getDatabase(KEY_VAULT_NAMESPACE.getDatabaseName()).drop();
        db = client.getDatabase("autoDataKeysTest");
        db.drop();
    }

    @AfterEach
    @SuppressWarnings("try")
    public void cleanUp() {
        try (ClientEncryption ignored = clientEncryption;
             MongoClient ignored1 = client) {
            // empty
        }
    }

    /**
     * See
     * <a href="https://github.com/mongodb/specifications/tree/master/source/client-side-encryption/tests#case-1-simple-creation-and-validation">
     * Case 1: Simple Creation and Validation</a>.
     */
    @ParameterizedTest(name = DISPLAY_NAME_PLACEHOLDER + " {0}")
    @MethodSource("arguments")
    void simpleCreationAndValidation(final KmsProvider kmsProvider) {
        CreateCollectionOptions createCollectionOptions = new CreateCollectionOptions().encryptedFields(Document.parse(
                "{"
                + "  fields: [{"
                + "    path: 'ssn',"
                + "    bsonType: 'string',"
                + "    keyId: null"
                + "  }]"
                + "}"));
        clientEncryption.createEncryptedCollection(db, COLL_NAME, createCollectionOptions,
                kmsProvider.createEncryptedCollectionParamsSupplier.get());
        MongoCollection<Document> coll = db.getCollection(COLL_NAME);
        assertEquals(
                121, // DocumentValidationFailure
                assertThrows(MongoWriteException.class, () -> coll.insertOne(Document.parse("{ ssn: '123-45-6789' }")))
                        .getCode());
    }

    /**
     * See
     * <a href="https://github.com/mongodb/specifications/tree/master/source/client-side-encryption/tests#case-2-missing-encryptedfields">
     * Case 2: Missing encryptedFields</a>.
     */
    @ParameterizedTest(name = DISPLAY_NAME_PLACEHOLDER + " {0}")
    @MethodSource("arguments")
    void missingEncryptedFields(final KmsProvider kmsProvider) {
        assertThrows(MongoConfigurationException.class, () -> clientEncryption.createEncryptedCollection(
                db, COLL_NAME, new CreateCollectionOptions(), kmsProvider.createEncryptedCollectionParamsSupplier.get()));
        assertTrue(stream(db.listCollectionNames().spliterator(), false).noneMatch(name -> name.equals(COLL_NAME)));
    }

    /**
     * See
     * <a href="https://github.com/mongodb/specifications/tree/master/source/client-side-encryption/tests#case-3-invalid-keyid">
     * Case 3: Invalid keyId</a>.
     */
    @ParameterizedTest(name = DISPLAY_NAME_PLACEHOLDER + " {0}")
    @MethodSource("arguments")
    void invalidKeyId(final KmsProvider kmsProvider) {
        CreateCollectionOptions createCollectionOptions = new CreateCollectionOptions().encryptedFields(Document.parse(
                "{"
                + "  fields: [{"
                + "    path: 'ssn',"
                + "    bsonType: 'string',"
                + "    keyId: false"
                + "  }]"
                + "}"));
        assertEquals(
                14, // TypeMismatch
                assertThrows(MongoCommandException.class, () -> clientEncryption.createEncryptedCollection(
                        db, COLL_NAME, createCollectionOptions, kmsProvider.createEncryptedCollectionParamsSupplier.get()))
                        .getCode());
    }

    /**
     * See
     * <a href="https://github.com/mongodb/specifications/tree/master/source/client-side-encryption/tests#case-4-insert-encrypted-value">
     * Case 4: Insert encrypted value</a>.
     */
    @ParameterizedTest(name = DISPLAY_NAME_PLACEHOLDER + " {0}")
    @MethodSource("arguments")
    void insertEncryptedValue(final KmsProvider kmsProvider) {
        CreateCollectionOptions createCollectionOptions = new CreateCollectionOptions().encryptedFields(Document.parse(
                "{"
                + "  fields: [{"
                + "    path: 'ssn',"
                + "    bsonType: 'string',"
                + "    keyId: null"
                + "  }]"
                + "}"));
        BsonDocument encryptedFields = clientEncryption.createEncryptedCollection(db, COLL_NAME, createCollectionOptions,
                kmsProvider.createEncryptedCollectionParamsSupplier.get());
        MongoCollection<Document> coll = db.getCollection(COLL_NAME);
        BsonBinary dataKeyId = encryptedFields.getArray("fields").get(0).asDocument().getBinary("keyId");
        BsonBinary encryptedValue = clientEncryption.encrypt(new BsonString("123-45-6789"),
                new EncryptOptions("Unindexed").keyId(dataKeyId));
        coll.insertOne(new Document("ssn", encryptedValue));
    }

    protected abstract MongoClient createMongoClient(MongoClientSettings settings);

    protected abstract ClientEncryption createClientEncryption(ClientEncryptionSettings settings);

    private static Stream<Arguments> arguments() {
        return KmsProvider.detect().stream().map(Arguments::of);
    }

    private enum KmsProvider {
        LOCAL("local",
                kmsProviderProperties -> kmsProviderProperties.put("key", Base64.getDecoder().decode(
                        "Mng0NCt4ZHVUYUJCa1kxNkVyNUR1QURhZ2h2UzR2d2RrZzh0cFBwM3R6NmdWMDFBMUN3YkQ5aXRRMkhGRGdQV09wOGVNYUMxT2k3NjZKelhaQmRCZ"
                                + "GJkTXVyZG9uSjFk")),
                createEncryptedCollectionParams -> {}
        ),
        AWS("aws",
                kmsProviderProperties -> {
                    kmsProviderProperties.put("accessKeyId", System.getProperty("org.mongodb.test.awsAccessKeyId"));
                    kmsProviderProperties.put("secretAccessKey", System.getProperty("org.mongodb.test.awsSecretAccessKey"));
                },
                createEncryptedCollectionParams -> createEncryptedCollectionParams.masterKey(BsonDocument.parse(
                        "{"
                        + "  region: 'us-east-1',"
                        + "  key: 'arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0'"
                        + "}"))
        );

        private final String name;
        private final Supplier<Map<String, Object>> propertiesSupplier;
        private final Supplier<CreateEncryptedCollectionParams> createEncryptedCollectionParamsSupplier;

        private static Set<KmsProvider> detect() {
            String awsAccessKeyId = System.getProperty("org.mongodb.test.awsAccessKeyId");
            return awsAccessKeyId != null && !awsAccessKeyId.isEmpty()
                    ? EnumSet.allOf(KmsProvider.class)
                    : EnumSet.of(KmsProvider.LOCAL);
        }

        KmsProvider(final String name, final Consumer<Map<String, Object>> propertiesUpdater,
                final Consumer<CreateEncryptedCollectionParams> encryptedCollectionParamsUpdater) {
            this.name = name;
            this.propertiesSupplier = () -> {
                Map<String, Object> result = new HashMap<>();
                propertiesUpdater.accept(result);
                return result;
            };
            this.createEncryptedCollectionParamsSupplier = () -> {
                CreateEncryptedCollectionParams result = new CreateEncryptedCollectionParams(name);
                encryptedCollectionParamsUpdater.accept(result);
                return result;
            };
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
