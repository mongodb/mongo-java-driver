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
import com.mongodb.MongoClientException;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.assertions.Assertions;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.AbstractClientSideEncryptionNotCreateMongocryptdClientTest.findAvailableMongocryptdLoopbackPort;
import static com.mongodb.client.AbstractClientSideEncryptionTest.cryptSharedLibPathSysPropValue;
import static com.mongodb.client.Fixture.getMongoClientSettings;
import static com.mongodb.client.unified.UnifiedClientEncryptionHelper.localKmsProviderKey;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static util.JsonPoweredTestHelper.getTestDocument;

/**
 * See
 * <a href="https://github.com/mongodb/specifications/tree/master/source/client-side-encryption/tests#bypass-spawning-mongocryptd">
 * 8. Bypass Spawning mongocryptd</a>.
 */
public abstract class AbstractClientSideEncryptionNotSpawnMongocryptdTest {
    @Nullable
    private static final String CRYPT_SHARED_LIB_PATH_SYS_PROP_VALUE = cryptSharedLibPathSysPropValue().orElse(null);
    private static final BsonDocument EXTERNAL_SCHEMA = externalSchema();
    private static final BsonDocument EXTERNAL_KEY = externalKey();
    private static final String LOCAL_KMS_PROVIDER_ID = "local";
    private static final Duration TIMEOUT = Duration.ofMillis(1_000);
    private static final MongoNamespace KEY_VAULT_NAMESPACE = new MongoNamespace("keyvault", "datakeys");
    private static final MongoNamespace NAMESPACE = new MongoNamespace("db", "coll");

    private MongoClient client;
    private InetSocketAddress mongocryptdSocketAddress;

    @BeforeEach
    public void setUp() {
        assumeTrue(serverVersionAtLeast(4, 2));
    }

    @AfterEach
    @SuppressWarnings("try")
    public void cleanUp() {
        mongocryptdSocketAddress = null;
        if (client != null) {
            client.close();
        }
    }

    protected abstract MongoClient createMongoClient(MongoClientSettings settings);

    /**
     * See
     * <a href="https://github.com/mongodb/specifications/tree/master/source/client-side-encryption/tests#via-loading-shared-library">
     * Via loading shared library</a>.
     */
    @Test
    void viaLoadingSharedLibrary() {
        assumeTrue(CRYPT_SHARED_LIB_PATH_SYS_PROP_VALUE != null);
        setUpCollection((mongocryptdSocketAddress, autoEncryptionSettingsBuilder) ->
                autoEncryptionSettingsBuilder.extraOptions(merge(commonExtraAutoEncryptionOptions(mongocryptdSocketAddress),
                        new SimpleImmutableEntry<>("cryptSharedLibPath", CRYPT_SHARED_LIB_PATH_SYS_PROP_VALUE),
                        new SimpleImmutableEntry<>("cryptSharedLibRequired", true),
                        new SimpleImmutableEntry<>("mongocryptdURI", format("mongodb://%s:%d/db?serverSelectionTimeoutMS=%d",
                                mongocryptdSocketAddress.getAddress().getHostAddress(),
                                mongocryptdSocketAddress.getPort(),
                                TIMEOUT.toMillis()))
                ))
        ).insertOne(Document.parse("{unencrypted: 'test'}"));
        assertMongocryptdNotSpawned();
    }

    /**
     * See
     * <a href="https://github.com/mongodb/specifications/tree/master/source/client-side-encryption/tests#via-mongocryptdbypassspawn">
     * Via mongocryptdBypassSpawn</a>.
     */
    @Test
    void viaMongocryptdBypassSpawn() {
        assumeFalse(CRYPT_SHARED_LIB_PATH_SYS_PROP_VALUE != null);
        MongoCollection<Document> collection = setUpCollection((mongocryptdSocketAddress, autoEncryptionSettingsBuilder) ->
                autoEncryptionSettingsBuilder.extraOptions(merge(commonExtraAutoEncryptionOptions(mongocryptdSocketAddress),
                        new SimpleImmutableEntry<>("mongocryptdBypassSpawn", true),
                        new SimpleImmutableEntry<>("mongocryptdURI", format("mongodb://%s:%d/db?serverSelectionTimeoutMS=%d",
                                mongocryptdSocketAddress.getAddress().getHostAddress(),
                                mongocryptdSocketAddress.getPort(),
                                TIMEOUT.toMillis()))
                ))
        );
        assertTrue(assertThrows(MongoClientException.class,
                () -> collection.insertOne(Document.parse("{encrypted: 'test'}"))).getMessage().contains("Timed out"));
    }

    /**
     * See
     * <a href="https://github.com/mongodb/specifications/tree/master/source/client-side-encryption/tests#via-bypassautoencryption">
     * Via bypassAutoEncryption</a>.
     */
    @Test
    void viaBypassAutoEncryption() {
        assumeFalse(CRYPT_SHARED_LIB_PATH_SYS_PROP_VALUE != null);
        setUpCollection((mongocryptdSocketAddress, autoEncryptionSettingsBuilder) -> autoEncryptionSettingsBuilder
                .extraOptions(commonExtraAutoEncryptionOptions(mongocryptdSocketAddress))
                .bypassAutoEncryption(true)
        ).insertOne(Document.parse("{unencrypted: 'test'}"));
        assertMongocryptdNotSpawned();
    }

    /**
     * See
     * <a href="https://github.com/mongodb/specifications/tree/master/source/client-side-encryption/tests#via-bypassqueryanalysis">
     * Via bypassQueryAnalysis</a>.
     */
    @Test
    void viaBypassQueryAnalysis() {
        assumeFalse(CRYPT_SHARED_LIB_PATH_SYS_PROP_VALUE != null);
        setUpCollection((mongocryptdSocketAddress, autoEncryptionSettingsBuilder) -> autoEncryptionSettingsBuilder
                .extraOptions(commonExtraAutoEncryptionOptions(mongocryptdSocketAddress))
                .bypassQueryAnalysis(true)
        ).insertOne(Document.parse("{unencrypted: 'test'}"));
        assertMongocryptdNotSpawned();
    }

    private MongoCollection<Document> setUpCollection(
            final BiConsumer<InetSocketAddress, AutoEncryptionSettings.Builder> autoEncryptionSettingsBuilderMutator) {
        setUpKeyVaultNamespace();
        InetSocketAddress localMongocryptdSocketAddress = new InetSocketAddress(
                InetAddress.getLoopbackAddress(), findAvailableMongocryptdLoopbackPort());
        AutoEncryptionSettings.Builder autoEncryptionSettingsBuilder = AutoEncryptionSettings.builder();
        autoEncryptionSettingsBuilderMutator.accept(localMongocryptdSocketAddress, autoEncryptionSettingsBuilder);
        mongocryptdSocketAddress = localMongocryptdSocketAddress;
        client = createMongoClient(MongoClientSettings.builder(getMongoClientSettings())
                .autoEncryptionSettings(autoEncryptionSettingsBuilder
                        .kmsProviders(singletonMap(LOCAL_KMS_PROVIDER_ID, singletonMap("key", localKmsProviderKey())))
                        .keyVaultNamespace(KEY_VAULT_NAMESPACE.getFullName())
                        .schemaMap(singletonMap(NAMESPACE.getFullName(), EXTERNAL_SCHEMA))
                        .build())
                .build());
        MongoDatabase db = client.getDatabase(NAMESPACE.getDatabaseName());
        db.drop();
        return db.getCollection(NAMESPACE.getCollectionName());
    }

    private void setUpKeyVaultNamespace() {
        try (MongoClient client = createMongoClient(MongoClientSettings.builder(getMongoClientSettings()).build())) {
            MongoDatabase db = client.getDatabase(KEY_VAULT_NAMESPACE.getDatabaseName());
            db.drop();
            db.getCollection(KEY_VAULT_NAMESPACE.getCollectionName(), BsonDocument.class)
                    .withWriteConcern(WriteConcern.MAJORITY)
                    .insertOne(EXTERNAL_KEY);
        }
    }

    private static Map<String, Object> commonExtraAutoEncryptionOptions(final InetSocketAddress mongocryptdSocketAddress) {
        return singletonMap("mongocryptdSpawnArgs", asList(
                // We pick a random available `mongocryptd` port and also include it in the PID file path
                // to reduce the chances of different test runs interfering with each other. The interference
                // may come from the fact that once spawned, `mongocryptd` stays up and running for some time,
                // which may cause failures in other runs if they use the same `mongocryptd` port / PID file.
                format("--pidfilepath=bypass-spawning-mongocryptd-%d.pid", mongocryptdSocketAddress.getPort()),
                format("--port=%d", mongocryptdSocketAddress.getPort())));
    }

    private void assertMongocryptdNotSpawned() {
        try (MongoClient mongocryptdClient = createMongoClient(MongoClientSettings.builder()
                .applyToClusterSettings(builder -> builder
                        .hosts(singletonList(new ServerAddress(mongocryptdSocketAddress)))
                        .serverSelectionTimeout(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .build())) {
            assertThrows(MongoTimeoutException.class, () -> mongocryptdClient.getDatabase(NAMESPACE.getDatabaseName())
                    .runCommand(Document.parse("{hello: 1}")),
                    "If nothing is thrown, then we connected to mongocryptd, i.e., it was spawned");
        }
    }

    private static BsonDocument externalSchema() {
        try {
            return getTestDocument(new File(Assertions.assertNotNull(AbstractClientSideEncryptionNotSpawnMongocryptdTest.class
                    .getResource("/client-side-encryption-external/external-schema.json")).toURI()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static BsonDocument externalKey() {
        try {
            return getTestDocument(new File(Assertions.assertNotNull(AbstractClientSideEncryptionNotSpawnMongocryptdTest.class
                    .getResource("/client-side-encryption-external/external-key.json")).toURI()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    private static <K, V> Map<K, V> merge(final Map<K, V> map, final Entry<K, V>... entries) {
        HashMap<K, V> result = new HashMap<>(map);
        result.putAll(Stream.of(entries).collect(Collectors.toMap(Entry::getKey, Entry::getValue)));
        return result;
    }
}
