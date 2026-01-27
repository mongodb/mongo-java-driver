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

package com.mongodb.client.csot;

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.ClientEncryptionSettings;
import com.mongodb.ClusterFixture;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.MongoUpdatedEncryptedFieldsException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.Fixture;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.CreateEncryptedCollectionParams;
import com.mongodb.client.model.ValidationOptions;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.client.test.CollectionHelper;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.internal.connection.TestCommandListener;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.codecs.BsonDocumentCodec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * See
 * <a href="https://github.com/mongodb/specifications/blob/master/source/client-side-operations-timeout/tests/README.md#3-clientencryption">Prose Tests</a>.
 */
public abstract class AbstractClientSideOperationsEncryptionTimeoutProseTest {

    protected static final String FAIL_COMMAND_NAME = "failCommand";
    private static final Map<String, Map<String, Object>> KMS_PROVIDERS = new HashMap<>();

    private final MongoNamespace keyVaultNamespace = new MongoNamespace("keyvault", "datakeys");

    private CollectionHelper<BsonDocument> keyVaultCollectionHelper;

    private TestCommandListener commandListener;

    private static final String MASTER_KEY = "Mng0NCt4ZHVUYUJCa1kxNkVyNUR1QURhZ2h2UzR2d2RrZzh0cFBwM3R6NmdWMDFBMUN3YkQ5a"
            + "XRRMkhGRGdQV09wOGVNYUMxT2k3NjZKelhaQmRCZGJkTXVyZG9uSjFk";

    protected abstract ClientEncryption createClientEncryption(ClientEncryptionSettings.Builder builder);

    protected abstract MongoClient createMongoClient(MongoClientSettings.Builder builder);

    @Test
    void shouldThrowOperationTimeoutExceptionWhenCreateDataKey() {
        assumeTrue(serverVersionAtLeast(4, 4));

        Map<String, Map<String, Object>> kmsProviders = new HashMap<>();
        Map<String, Object> localProviderMap = new HashMap<>();
        localProviderMap.put("key", Base64.getDecoder().decode(MASTER_KEY));
        kmsProviders.put("local", localProviderMap);

        try (ClientEncryption clientEncryption = createClientEncryption(getClientEncryptionSettingsBuilder(100))) {

            keyVaultCollectionHelper.runAdminCommand("{"
                    + "    configureFailPoint: \"" + FAIL_COMMAND_NAME + "\","
                    + "  mode: { times: 1 },"
                    + "  data: {"
                    + "    failCommands: [\"insert\"],"
                    + "    blockConnection: true,"
                    + "    blockTimeMS: " + 100
                    + "  }"
                    + "}");

            assertThrows(MongoOperationTimeoutException.class, () -> clientEncryption.createDataKey("local"));

            List<CommandStartedEvent> commandStartedEvents = commandListener.getCommandStartedEvents();
            assertEquals(1, commandStartedEvents.size());
            assertEquals(keyVaultNamespace.getCollectionName(),
                    commandStartedEvents.get(0).getCommand().get("insert").asString().getValue());
            assertNotNull(commandListener.getCommandFailedEvent("insert"));
        }

    }

    @Test
    void shouldThrowOperationTimeoutExceptionWhenEncryptData() {
        assumeTrue(serverVersionAtLeast(4, 4));

        try (ClientEncryption clientEncryption = createClientEncryption(getClientEncryptionSettingsBuilder(150))) {

            clientEncryption.createDataKey("local");

            keyVaultCollectionHelper.runAdminCommand("{"
                    + "    configureFailPoint: \"" + FAIL_COMMAND_NAME + "\","
                    + "  mode: { times: 1 },"
                    + "  data: {"
                    + "    failCommands: [\"find\"],"
                    + "    blockConnection: true,"
                    + "    blockTimeMS: " + 150
                    + "  }"
                    + "}");

            BsonBinary dataKey = clientEncryption.createDataKey("local");

            EncryptOptions encryptOptions = new EncryptOptions("AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic");
            encryptOptions.keyId(dataKey);
            commandListener.reset();
            assertThrows(MongoOperationTimeoutException.class, () -> clientEncryption.encrypt(new BsonString("hello"), encryptOptions));

            List<CommandStartedEvent> commandStartedEvents = commandListener.getCommandStartedEvents();
            assertEquals(1, commandStartedEvents.size());
            assertEquals(keyVaultNamespace.getCollectionName(), commandStartedEvents.get(0).getCommand().get("find").asString().getValue());
            assertNotNull(commandListener.getCommandFailedEvent("find"));
        }

    }

    @Test
    void shouldThrowOperationTimeoutExceptionWhenDecryptData() {
        assumeTrue(serverVersionAtLeast(4, 4));

        BsonBinary encrypted;
        try (ClientEncryption clientEncryption = createClientEncryption(getClientEncryptionSettingsBuilder(400))) {
            clientEncryption.createDataKey("local");
            BsonBinary dataKey = clientEncryption.createDataKey("local");
            EncryptOptions encryptOptions = new EncryptOptions("AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic");
            encryptOptions.keyId(dataKey);
            encrypted = clientEncryption.encrypt(new BsonString("hello"), encryptOptions);
        }

        try (ClientEncryption clientEncryption = createClientEncryption(getClientEncryptionSettingsBuilder(400))) {
            keyVaultCollectionHelper.runAdminCommand("{"
                    + "  configureFailPoint: \"" + FAIL_COMMAND_NAME + "\","
                    + "  mode: { times: 1 },"
                    + "  data: {"
                    + "    failCommands: [\"find\"],"
                    + "    blockConnection: true,"
                    + "    blockTimeMS: " + 500
                    + "  }"
                    + "}");
            commandListener.reset();
            assertThrows(MongoOperationTimeoutException.class, () -> clientEncryption.decrypt(encrypted));

            List<CommandStartedEvent> commandStartedEvents = commandListener.getCommandStartedEvents();
            assertEquals(1, commandStartedEvents.size());
            assertEquals(keyVaultNamespace.getCollectionName(), commandStartedEvents.get(0).getCommand().get("find").asString().getValue());
            assertNotNull(commandListener.getCommandFailedEvent("find"));
        }
    }

    /**
     * Not a prose spec test. However, it is additional test case for better coverage.
     */
    @Test
    void shouldDecreaseOperationTimeoutForSubsequentOperations() {
        assumeTrue(serverVersionAtLeast(4, 4));
        long initialTimeoutMS = 2500;

        keyVaultCollectionHelper.runAdminCommand("{"
                + "    configureFailPoint: \"" + FAIL_COMMAND_NAME + "\","
                + "  mode: \"alwaysOn\","
                + "  data: {"
                + "    failCommands: [\"insert\", \"find\", \"listCollections\"],"
                + "    blockConnection: true,"
                + "    blockTimeMS: " + 10
                + "  }"
                + "}");

        try (ClientEncryption clientEncryption = createClientEncryption(getClientEncryptionSettingsBuilder()
                .timeout(initialTimeoutMS, MILLISECONDS))) {
            BsonBinary dataKeyId = clientEncryption.createDataKey("local", new DataKeyOptions());
            String base64DataKeyId = Base64.getEncoder().encodeToString(dataKeyId.getData());

            final String dbName = "test";
            final String collName = "coll";

            AutoEncryptionSettings autoEncryptionSettings = AutoEncryptionSettings.builder()
                    .keyVaultNamespace(keyVaultNamespace.getFullName())
                    .keyVaultMongoClientSettings(getMongoClientSettingsBuilder()
                            .build())
                    .kmsProviders(KMS_PROVIDERS)
                    .build();

            try (MongoClient mongoClient = createMongoClient(getMongoClientSettingsBuilder()
                    .autoEncryptionSettings(autoEncryptionSettings)
                    .timeout(initialTimeoutMS, MILLISECONDS))) {

                CreateCollectionOptions createCollectionOptions = new CreateCollectionOptions();
                createCollectionOptions.validationOptions(new ValidationOptions()
                        .validator(new BsonDocument("$jsonSchema", BsonDocument.parse("{"
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
                                + "}"))));

                MongoCollection<Document> collection = mongoClient.getDatabase(dbName).getCollection(collName);
                collection.drop();

                mongoClient.getDatabase(dbName).createCollection(collName, createCollectionOptions);

                commandListener.reset();
                collection.insertOne(new Document("encryptedField", "123456789"));

                List<CommandStartedEvent> commandStartedEvents = commandListener.getCommandStartedEvents();
                assertTimeoutIsDecreasingForCommands(Arrays.asList("listCollections", "find", "insert"), commandStartedEvents,
                        initialTimeoutMS);
            }
        }
    }

    /**
     * Not a prose spec test. However, it is additional test case for better coverage.
     */
    @ParameterizedTest
    @ValueSource(strings = {"insert", "create"})
    void shouldThrowTimeoutExceptionWhenCreateEncryptedCollection(final String commandToTimeout) {
        assumeTrue(serverVersionAtLeast(7, 0));
        //given
        long initialTimeoutMS = 200;

        try (ClientEncryption clientEncryption = createClientEncryption(getClientEncryptionSettingsBuilder()
                .timeout(initialTimeoutMS, MILLISECONDS))) {
            final String dbName = "test";
            final String collName = "coll";

            try (MongoClient mongoClient = createMongoClient(getMongoClientSettingsBuilder()
                    .timeout(initialTimeoutMS, MILLISECONDS))) {
                CreateCollectionOptions createCollectionOptions = new CreateCollectionOptions().encryptedFields(Document.parse(
                        "{"
                                + "  fields: [{"
                                + "    path: 'ssn',"
                                + "    bsonType: 'string',"
                                + "    keyId: null"
                                + "  }]"
                                + "}"));

                keyVaultCollectionHelper.runAdminCommand("{"
                        + "    configureFailPoint: \"" + FAIL_COMMAND_NAME + "\","
                        + "  mode: { times: 1 },"
                        + "  data: {"
                        + "    failCommands: [\"" + commandToTimeout + "\"],"
                        + "    blockConnection: true,"
                        + "    blockTimeMS: " + initialTimeoutMS
                        + "  }"
                        + "}");

                MongoDatabase database = mongoClient.getDatabase(dbName);
                database.getCollection(collName).drop();
                commandListener.reset();

                //when
                MongoUpdatedEncryptedFieldsException encryptionException = assertThrows(MongoUpdatedEncryptedFieldsException.class, () ->
                        clientEncryption.createEncryptedCollection(database, collName, createCollectionOptions,
                                new CreateEncryptedCollectionParams("local")));
                //then
                assertInstanceOf(MongoOperationTimeoutException.class, encryptionException.getCause());
            }
        }
    }

    private static void assertTimeoutIsDecreasingForCommands(final List<String> commandNames,
                                                             final List<CommandStartedEvent> commandStartedEvents,
                                                             final long initialTimeoutMs) {
        long previousMaxTimeMS = initialTimeoutMs;
        assertEquals(commandNames.size(), commandStartedEvents.size(), "There have been more commands then expected");
        for (int i = 0; i < commandStartedEvents.size(); i++) {
            CommandStartedEvent commandStartedEvent = commandStartedEvents.get(i);
            String expectedCommandName = commandNames.get(i);
            assertEquals(expectedCommandName, commandStartedEvent.getCommandName());

            BsonDocument command = commandStartedEvent.getCommand();
            assertTrue(command.containsKey("maxTimeMS"), "Command " + expectedCommandName + " should have maxTimeMS set");

            long maxTimeMS = command.getInt64("maxTimeMS").getValue();

            if (i > 0) {
                assertThat(commandStartedEvent.getCommandName() + " " + "maxTimeMS should be less than that of a previous "
                        + commandStartedEvents.get(i - 1).getCommandName() + " command", maxTimeMS, lessThan(previousMaxTimeMS));
            } else {
                assertThat("maxTimeMS should be less than the configured timeout " + initialTimeoutMs + "ms",
                        maxTimeMS, lessThan(previousMaxTimeMS));
            }
            previousMaxTimeMS = maxTimeMS;
        }
    }

    protected ClientEncryptionSettings.Builder getClientEncryptionSettingsBuilder(final long vaultTimeout) {
        return ClientEncryptionSettings
                .builder()
                .keyVaultNamespace(keyVaultNamespace.getFullName())
                .keyVaultMongoClientSettings(getMongoClientSettingsBuilder()
                        .timeout(vaultTimeout, TimeUnit.MILLISECONDS).build())
                .kmsProviders(KMS_PROVIDERS);
    }

    protected ClientEncryptionSettings.Builder getClientEncryptionSettingsBuilder() {
        return ClientEncryptionSettings
                .builder()
                .keyVaultNamespace(keyVaultNamespace.getFullName())
                .keyVaultMongoClientSettings(getMongoClientSettingsBuilder().build())
                .kmsProviders(KMS_PROVIDERS);
    }

    protected MongoClientSettings.Builder getMongoClientSettingsBuilder() {
        return Fixture.getMongoClientSettingsBuilder()
                .readConcern(ReadConcern.MAJORITY)
                .writeConcern(WriteConcern.MAJORITY)
                .readPreference(ReadPreference.primary())
                .addCommandListener(commandListener);
    }

    @BeforeEach
    public void setUp() {
        Map<String, Object> localProviderMap = new HashMap<>();
        localProviderMap.put("key", Base64.getDecoder().decode(MASTER_KEY));
        KMS_PROVIDERS.put("local", localProviderMap);

        keyVaultCollectionHelper = new CollectionHelper<>(new BsonDocumentCodec(), keyVaultNamespace);
        keyVaultCollectionHelper.create();
        commandListener = new TestCommandListener();
    }

    @AfterEach
    public void tearDown() {
        ClusterFixture.disableFailPoint(FAIL_COMMAND_NAME);
        if (keyVaultCollectionHelper != null) {
            keyVaultCollectionHelper.drop();
        }
    }
}
