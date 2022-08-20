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

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.ClientEncryptionSettings;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ValidationOptions;
import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.client.vault.ClientEncryptions;
import com.mongodb.event.CommandEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.internal.connection.TestCommandListener;
import com.mongodb.lang.NonNull;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.isClientSideEncryptionTest;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.Fixture.getMongoClient;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static util.JsonPoweredTestHelper.getTestDocument;

public abstract class AbstractClientSideEncryptionDeadlockTest {
    private BsonBinary cipherText;
    private MongoClient encryptingClient;
    private Map<String, Map<String, Object>> kmsProviders;

    protected abstract MongoClient createMongoClient(MongoClientSettings settings);

    @BeforeEach
    public void setUp() throws IOException, URISyntaxException {
        assumeTrue(serverVersionAtLeast(4, 2));
        assumeTrue(isClientSideEncryptionTest());

        MongoDatabase keyVaultDatabase = getMongoClient().getDatabase("keyvault");
        MongoCollection<BsonDocument> dataKeysCollection = keyVaultDatabase.getCollection("datakeys", BsonDocument.class)
                .withWriteConcern(WriteConcern.MAJORITY);
        dataKeysCollection.drop();
        dataKeysCollection.insertOne(bsonDocumentFromPath("external-key.json"));

        MongoDatabase encryptedDatabase = getMongoClient().getDatabase("db");
        MongoCollection<BsonDocument> encryptedCollection = encryptedDatabase.getCollection("coll", BsonDocument.class)
                .withWriteConcern(WriteConcern.MAJORITY);
        encryptedCollection.drop();
        encryptedDatabase.createCollection("coll", new CreateCollectionOptions()
                .validationOptions(new ValidationOptions()
                        .validator(new BsonDocument("$jsonSchema", bsonDocumentFromPath("external-schema.json")))));

        kmsProviders = new HashMap<>();
        Map<String, Object> localProviderMap = new HashMap<>();
        localProviderMap.put("key",
                Base64.getDecoder().decode(
                        "Mng0NCt4ZHVUYUJCa1kxNkVyNUR1QURhZ2h2UzR2d2RrZzh0cFBwM3R6NmdWMDFBMUN3YkQ5aXRRMkhGRGdQV09wOGVNYUMxT2k3NjZKelhaQmRCZ"
                                + "GJkTXVyZG9uSjFk"));
        kmsProviders.put("local", localProviderMap);
        ClientEncryption clientEncryption = ClientEncryptions.create(
                ClientEncryptionSettings.builder()
                        .keyVaultMongoClientSettings(getKeyVaultClientSettings(new TestCommandListener()))
                        .keyVaultNamespace("keyvault.datakeys")
                        .kmsProviders(kmsProviders)
                        .build());
        cipherText = clientEncryption.encrypt(new BsonString("string0"),
                new EncryptOptions("AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic").keyAltName("local"));
        clientEncryption.close();
    }

    @AfterEach
    @SuppressWarnings("try")
    public void cleanUp() {
        //noinspection EmptyTryBlock
        try (MongoClient ignored = this.encryptingClient) {
            // just using try-with-resources to ensure they all get closed, even in the case of exceptions
        }
    }

    private static Stream<Arguments> testArgumentProvider() {
        return Stream.of(
                //
                arguments(1, 2, false, false,
                        asList(new ExpectedEvent("db", "listCollections"),
                                new ExpectedEvent("keyvault", "find"),
                                new ExpectedEvent("db", "insert"),
                                new ExpectedEvent("db", "find")),
                        emptyList()),
                arguments(1, 2, false, true,
                        asList(new ExpectedEvent("db", "listCollections"),
                                new ExpectedEvent("db", "insert"),
                                new ExpectedEvent("db", "find")),
                        asList(new ExpectedEvent("keyvault", "find"))),
                arguments(1, 2, true, false,
                        asList(new ExpectedEvent("db", "find"),
                                new ExpectedEvent("keyvault", "find")),
                        emptyList()),

                arguments(1, 1, true, true,
                        asList(new ExpectedEvent("db", "find")),
                        asList(new ExpectedEvent("keyvault", "find")))
        );
    }

    @ParameterizedTest
    @MethodSource("testArgumentProvider")
    public void shouldPassAllOutcomes(final int maxPoolSize,
                                      final int expectedNumberOfClientsCreated,
                                      final boolean bypassAutoEncryption,
                                      final boolean externalKeyVaultClient,
                                      final List<ExpectedEvent> expectedEncryptingClientEvents,
                                      final List<ExpectedEvent> expectedExternalKeyVaultsClientEvents) {
        AutoEncryptionSettings.Builder autoEncryptionSettingsBuilder = AutoEncryptionSettings.builder()
                .keyVaultNamespace("keyvault.datakeys")
                .kmsProviders(kmsProviders)
                .bypassAutoEncryption(bypassAutoEncryption);
        TestCommandListener externalKeyVaultClientCommandListener =
                new TestCommandListener(singletonList("commandStartedEvent"), emptyList());
        if (externalKeyVaultClient) {
            autoEncryptionSettingsBuilder.keyVaultMongoClientSettings(getKeyVaultClientSettings(externalKeyVaultClientCommandListener));
        }

        TestCommandListener encryptingClientCommandListener = new TestCommandListener(singletonList("commandStartedEvent"), emptyList());
        encryptingClient = createMongoClient(getClientSettings(maxPoolSize, encryptingClientCommandListener,
                autoEncryptionSettingsBuilder.build()));

        BsonDocument unencryptedDocument = new BsonDocument("_id", new BsonInt32(0)).append("encrypted", new BsonString("string0"));

        if (bypassAutoEncryption) {
            getMongoClient().getDatabase("db")
                    .getCollection("coll", BsonDocument.class)
                    .withWriteConcern(WriteConcern.MAJORITY)
                    .insertOne(new BsonDocument("_id", new BsonInt32(0)).append("encrypted", cipherText));

        } else {
            encryptingClient.getDatabase("db")
                    .getCollection("coll", BsonDocument.class)
                    .withWriteConcern(WriteConcern.MAJORITY)
                    .insertOne(unencryptedDocument);
        }

        BsonDocument result = encryptingClient.getDatabase("db")
                .getCollection("coll", BsonDocument.class)
                .find().filter(Filters.eq("_id", 0)).first();

        assertEquals(unencryptedDocument, result);

        assertEquals(expectedNumberOfClientsCreated, getNumUniqueClients(encryptingClientCommandListener), "Unique clients");

        assertEventEquality(encryptingClientCommandListener, expectedEncryptingClientEvents);
        assertEventEquality(externalKeyVaultClientCommandListener, expectedExternalKeyVaultsClientEvents);
    }

    private void assertEventEquality(final TestCommandListener commandListener, final List<ExpectedEvent> expectedStartEvents) {
        List<CommandEvent> actualStartedEvents = commandListener.getCommandStartedEvents();
        assertEquals(expectedStartEvents.size(), actualStartedEvents.size());
        for (int i = 0; i < expectedStartEvents.size(); i++) {
            ExpectedEvent expectedEvent = expectedStartEvents.get(i);
            CommandStartedEvent actualEvent = (CommandStartedEvent) actualStartedEvents.get(i);
            assertEquals(expectedEvent.getDatabase(), actualEvent.getDatabaseName(), "Database name");
            assertEquals(expectedEvent.getCommandName(), actualEvent.getCommandName(), "Command name");
        }
    }

    private int getNumUniqueClients(final TestCommandListener commandListener) {
        Set<String> uniqueClients = new HashSet<>();
        for (CommandEvent event : commandListener.getEvents()) {
            uniqueClients.add(event.getConnectionDescription().getConnectionId().getServerId().getClusterId().getValue());
        }
        return uniqueClients.size();
    }

    @NonNull
    private static MongoClientSettings getKeyVaultClientSettings(final CommandListener commandListener) {
        return getClientSettings(1, commandListener, null);
    }

    @NonNull
    private static MongoClientSettings getClientSettings(final int maxPoolSize,
                                                         final CommandListener commandListener,
                                                         final AutoEncryptionSettings autoEncryptionSettings) {
        return getMongoClientSettingsBuilder()
                .autoEncryptionSettings(autoEncryptionSettings)
                .readConcern(ReadConcern.MAJORITY)
                .writeConcern(WriteConcern.MAJORITY)
                .addCommandListener(commandListener)
                .applyToConnectionPoolSettings(builder -> builder.maxSize(maxPoolSize))
                .build();
    }

    private static BsonDocument bsonDocumentFromPath(final String path) throws URISyntaxException, IOException {
        return getTestDocument(new File(ClientSideEncryptionExternalKeyVaultTest.class
                .getResource("/client-side-encryption-external/" + path).toURI()));
    }

    private static final class ExpectedEvent {
        private final String database;
        private final String commandName;

        ExpectedEvent(final String database, final String commandName) {
            this.database = database;
            this.commandName = commandName;
        }

        String getDatabase() {
            return database;
        }

        String getCommandName() {
            return commandName;
        }
    }
}
