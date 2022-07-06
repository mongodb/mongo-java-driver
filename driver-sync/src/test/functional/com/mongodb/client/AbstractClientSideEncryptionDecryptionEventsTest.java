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
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoSocketReadException;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.event.CommandSucceededEvent;
import com.mongodb.internal.connection.TestCommandListener;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mongodb.ClusterFixture.configureFailPoint;
import static com.mongodb.ClusterFixture.isServerlessTest;
import static com.mongodb.ClusterFixture.isStandalone;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.Fixture.getDefaultDatabase;
import static com.mongodb.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

// See: https://github.com/mongodb/specifications/tree/master/source/client-side-encryption/tests#decryption-events
public abstract class AbstractClientSideEncryptionDecryptionEventsTest {
    private static final List<Bson> AGGREGATION_PIPELINE = singletonList(Aggregates.match(new BsonDocument()));
    private MongoClient encryptedClient;
    private ClientEncryption clientEncryption;
    private TestCommandListener commandListener;
    private BsonBinary ciphertext;
    private BsonBinary malformedCiphertext;


    protected abstract MongoClient createMongoClient(MongoClientSettings settings);
    protected abstract ClientEncryption createClientEncryption(ClientEncryptionSettings settings);

    @BeforeEach
    public void setUp() {
        assumeTrue(serverVersionAtLeast(6, 0));
        assumeFalse(isStandalone());
        assumeFalse(isServerlessTest());

        getDefaultDatabase().getCollection("decryption_events").drop();
        getDefaultDatabase().createCollection("decryption_events");

        Map<String, Map<String, Object>> kmsProviders = new HashMap<>();
        Map<String, Object> localProviderMap = new HashMap<>();
        localProviderMap.put("key",
                Base64.getDecoder().decode(
                        "Mng0NCt4ZHVUYUJCa1kxNkVyNUR1QURhZ2h2UzR2d2RrZzh0cFBwM3R6NmdWMDFBMUN3YkQ5aXRRMkhGRGdQV09wOGVNYUMxT2k3NjZKelhaQmRCZ"
                                + "GJkTXVyZG9uSjFk"));
        kmsProviders.put("local", localProviderMap);

        commandListener = new TestCommandListener();
        clientEncryption = createClientEncryption(ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(getMongoClientSettingsBuilder().addCommandListener(commandListener).build())
                .keyVaultNamespace("keyvault.datakeys")
                .kmsProviders(kmsProviders)
                .build());

        BsonBinary keyId = clientEncryption.createDataKey("local");

        ciphertext = clientEncryption.encrypt(new BsonString("hello"), new EncryptOptions("AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic").keyId(keyId));

        // Copy ciphertext into a variable named malformedCiphertext. Change the last byte. This will produce an invalid HMAC tag.
        byte[] malformedBytes = ciphertext.getData().clone();
        malformedBytes[malformedBytes.length - 1] = (byte) (malformedBytes[malformedBytes.length - 1] == 0 ? 0 : 1);
        malformedCiphertext = new BsonBinary(ciphertext.getType(), malformedBytes);

        commandListener = new TestCommandListener();
        encryptedClient = createMongoClient(getMongoClientSettingsBuilder()
                .autoEncryptionSettings(
                        AutoEncryptionSettings.builder()
                                .keyVaultNamespace("keyvault.datakeys")
                                .kmsProviders(kmsProviders)
                                .build())
                .retryReads(false)
                .addCommandListener(commandListener)
                .build());
    }

    @AfterEach
    public void cleanUp() {
        //noinspection EmptyTryBlock
        try (ClientEncryption ignored = this.clientEncryption;
             MongoClient ignored1 = this.encryptedClient
        ) {
            // just using try-with-resources to ensure they all get closed, even in the case of exceptions
        }
    }

    @Test
    public void commandError() {
        configureFailPoint(BsonDocument.parse("{"
                + "    'configureFailPoint': 'failCommand',"
                + "    'mode': {"
                + "        'times': 1"
                + "    },"
                + "    'data': {"
                + "        'errorCode': 123,"
                + "        'failCommands': ["
                + "            'aggregate'"
                + "        ]"
                + "    }"
                + "}"));



        assertThrows(MongoCommandException.class, () -> encryptedClient
                .getDatabase(getDefaultDatabaseName())
                .getCollection("decryption_events")
                .aggregate(AGGREGATION_PIPELINE)
                .first());

        assertDoesNotThrow(() -> commandListener.getCommandFailedEvent("aggregate"));
    }

    @Test
    public void networkError() {
        configureFailPoint(BsonDocument.parse("{"
                + "    'configureFailPoint': 'failCommand',"
                + "    'mode': {"
                + "        'times': 1"
                + "    },"
                + "    'data': {"
                + "        'errorCode': 123,"
                + "         'closeConnection': true,"
                + "        'failCommands': ["
                + "            'aggregate'"
                + "        ]"
                + "    }"
                + "}"));

        assertThrows(MongoSocketReadException.class, () -> encryptedClient
                .getDatabase(getDefaultDatabaseName())
                .getCollection("decryption_events")
                .aggregate(AGGREGATION_PIPELINE)
                .first());

        assertDoesNotThrow(() -> commandListener.getCommandFailedEvent("aggregate"));
    }

    @Test
    public void decryptError() {
        MongoCollection<Document> decryptionEvents = encryptedClient
                .getDatabase(getDefaultDatabaseName())
                .getCollection("decryption_events");

        decryptionEvents.insertOne(new Document("encrypted", malformedCiphertext));

        assertThrows(MongoException.class, () -> decryptionEvents
                .aggregate(AGGREGATION_PIPELINE)
                .first());

        CommandSucceededEvent succeededEvent = commandListener.getCommandSucceededEvent("aggregate");
        assertEquals(BsonType.BINARY, succeededEvent
                .getResponse()
                .getDocument("cursor")
                .getArray("firstBatch")
                .get(0)
                .asDocument()
                .get("encrypted")
                .getBsonType());
    }

    @Test
    public void decryptSuccess() {
        MongoCollection<Document> decryptionEvents = encryptedClient
                .getDatabase(getDefaultDatabaseName())
                .getCollection("decryption_events");

        decryptionEvents.insertOne(new Document("encrypted", ciphertext));
        Document document = assertDoesNotThrow(() -> decryptionEvents
                .aggregate(AGGREGATION_PIPELINE)
                .first());

        assertEquals("hello", document.getString("encrypted"));
        CommandSucceededEvent succeededEvent = commandListener.getCommandSucceededEvent("aggregate");
        assertEquals(BsonType.BINARY, succeededEvent
                .getResponse()
                .getDocument("cursor")
                .getArray("firstBatch")
                .get(0)
                .asDocument()
                .get("encrypted")
                .getBsonType());
    }
}
