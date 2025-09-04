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
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.DropCollectionOptions;
import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.client.model.vault.TextOptions;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.connection.ServerVersion;
import com.mongodb.fixture.EncryptionFixture;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.mongodb.ClusterFixture.getDefaultDatabaseName;
import static com.mongodb.ClusterFixture.getMongoCryptVersion;
import static com.mongodb.ClusterFixture.hasEncryptionTestsEnabled;
import static com.mongodb.ClusterFixture.isStandalone;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.Fixture.getDefaultDatabase;
import static com.mongodb.client.Fixture.getMongoClient;
import static com.mongodb.client.Fixture.getMongoClientSettings;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static com.mongodb.fixture.EncryptionFixture.getKmsProviders;
import static java.util.Arrays.asList;
import static org.junit.Assume.assumeTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static util.JsonPoweredTestHelper.getTestDocument;

public abstract class AbstractClientEncryptionTextExplicitEncryptionTest {

    private static final ServerVersion REQUIRED_LIB_MONGOCRYPT_VERSION = new ServerVersion(asList(1, 5, 1));
    private MongoClient encryptedClient;
    private MongoDatabase encryptedDatabase;
    private ClientEncryption clientEncryption;
    private BsonBinary key1Id;

    protected abstract MongoClient createMongoClient(MongoClientSettings settings);
    protected abstract ClientEncryption createClientEncryption(ClientEncryptionSettings settings);


    @BeforeEach
    public void setUp() {
        assumeTrue("Text explicit encryption tests disabled", hasEncryptionTestsEnabled());
        assumeTrue("Requires newer MongoCrypt version", getMongoCryptVersion().compareTo(REQUIRED_LIB_MONGOCRYPT_VERSION) >= 0);
        assumeTrue(serverVersionAtLeast(8, 2));
        assumeFalse(isStandalone());

        MongoNamespace dataKeysNamespace = new MongoNamespace("keyvault.datakeys");
        BsonDocument encryptedFieldsPrefixSuffix = bsonDocumentFromPath("encryptedFields-prefix-suffix.json");
        BsonDocument encryptedFieldsSubstring = bsonDocumentFromPath("encryptedFields-substring.json");
        BsonDocument key1Document = bsonDocumentFromPath("keys/key1-document.json");

        MongoDatabase database = getDefaultDatabase().withWriteConcern(WriteConcern.MAJORITY);
        database.getCollection("prefix-suffix")
                .drop(new DropCollectionOptions().encryptedFields(encryptedFieldsPrefixSuffix));
        database.createCollection("prefix-suffix",
                new CreateCollectionOptions().encryptedFields(encryptedFieldsPrefixSuffix));

        database.getCollection("substring")
                .drop(new DropCollectionOptions().encryptedFields(encryptedFieldsSubstring));
        database.createCollection("substring",
                new CreateCollectionOptions().encryptedFields(encryptedFieldsSubstring));

        MongoCollection<BsonDocument> dataKeysCollection = getMongoClient()
                .getDatabase(dataKeysNamespace.getDatabaseName())
                .getCollection(dataKeysNamespace.getCollectionName(), BsonDocument.class)
                .withWriteConcern(WriteConcern.MAJORITY);

        dataKeysCollection.drop();
        dataKeysCollection.insertOne(key1Document);
        key1Id = key1Document.getBinary("_id");

        Map<String, Map<String, Object>> kmsProviders = getKmsProviders(EncryptionFixture.KmsProviderType.LOCAL);

        clientEncryption = createClientEncryption(ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(getMongoClientSettings())
                .keyVaultNamespace(dataKeysNamespace.getFullName())
                .kmsProviders(kmsProviders)
                .build());

        encryptedClient = createMongoClient(getMongoClientSettingsBuilder()
                .autoEncryptionSettings(
                        AutoEncryptionSettings.builder()
                                .keyVaultNamespace(dataKeysNamespace.getFullName())
                                .kmsProviders(kmsProviders)
                                .bypassQueryAnalysis(true)
                                .build())
                .build());

        encryptedDatabase = encryptedClient.getDatabase(getDefaultDatabaseName()).withWriteConcern(WriteConcern.MAJORITY);

        EncryptOptions prefixSuffixEncryptOptions = new EncryptOptions("TextPreview")
                .keyId(key1Id)
                .contentionFactor(0L)
                .textOptions(new TextOptions()
                        .caseSensitive(true)
                        .diacriticSensitive(true)
                        .prefixOptions(BsonDocument.parse("{strMaxQueryLength: 10, strMinQueryLength: 2}"))
                        .suffixOptions(BsonDocument.parse("{strMaxQueryLength: 10, strMinQueryLength: 2}"))
                );

        BsonBinary foobarbaz = clientEncryption.encrypt(new BsonString("foobarbaz"), prefixSuffixEncryptOptions);

        encryptedDatabase
                .getCollection("prefix-suffix")
                .insertOne(new Document("_id", 0).append("encryptedText", foobarbaz));

        EncryptOptions substringEncryptOptions = new EncryptOptions("TextPreview")
                .keyId(key1Id)
                .contentionFactor(0L)
                .textOptions(new TextOptions()
                        .caseSensitive(true)
                        .diacriticSensitive(true)
                        .substringOptions(BsonDocument.parse("{strMaxLength: 10, strMaxQueryLength: 10, strMinQueryLength: 2}"))
                );
        foobarbaz = clientEncryption.encrypt(new BsonString("foobarbaz"), substringEncryptOptions);

        encryptedDatabase
                .getCollection("substring")
                .insertOne(new Document("_id", 0).append("encryptedText", foobarbaz));
    }

    @Test
    @DisplayName("Case 1: can find a document by prefix")
    public void test1CanFindADocumentByPrefix() {
    EncryptOptions encryptOptions = new EncryptOptions("TextPreview")
            .keyId(key1Id)
            .contentionFactor(0L)
            .queryType("prefixPreview")
            .textOptions(new TextOptions()
                    .caseSensitive(true)
                    .diacriticSensitive(true)
                    .prefixOptions(BsonDocument.parse("{strMaxQueryLength: 10, strMinQueryLength: 2}"))
            );

        BsonBinary encrypted = clientEncryption.encrypt(new BsonString("foo"), encryptOptions);
        Document result = encryptedDatabase.getCollection("prefix-suffix")
                .find(new Document("$expr",
                        new Document("$encStrStartsWith",
                            new Document("input", "$encryptedText").append("prefix", encrypted)))).first();

        assertDocumentEquals(Document.parse("{ \"_id\": 0, \"encryptedText\": \"foobarbaz\" }"), result);
    }

    @Test
    @DisplayName("Case 2: can find a document by suffix")
    public void test2CanFindADocumentBySuffix() {
        EncryptOptions encryptOptions = new EncryptOptions("TextPreview")
                .keyId(key1Id)
                .contentionFactor(0L)
                .queryType("suffixPreview")
                .textOptions(new TextOptions()
                        .caseSensitive(true)
                        .diacriticSensitive(true)
                        .suffixOptions(BsonDocument.parse("{strMaxQueryLength: 10, strMinQueryLength: 2}"))
                );

        BsonBinary encrypted = clientEncryption.encrypt(new BsonString("baz"), encryptOptions);
        Document result = encryptedDatabase.getCollection("prefix-suffix")
                .find(new Document("$expr",
                        new Document("$encStrEndsWith",
                                new Document("input", "$encryptedText").append("suffix", encrypted)))).first();

        assertDocumentEquals(Document.parse("{ \"_id\": 0, \"encryptedText\": \"foobarbaz\" }"), result);
    }

    @Test
    @DisplayName("Case 3: assert no document found by prefix")
    public void test3AssertNoDocumentFoundByPrefix() {
        EncryptOptions encryptOptions = new EncryptOptions("TextPreview")
                .keyId(key1Id)
                .contentionFactor(0L)
                .queryType("prefixPreview")
                .textOptions(new TextOptions()
                        .caseSensitive(true)
                        .diacriticSensitive(true)
                        .prefixOptions(BsonDocument.parse("{strMaxQueryLength: 10, strMinQueryLength: 2}"))
                );

        BsonBinary encrypted = clientEncryption.encrypt(new BsonString("baz"), encryptOptions);
        Document result = encryptedDatabase.getCollection("prefix-suffix")
                .find(new Document("$expr",
                        new Document("$encStrStartsWith",
                                new Document("input", "$encryptedText").append("prefix", encrypted)))).first();

        assertNull(result);
    }

    @Test
    @DisplayName("Case 4: assert no document found by suffix")
    public void test4AssertNoDocumentFoundByPrefix() {
        EncryptOptions encryptOptions = new EncryptOptions("TextPreview")
                .keyId(key1Id)
                .contentionFactor(0L)
                .queryType("suffixPreview")
                .textOptions(new TextOptions()
                        .caseSensitive(true)
                        .diacriticSensitive(true)
                        .suffixOptions(BsonDocument.parse("{strMaxQueryLength: 10, strMinQueryLength: 2}"))
                );

        BsonBinary encrypted = clientEncryption.encrypt(new BsonString("foo"), encryptOptions);
        Document result = encryptedDatabase.getCollection("prefix-suffix")
                .find(new Document("$expr",
                        new Document("$encStrEndsWith",
                                new Document("input", "$encryptedText").append("suffix", encrypted)))).first();

        assertNull(result);
    }

    @Test
    @DisplayName("Case 5: can find a document by substring")
    public void test5CanFindADocumentBySubstring() {
        EncryptOptions encryptOptions = new EncryptOptions("TextPreview")
                .keyId(key1Id)
                .contentionFactor(0L)
                .queryType("substringPreview")
                .textOptions(new TextOptions()
                        .caseSensitive(true)
                        .diacriticSensitive(true)
                        .substringOptions(BsonDocument.parse("{strMaxLength: 10, strMaxQueryLength: 10, strMinQueryLength: 2}"))
                );

        BsonBinary encrypted = clientEncryption.encrypt(new BsonString("bar"), encryptOptions);
        Document result = encryptedDatabase.getCollection("substring")
                .find(new Document("$expr",
                        new Document("$encStrContains",
                                new Document("input", "$encryptedText").append("substring", encrypted)))).first();

        assertDocumentEquals(Document.parse("{ \"_id\": 0, \"encryptedText\": \"foobarbaz\" }"), result);
    }

    @Test
    @DisplayName("Case 6: assert no document found by substring")
    public void test6AssertNoDocumentFoundBySubstring() {
        EncryptOptions encryptOptions = new EncryptOptions("TextPreview")
                .keyId(key1Id)
                .contentionFactor(0L)
                .queryType("substringPreview")
                .textOptions(new TextOptions()
                        .caseSensitive(true)
                        .diacriticSensitive(true)
                        .substringOptions(BsonDocument.parse("{strMaxLength: 10, strMaxQueryLength: 10, strMinQueryLength: 2}"))
                );

        BsonBinary encrypted = clientEncryption.encrypt(new BsonString("qux"), encryptOptions);
        Document result = encryptedDatabase.getCollection("substring")
                .find(new Document("$expr",
                        new Document("$encStrContains",
                                new Document("input", "$encryptedText").append("substring", encrypted)))).first();

        assertNull(result);
    }

    @Test
    @DisplayName("Case 7: assert `contentionFactor` is required")
    public void test7AssertContentionFactorIsRequired() {
        EncryptOptions encryptOptions = new EncryptOptions("TextPreview")
                .keyId(key1Id)
                .queryType("prefixPreview")
                .textOptions(new TextOptions()
                        .caseSensitive(true)
                        .diacriticSensitive(true)
                        .prefixOptions(BsonDocument.parse("{strMaxQueryLength: 10, strMinQueryLength: 2}"))
                );
        MongoException exception = assertThrows(MongoException.class, () -> clientEncryption.encrypt(new BsonString("foo"), encryptOptions));
        assertTrue(exception.getMessage().contains("contention factor is required for textPreview algorithm"));
    }


    @AfterEach
    @SuppressWarnings("try")
    public void cleanUp() {
        //noinspection EmptyTryBlock
        getDefaultDatabase().withWriteConcern(WriteConcern.MAJORITY).drop();
        try (ClientEncryption ignored = this.clientEncryption;
             MongoClient ignored1 = this.encryptedClient
        ) {
            // just using try-with-resources to ensure they all get closed, even in the case of exceptions
        }
    }

    private static void assertDocumentEquals(final Document expectedDocument, final Document actualDocument) {
        actualDocument.remove("__safeContent__");
        assertEquals(expectedDocument, actualDocument);
    }

    private static BsonDocument bsonDocumentFromPath(final String path) {
        return getTestDocument("client-side-encryption/etc/data/" + path);
    }
}
