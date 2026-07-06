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
import com.mongodb.client.model.vault.StringOptions;
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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static util.JsonPoweredTestHelper.getTestDocument;

public abstract class AbstractClientEncryptionStringExplicitEncryptionTest {

    private static final ServerVersion REQUIRED_LIB_MONGOCRYPT_VERSION = new ServerVersion(asList(1, 19, 1));
    private boolean gaSupported;
    private MongoClient explicitEncryptedClient;
    private MongoClient autoEncryptedClient;
    private MongoDatabase explicitEncryptedDatabase;
    private MongoDatabase autoEncryptedDatabase;
    private ClientEncryption clientEncryption;
    private BsonBinary key1Id;

    protected abstract MongoClient createMongoClient(MongoClientSettings settings);
    protected abstract ClientEncryption createClientEncryption(ClientEncryptionSettings settings);

    @BeforeEach
    public void setUp() {
        assumeTrue(hasEncryptionTestsEnabled(), "String explicit encryption tests disabled");
        assumeTrue(getMongoCryptVersion().compareTo(REQUIRED_LIB_MONGOCRYPT_VERSION) >= 0, "Requires newer MongoCrypt version");
        assumeTrue(serverVersionAtLeast(8, 2));
        assumeFalse(isStandalone());

        gaSupported = serverVersionAtLeast(9, 0);

        MongoNamespace dataKeysNamespace = new MongoNamespace("keyvault.datakeys");
        BsonDocument key1Document = bsonDocumentFromPath("keys/key1-document.json");

        Map<String, Map<String, Object>> kmsProviders = getKmsProviders(EncryptionFixture.KmsProviderType.LOCAL);

        if (gaSupported) {
            createEncryptedCollection("prefix-suffix", "encryptedFields-prefix-suffix.json");
            createEncryptedCollection("prefix-suffix-ci-di", "encryptedFields-prefix-suffix-ci-di.json");
            createEncryptedCollection("substring", "encryptedFields-substring.json");
            createEncryptedCollection("substring-ci-di", "encryptedFields-substring-ci-di.json");
        } else {
            createEncryptedCollection("prefix-suffix-preview", "encryptedFields-prefix-suffix-preview.json");
            createEncryptedCollection("substring-preview", "encryptedFields-substring-preview.json");
        }

        MongoCollection<BsonDocument> dataKeysCollection = getMongoClient()
                .getDatabase(dataKeysNamespace.getDatabaseName())
                .getCollection(dataKeysNamespace.getCollectionName(), BsonDocument.class)
                .withWriteConcern(WriteConcern.MAJORITY);
        dataKeysCollection.drop();
        dataKeysCollection.insertOne(key1Document);
        key1Id = key1Document.getBinary("_id");

        clientEncryption = createClientEncryption(ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(getMongoClientSettings())
                .keyVaultNamespace(dataKeysNamespace.getFullName())
                .kmsProviders(kmsProviders)
                .build());

        explicitEncryptedClient = createMongoClient(getMongoClientSettingsBuilder()
                .autoEncryptionSettings(AutoEncryptionSettings.builder()
                        .keyVaultNamespace(dataKeysNamespace.getFullName())
                        .kmsProviders(kmsProviders)
                        .bypassQueryAnalysis(true)
                        .build())
                .build());
        explicitEncryptedDatabase = explicitEncryptedClient.getDatabase(getDefaultDatabaseName())
                .withWriteConcern(WriteConcern.MAJORITY);

        autoEncryptedClient = createMongoClient(getMongoClientSettingsBuilder()
                .autoEncryptionSettings(AutoEncryptionSettings.builder()
                        .keyVaultNamespace(dataKeysNamespace.getFullName())
                        .kmsProviders(kmsProviders)
                        .build())
                .build());
        autoEncryptedDatabase = autoEncryptedClient.getDatabase(getDefaultDatabaseName())
                .withWriteConcern(WriteConcern.MAJORITY);

        // Seed the prefix-suffix collection(s) with an encrypted "foobarbaz" document.
        BsonBinary prefixSuffixSeed = clientEncryption.encrypt(new BsonString("foobarbaz"),
                new EncryptOptions("String")
                        .keyId(key1Id)
                        .contentionFactor(0L)
                        .stringOptions(new StringOptions()
                                .caseSensitive(true)
                                .diacriticSensitive(true)
                                .prefixOptions(BsonDocument.parse("{strMaxQueryLength: 10, strMinQueryLength: 2}"))
                                .suffixOptions(BsonDocument.parse("{strMaxQueryLength: 10, strMinQueryLength: 2}"))));
        if (gaSupported) {
            explicitEncryptedDatabase.getCollection("prefix-suffix")
                    .insertOne(new Document("_id", 0).append("encryptedText", prefixSuffixSeed));
        } else {
            explicitEncryptedDatabase.getCollection("prefix-suffix-preview")
                    .insertOne(new Document("_id", 0).append("encryptedText", prefixSuffixSeed));
        }

        // Seed the substring collection: GA "substring" on 9.0+, preview on pre-9.0.
        BsonBinary substringSeed = clientEncryption.encrypt(new BsonString("foobarbaz"),
                new EncryptOptions("String")
                        .keyId(key1Id)
                        .contentionFactor(0L)
                        .stringOptions(new StringOptions()
                                .caseSensitive(true)
                                .diacriticSensitive(true)
                                .substringOptions(BsonDocument.parse(
                                        "{strMaxLength: 10, strMaxQueryLength: 6, strMinQueryLength: 2}"))));
        explicitEncryptedDatabase.getCollection(gaSupported ? "substring" : "substring-preview")
                .insertOne(new Document("_id", 0).append("encryptedText", substringSeed));
    }

    @Test
    @DisplayName("Case 1: can find a document by prefix")
    public void test1CanFindADocumentByPrefix() {
        String queryType = gaSupported ? "prefix" : "prefixPreview";
        String collection = gaSupported ? "prefix-suffix" : "prefix-suffix-preview";
        BsonBinary encrypted = encryptForPrefix("foo", queryType, true, true);
        Document result = explicitEncryptedDatabase.getCollection(collection)
                .find(encStrStartsWith(encrypted)).first();
        assertDocumentEquals(Document.parse("{ \"_id\": 0, \"encryptedText\": \"foobarbaz\" }"), result);
    }

    @Test
    @DisplayName("Case 2: can find a document by suffix")
    public void test2CanFindADocumentBySuffix() {
        String queryType = gaSupported ? "suffix" : "suffixPreview";
        String collection = gaSupported ? "prefix-suffix" : "prefix-suffix-preview";
        BsonBinary encrypted = encryptForSuffix("baz", queryType, true, true);
        Document result = explicitEncryptedDatabase.getCollection(collection)
                .find(encStrEndsWith(encrypted)).first();
        assertDocumentEquals(Document.parse("{ \"_id\": 0, \"encryptedText\": \"foobarbaz\" }"), result);
    }

    @Test
    @DisplayName("Case 3: assert no document found by prefix")
    public void test3AssertNoDocumentFoundByPrefix() {
        String queryType = gaSupported ? "prefix" : "prefixPreview";
        String collection = gaSupported ? "prefix-suffix" : "prefix-suffix-preview";
        BsonBinary encrypted = encryptForPrefix("baz", queryType, true, true);
        Document result = explicitEncryptedDatabase.getCollection(collection)
                .find(encStrStartsWith(encrypted)).first();
        assertNull(result);
    }

    @Test
    @DisplayName("Case 4: assert no document found by suffix")
    public void test4AssertNoDocumentFoundBySuffix() {
        String queryType = gaSupported ? "suffix" : "suffixPreview";
        String collection = gaSupported ? "prefix-suffix" : "prefix-suffix-preview";
        BsonBinary encrypted = encryptForSuffix("foo", queryType, true, true);
        Document result = explicitEncryptedDatabase.getCollection(collection)
                .find(encStrEndsWith(encrypted)).first();
        assertNull(result);
    }

    @Test
    @DisplayName("Case 5: can find a document by substring")
    public void test5CanFindADocumentBySubstring() {
        String queryType = gaSupported ? "substring" : "substringPreview";
        String collection = gaSupported ? "substring" : "substring-preview";
        BsonBinary encrypted = encryptForSubstring("bar", queryType, true, true);
        Document result = explicitEncryptedDatabase.getCollection(collection)
                .find(encStrContains(encrypted)).first();
        assertDocumentEquals(Document.parse("{ \"_id\": 0, \"encryptedText\": \"foobarbaz\" }"), result);
    }

    @Test
    @DisplayName("Case 6: assert no document found by substring")
    public void test6AssertNoDocumentFoundBySubstring() {
        String queryType = gaSupported ? "substring" : "substringPreview";
        String collection = gaSupported ? "substring" : "substring-preview";
        BsonBinary encrypted = encryptForSubstring("qux", queryType, true, true);
        Document result = explicitEncryptedDatabase.getCollection(collection)
                .find(encStrContains(encrypted)).first();
        assertNull(result);
    }

    @Test
    @DisplayName("Case 7: assert `contentionFactor` is required")
    public void test7AssertContentionFactorIsRequired() {
        assumeTrue(gaSupported);
        EncryptOptions encryptOptions = new EncryptOptions("String")
                .keyId(key1Id)
                .queryType("prefix")
                .stringOptions(new StringOptions()
                        .caseSensitive(true)
                        .diacriticSensitive(true)
                        .prefixOptions(BsonDocument.parse("{strMaxQueryLength: 10, strMinQueryLength: 2}")));
        MongoException exception = assertThrows(MongoException.class,
                () -> clientEncryption.encrypt(new BsonString("foo"), encryptOptions));
        assertTrue(exception.getMessage().contains("contention factor is required for string algorithm"));
    }

    @Test
    @DisplayName("Case 8: can find an auto-encrypted case-insensitively indexed document by prefix and suffix")
    public void test8AutoEncryptedCaseInsensitivePrefixAndSuffix() {
        assumeTrue(gaSupported);
        autoEncryptedDatabase.getCollection("prefix-suffix-ci-di")
                .insertOne(new Document("encryptedText", "BingQiLin"));

        BsonBinary prefix = encryptForPrefix("bing", "prefix", false, false);
        Document byPrefix = explicitEncryptedDatabase.getCollection("prefix-suffix-ci-di")
                .find(encStrStartsWith(prefix)).first();
        assertEncryptedTextEquals("BingQiLin", byPrefix);

        BsonBinary suffix = encryptForSuffix("lin", "suffix", false, false);
        Document bySuffix = explicitEncryptedDatabase.getCollection("prefix-suffix-ci-di")
                .find(encStrEndsWith(suffix)).first();
        assertEncryptedTextEquals("BingQiLin", bySuffix);
    }

    @Test
    @DisplayName("Case 9: can find an auto-encrypted diacritic-insensitively indexed document by prefix and suffix")
    public void test9AutoEncryptedDiacriticInsensitivePrefixAndSuffix() {
        assumeTrue(gaSupported);
        autoEncryptedDatabase.getCollection("prefix-suffix-ci-di")
                .insertOne(new Document("encryptedText", "cafébarbäz"));

        BsonBinary prefix = encryptForPrefix("cafe", "prefix", false, false);
        Document byPrefix = explicitEncryptedDatabase.getCollection("prefix-suffix-ci-di")
                .find(encStrStartsWith(prefix)).first();
        assertEncryptedTextEquals("cafébarbäz", byPrefix);

        BsonBinary suffix = encryptForSuffix("baz", "suffix", false, false);
        Document bySuffix = explicitEncryptedDatabase.getCollection("prefix-suffix-ci-di")
                .find(encStrEndsWith(suffix)).first();
        assertEncryptedTextEquals("cafébarbäz", bySuffix);
    }

    @Test
    @DisplayName("Case 10: can find an auto-encrypted case-insensitively indexed document by substring")
    public void test10AutoEncryptedCaseInsensitiveSubstring() {
        assumeTrue(gaSupported);
        autoEncryptedDatabase.getCollection("substring-ci-di")
                .insertOne(new Document("encryptedText", "FooBarBaz"));

        BsonBinary substring = encryptForSubstring("bar", "substring", false, false);
        Document result = explicitEncryptedDatabase.getCollection("substring-ci-di")
                .find(encStrContains(substring)).first();
        assertEncryptedTextEquals("FooBarBaz", result);
    }

    @Test
    @DisplayName("Case 11: can find an auto-encrypted diacritic-insensitively indexed document by substring")
    public void test11AutoEncryptedDiacriticInsensitiveSubstring() {
        assumeTrue(gaSupported);
        autoEncryptedDatabase.getCollection("substring-ci-di")
                .insertOne(new Document("encryptedText", "foocafébaz"));

        BsonBinary substring = encryptForSubstring("cafe", "substring", false, false);
        Document result = explicitEncryptedDatabase.getCollection("substring-ci-di")
                .find(encStrContains(substring)).first();
        assertEncryptedTextEquals("foocafébaz", result);
    }

    @AfterEach
    @SuppressWarnings("try")
    public void cleanUp() {
        getDefaultDatabase().withWriteConcern(WriteConcern.MAJORITY).drop();
        try (ClientEncryption ignored = this.clientEncryption;
             MongoClient ignored1 = this.explicitEncryptedClient;
             MongoClient ignored2 = this.autoEncryptedClient
        ) {
            // just using try-with-resources to ensure they all get closed, even in the case of exceptions
        }
    }

    private void createEncryptedCollection(final String name, final String encryptedFieldsFile) {
        BsonDocument encryptedFields = bsonDocumentFromPath(encryptedFieldsFile);
        MongoDatabase database = getDefaultDatabase().withWriteConcern(WriteConcern.MAJORITY);
        database.getCollection(name).drop(new DropCollectionOptions().encryptedFields(encryptedFields));
        database.createCollection(name, new CreateCollectionOptions().encryptedFields(encryptedFields));
    }

    private BsonBinary encryptForPrefix(final String value, final String queryType,
            final boolean caseSensitive, final boolean diacriticSensitive) {
        return clientEncryption.encrypt(new BsonString(value), new EncryptOptions("String")
                .keyId(key1Id)
                .contentionFactor(0L)
                .queryType(queryType)
                .stringOptions(new StringOptions()
                        .caseSensitive(caseSensitive)
                        .diacriticSensitive(diacriticSensitive)
                        .prefixOptions(BsonDocument.parse("{strMaxQueryLength: 10, strMinQueryLength: 2}"))));
    }

    private BsonBinary encryptForSuffix(final String value, final String queryType,
            final boolean caseSensitive, final boolean diacriticSensitive) {
        return clientEncryption.encrypt(new BsonString(value), new EncryptOptions("String")
                .keyId(key1Id)
                .contentionFactor(0L)
                .queryType(queryType)
                .stringOptions(new StringOptions()
                        .caseSensitive(caseSensitive)
                        .diacriticSensitive(diacriticSensitive)
                        .suffixOptions(BsonDocument.parse("{strMaxQueryLength: 10, strMinQueryLength: 2}"))));
    }

    private BsonBinary encryptForSubstring(final String value, final String queryType,
            final boolean caseSensitive, final boolean diacriticSensitive) {
        return clientEncryption.encrypt(new BsonString(value), new EncryptOptions("String")
                .keyId(key1Id)
                .contentionFactor(0L)
                .queryType(queryType)
                .stringOptions(new StringOptions()
                        .caseSensitive(caseSensitive)
                        .diacriticSensitive(diacriticSensitive)
                        .substringOptions(BsonDocument.parse(
                                "{strMaxLength: 10, strMaxQueryLength: 6, strMinQueryLength: 2}"))));
    }

    private static Document encStrStartsWith(final BsonBinary encrypted) {
        return new Document("$expr", new Document("$encStrStartsWith",
                new Document("input", "$encryptedText").append("prefix", encrypted)));
    }

    private static Document encStrEndsWith(final BsonBinary encrypted) {
        return new Document("$expr", new Document("$encStrEndsWith",
                new Document("input", "$encryptedText").append("suffix", encrypted)));
    }

    private static Document encStrContains(final BsonBinary encrypted) {
        return new Document("$expr", new Document("$encStrContains",
                new Document("input", "$encryptedText").append("substring", encrypted)));
    }

    private static void assertDocumentEquals(final Document expectedDocument, final Document actualDocument) {
        actualDocument.remove("__safeContent__");
        assertEquals(expectedDocument, actualDocument);
    }

    private static void assertEncryptedTextEquals(final String expectedText, final Document actualDocument) {
        assertNotNull(actualDocument);
        assertEquals(expectedText, actualDocument.getString("encryptedText"));
    }

    private static BsonDocument bsonDocumentFromPath(final String path) {
        return getTestDocument("client-side-encryption/etc/data/" + path);
    }
}
