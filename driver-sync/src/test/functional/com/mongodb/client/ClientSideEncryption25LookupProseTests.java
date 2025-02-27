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
 */

package com.mongodb.client;

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.ClientEncryptionSettings;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.ValidationOptions;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.client.vault.ClientEncryptions;
import com.mongodb.crypt.capi.MongoCryptException;
import com.mongodb.fixture.EncryptionFixture;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.types.Binary;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.mongodb.ClusterFixture.isStandalone;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.ClusterFixture.serverVersionLessThan;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static com.mongodb.fixture.EncryptionFixture.getKmsProviders;
import static com.mongodb.testing.MongoAssertions.assertCause;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static util.JsonPoweredTestHelper.getTestDocument;

/**
 * <a href="https://github.com/mongodb/specifications/blob/master/source/client-side-encryption/tests/README.md#25-test-lookup">
 */
public class ClientSideEncryption25LookupProseTests {
    private MongoClient client;

    protected MongoClient createMongoClient(final MongoClientSettings settings) {
        return MongoClients.create(settings);
    }

    protected ClientEncryption createClientEncryption(final ClientEncryptionSettings settings) {
        return ClientEncryptions.create(settings);
    }

    @BeforeEach
    public void setUp() {
        assumeFalse(isStandalone());
        assumeTrue(serverVersionAtLeast(7, 0));

        // Create an encrypted MongoClient named `encryptedClient` configured with:
        MongoNamespace dataKeysNamespace = new MongoNamespace("db.keyvault");
        Map<String, Map<String, Object>> kmsProviders = getKmsProviders(EncryptionFixture.KmsProviderType.LOCAL);
        MongoClient encryptedClient = createMongoClient(getMongoClientSettingsBuilder()
                .autoEncryptionSettings(
                        AutoEncryptionSettings.builder()
                                .keyVaultNamespace(dataKeysNamespace.getFullName())
                                .kmsProviders(kmsProviders)
                                .build())
                .build());
        // Use `encryptedClient` to drop `db.keyvault`.
        MongoDatabase encryptedDb = encryptedClient.getDatabase("db");
        MongoCollection<BsonDocument> encryptedCollection = encryptedDb
                .getCollection(dataKeysNamespace.getCollectionName(), BsonDocument.class)
                .withWriteConcern(WriteConcern.MAJORITY);
        encryptedCollection.drop();
        // Insert `<key-doc.json>` into `db.keyvault` with majority write concern.
        encryptedCollection.insertOne(bsonDocumentFromPath("key-doc.json"));

        // Use `encryptedClient` to drop and create the following collections:
        Arrays.asList("csfle", "csfle2", "qe", "qe2", "no_schema", "no_schema2").forEach(c -> {
            encryptedDb.getCollection(c).drop();
        });
        // create
        encryptedDb.createCollection("csfle", new CreateCollectionOptions()
                .validationOptions(new ValidationOptions()
                        .validator(new BsonDocument("$jsonSchema", bsonDocumentFromPath("schema-csfle.json")))));
        encryptedDb.createCollection("csfle2", new CreateCollectionOptions()
                .validationOptions(new ValidationOptions()
                        .validator(new BsonDocument("$jsonSchema", bsonDocumentFromPath("schema-csfle2.json")))));

        encryptedDb.createCollection("qe",
                new CreateCollectionOptions().encryptedFields(bsonDocumentFromPath("schema-qe.json")));
        encryptedDb.createCollection("qe2",
                new CreateCollectionOptions().encryptedFields(bsonDocumentFromPath("schema-qe2.json")));

        encryptedDb.createCollection("no_schema");
        encryptedDb.createCollection("no_schema2");

        // Insert documents with `encryptedClient`:
        Consumer<String> insert = (name) -> {
            encryptedDb.getCollection(name).insertOne(new Document(name, name));
        };
        insert.accept("csfle");
        insert.accept("csfle2");
        insert.accept("qe");
        insert.accept("qe2");
        insert.accept("no_schema");
        insert.accept("no_schema2");

        // Create an unencrypted MongoClient named `unencryptedClient`.
        MongoClient unencryptedClient = createMongoClient(getMongoClientSettingsBuilder().build());
        MongoDatabase unencryptedDb = unencryptedClient.getDatabase("db");

        Consumer<String> assertDocument = (name) -> {
            List<BsonDocument> pipeline = Arrays.asList(
                    BsonDocument.parse("{\"$project\" : {\"_id\" : 0, \"__safeContent__\" : 0}}")
            );
            Document decryptedDoc = encryptedDb.getCollection(name)
                    .aggregate(pipeline).first();
            assertEquals(decryptedDoc, new Document(name, name));
            Document encryptedDoc = unencryptedDb.getCollection(name)
                    .aggregate(pipeline).first();
            assertNotNull(encryptedDoc);
            assertEquals(Binary.class, encryptedDoc.get(name).getClass());
        };

        assertDocument.accept("csfle");
        assertDocument.accept("csfle2");
        assertDocument.accept("qe");
        assertDocument.accept("qe2");

        unencryptedClient.close();
        encryptedClient.close();

        client = createMongoClient(getMongoClientSettingsBuilder()
                .autoEncryptionSettings(
                        AutoEncryptionSettings.builder()
                                .keyVaultNamespace(dataKeysNamespace.getFullName())
                                .kmsProviders(kmsProviders)
                                .build())
                .build());
    }

    @AfterEach
    @SuppressWarnings("try")
    public void cleanUp() {
        //noinspection EmptyTryBlock
        try (MongoClient ignored = this.client) {
            // just using try-with-resources to ensure they all get closed, even in the case of exceptions
        }
    }

    @ParameterizedTest
    @CsvSource({
            "csfle, no_schema",
            "qe, no_schema",
            "no_schema, csfle",
            "no_schema, qe",
            "csfle, csfle2",
            "qe, qe2",
            "no_schema, no_schema2"})
    void cases1Through7(final String from, final String to) {
        assumeTrue(serverVersionAtLeast(8, 1));
        String mql = ("[\n"
                + "    {\"$match\" : {\"<from>\" : \"<from>\"}},\n"
                + "    {\n"
                + "        \"$lookup\" : {\n"
                + "            \"from\" : \"<to>\",\n"
                + "            \"as\" : \"matched\",\n"
                + "            \"pipeline\" : [ {\"$match\" : {\"<to>\" : \"<to>\"}}, {\"$project\" : {\"_id\" : 0, \"__safeContent__\" : 0}} ]\n"
                + "        }\n"
                + "    },\n"
                + "    {\"$project\" : {\"_id\" : 0, \"__safeContent__\" : 0}}\n"
                + "]").replace("<from>", from).replace("<to>", to);

        List<BsonDocument> pipeline = BsonArray.parse(mql).stream()
                .map(stage -> stage.asDocument())
                .collect(Collectors.toList());
        assertEquals(
                Document.parse("{\"<from>\" : \"<from>\", \"matched\" : [ {\"<to>\" : \"<to>\"} ]}"
                        .replace("<from>", from).replace("<to>", to)),
                client.getDatabase("db").getCollection(from).aggregate(pipeline).first());
    }

    @Test
    void case8() {
        assumeTrue(serverVersionAtLeast(8, 1));
        List<BsonDocument> pipeline = BsonArray.parse("[\n"
                + "    {\"$match\" : {\"csfle\" : \"qe\"}},\n"
                + "    {\n"
                + "        \"$lookup\" : {\n"
                + "            \"from\" : \"qe\",\n"
                + "            \"as\" : \"matched\",\n"
                + "            \"pipeline\" : [ {\"$match\" : {\"qe\" : \"qe\"}}, {\"$project\" : {\"_id\" : 0}} ]\n"
                + "        }\n"
                + "    },\n"
                + "    {\"$project\" : {\"_id\" : 0}}\n"
                + "]").stream().map(stage -> stage.asDocument()).collect(Collectors.toList());

        assertCause(
                MongoCryptException.class,
                "not supported",
                () -> client.getDatabase("db").getCollection("csfle").aggregate(pipeline).first());
    }

    @Test
    void case9() {
        assumeTrue(serverVersionLessThan(8, 1));
        List<BsonDocument> pipeline = BsonArray.parse("[\n"
                + "    {\"$match\" : {\"csfle\" : \"csfle\"}},\n"
                + "    {\n"
                + "        \"$lookup\" : {\n"
                + "            \"from\" : \"no_schema\",\n"
                + "            \"as\" : \"matched\",\n"
                + "            \"pipeline\" : [ {\"$match\" : {\"no_schema\" : \"no_schema\"}}, {\"$project\" : {\"_id\" : 0}} ]\n"
                + "        }\n"
                + "    },\n"
                + "    {\"$project\" : {\"_id\" : 0}}\n"
                + "]").stream().map(stage -> stage.asDocument()).collect(Collectors.toList());
        assertCause(
                RuntimeException.class,
                "Upgrade",
                () -> client.getDatabase("db").getCollection("csfle").aggregate(pipeline).first());
    }

    public static BsonDocument bsonDocumentFromPath(final String path) {
        try {
            return getTestDocument(new File(ClientSideEncryption25LookupProseTests.class
                    .getResource("/client-side-encryption-data/lookup/" + path).toURI()));
        } catch (Exception e) {
            fail("Unable to load resource", e);
            return null;
        }
    }
}
