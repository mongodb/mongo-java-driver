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
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.ValidationOptions;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.client.vault.ClientEncryptions;
import com.mongodb.crypt.capi.MongoCryptException;
import com.mongodb.event.CommandEvent;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import com.mongodb.fixture.EncryptionFixture;
import com.mongodb.internal.connection.TestCommandListener;
import com.mongodb.internal.crypt.capi.CAPI;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.isStandalone;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static com.mongodb.fixture.EncryptionFixture.getKmsProviders;
import static com.mongodb.internal.connection.OidcAuthenticationProseTests.assertCause;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static util.JsonPoweredTestHelper.getTestDocument;

public class ClientSideEncryption25Lookup {
    private MongoClient client;
    private TestCommandListener listener;

    protected MongoClient createMongoClient(final MongoClientSettings settings) {
        return MongoClients.create(settings);
    }

    protected ClientEncryption createClientEncryption(final ClientEncryptionSettings settings) {
        return ClientEncryptions.create(settings);
    }

    @BeforeEach
    public void setUp() throws InterruptedException {
        // TODO remove debug:
        TestCommandListener commandListener = new TestCommandListener();
        TestCommandListener commandListener2 = new TestCommandListener();
                                
        
        assumeTrue(serverVersionAtLeast(7, 0));
        assumeFalse(isStandalone());

        // Create an unencrypted MongoClient.
        MongoClient unencryptedClient = createMongoClient(getMongoClientSettingsBuilder()
                .addCommandListener(commandListener)
                .build());

        // drop database db
        MongoDatabase db = unencryptedClient.getDatabase("db");
        db.drop();

        // Insert <key-doc.json> into db.keyvault.
        MongoNamespace dataKeysNamespace = new MongoNamespace("db.keyvault");
        db.getCollection(dataKeysNamespace.getCollectionName(), BsonDocument.class)
                .insertOne(bsonDocumentFromPath("key-doc.json"));

        // Create the following collections:
        db.createCollection("csfle", new CreateCollectionOptions()
                .validationOptions(new ValidationOptions()
                        .validator(new BsonDocument("$jsonSchema", bsonDocumentFromPath("schema-csfle.json")))));
        db.createCollection("csfle2", new CreateCollectionOptions()
                .validationOptions(new ValidationOptions()
                        .validator(new BsonDocument("$jsonSchema", bsonDocumentFromPath("schema-csfle2.json")))));

        db.createCollection("qe",
                new CreateCollectionOptions().encryptedFields(bsonDocumentFromPath("schema-qe.json")));
        db.createCollection("qe2",
                new CreateCollectionOptions().encryptedFields(bsonDocumentFromPath("schema-qe2.json")));

        db.createCollection("no_schema");
        db.createCollection("no_schema2");

        // Create an encrypted MongoClient configured with:
        Map<String, Map<String, Object>> kmsProviders = getKmsProviders(EncryptionFixture.KmsProviderType.LOCAL);
        MongoClient encryptedClient = createMongoClient(getMongoClientSettingsBuilder()
                .addCommandListener(commandListener2)
                .autoEncryptionSettings(
                        AutoEncryptionSettings.builder()
                                .keyVaultNamespace(dataKeysNamespace.getFullName())
                                .kmsProviders(kmsProviders)
                                .build())
                .build());

        // Insert documents with the encrypted MongoClient:
        Consumer<String> insert = (name) -> {
            encryptedClient.getDatabase("db").getCollection(name)
                    .insertOne(new Document(name, name));
        };
        insert.accept("csfle");
        insert.accept("csfle2");
        insert.accept("qe");
        insert.accept("qe2");
        insert.accept("no_schema");
        insert.accept("no_schema2");

        Consumer<String> assertDocument = (name) -> {
            List<BsonDocument> pipeline = Arrays.asList(
                    BsonDocument.parse("{\"$project\" : {\"_id\" : 0, \"__safeContent__\" : 0}}")
            );
            Document decryptedDoc = encryptedClient.getDatabase("db").getCollection(name)
                    .aggregate(pipeline).first();
            assertEquals(decryptedDoc, new Document(name, name));
            Document encryptedDoc = unencryptedClient.getDatabase("db").getCollection(name)
                    .aggregate(pipeline).first();
            assertNotNull(encryptedDoc);
            assertEquals(Binary.class, encryptedDoc.get(name).getClass());

            // TODO
            System.out.println("ENC-DOC:  " + encryptedDoc.toJson());
            System.out.println("DEC-DOC: " + decryptedDoc.toJson());
            System.out.println("VERSION: " + CAPI.mongocrypt_version(null).toString());
        };

        assertDocument.accept("csfle");
        assertDocument.accept("csfle2");
        assertDocument.accept("qe");
        assertDocument.accept("qe2");

        // TODO need registration mechanism for shutdown hook?
        unencryptedClient.close();
        encryptedClient.close();

        List<CommandEvent> events = commandListener2.getEvents();
        System.out.println(">> " + events);

        List<CommandStartedEvent> events2 = commandListener2.getCommandStartedEvents();
        String collect = events2.stream()
                .map(v -> v.getCommand().toJson())
                .collect(Collectors.joining("\n"));
        System.out.println(">> " + collect);

        listener = new TestCommandListener();
        client = createMongoClient(getMongoClientSettingsBuilder()
                .addCommandListener(listener)
                .autoEncryptionSettings(
                        AutoEncryptionSettings.builder()
                                .keyVaultNamespace(dataKeysNamespace.getFullName())
                                .kmsProviders(kmsProviders)
                                .build())
                .build());
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    private static <K, V> Map<K, V> merge( final Map.Entry<K, V>... entries) {
        HashMap<K, V> result = new HashMap<>();
        result.putAll(Stream.of(entries).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
        return result;
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
    public void case0(final String from, final String to) {

        String mql = ("[\n"
                + "    {\"$match\" : {\"<from>\" : \"<from>\"}},\n"
                + "    {\n"
                + "        \"$lookup\" : {\n"
                + "            \"from\" : \"<to>\",\n"
                + "            \"as\" : \"matched\",\n"
                + "            \"pipeline\" : [ {\"$match\" : {\"<to>\" : \"<to>\"}}, {\"$project\" : {\"_id\" : 0}} ]\n"
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
    public void case1() {
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
        assertEquals(
                Document.parse("{\"csfle\" : \"csfle\", \"matched\" : [ {\"no_schema\" : \"no_schema\"} ]}"),
                client.getDatabase("db").getCollection("csfle").aggregate(pipeline).first());
    }

    @Test
    public void case2() {
        List<BsonDocument> pipeline = BsonArray.parse("[\n"
                + "    {\"$match\" : {\"qe\" : \"qe\"}},\n"
                + "    {\n"
                + "       \"$lookup\" : {\n"
                + "          \"from\" : \"no_schema\",\n"
                + "          \"as\" : \"matched\",\n"
                + "          \"pipeline\" :\n"
                + "             [ {\"$match\" : {\"no_schema\" : \"no_schema\"}}, {\"$project\" : {\"_id\" : 0, \"__safeContent__\" : 0}} ]\n"
                + "       }\n"
                + "    },\n"
                + "    {\"$project\" : {\"_id\" : 0, \"__safeContent__\" : 0}}\n"
                + "]").stream().map(stage -> stage.asDocument()).collect(Collectors.toList());
        assertEquals(
                Document.parse("{\"qe\" : \"qe\", \"matched\" : [ {\"no_schema\" : \"no_schema\"} ]}"),
                client.getDatabase("db").getCollection("qe").aggregate(pipeline).first());
    }

    @Test
    public void case3() {
        List<BsonDocument> pipeline = BsonArray.parse("[\n"
                + "    {\"$match\" : {\"no_schema\" : \"no_schema\"}},\n"
                + "    {\n"
                + "        \"$lookup\" : {\n"
                + "            \"from\" : \"csfle\",\n"
                + "            \"as\" : \"matched\",\n"
                + "            \"pipeline\" : [ {\"$match\" : {\"csfle\" : \"csfle\"}}, {\"$project\" : {\"_id\" : 0}} ]\n"
                + "        }\n"
                + "    },\n"
                + "    {\"$project\" : {\"_id\" : 0}}\n"
                + "]").stream().map(stage -> stage.asDocument()).collect(Collectors.toList());
        assertEquals(
                Document.parse("{\"no_schema\" : \"no_schema\", \"matched\" : [ {\"csfle\" : \"csfle\"} ]}"),
                client.getDatabase("db").getCollection("no_schema").aggregate(pipeline).first());
    }

    @Test
    public void case4() {
        List<BsonDocument> pipeline = BsonArray.parse("[\n"
                + "   {\"$match\" : {\"no_schema\" : \"no_schema\"}},\n"
                + "   {\n"
                + "      \"$lookup\" : {\n"
                + "         \"from\" : \"qe\",\n"
                + "         \"as\" : \"matched\",\n"
                + "         \"pipeline\" : [ {\"$match\" : {\"qe\" : \"qe\"}}, {\"$project\" : {\"_id\" : 0, \"__safeContent__\" : 0}} ]\n"
                + "      }\n"
                + "   },\n"
                + "   {\"$project\" : {\"_id\" : 0}}\n"
                + "]").stream().map(stage -> stage.asDocument()).collect(Collectors.toList());

        Document first = client.getDatabase("db").getCollection("no_schema").aggregate(pipeline).first();

        List<CommandEvent> events2 = listener.getEvents();
        String collect = events2.stream()
                .map(v -> {
                    if (v instanceof CommandStartedEvent) {
                        return "STARTED: " + ((CommandStartedEvent) v).getCommand().toJson();
                    } else if (v instanceof CommandSucceededEvent) {
                        return "SUCCEEDED: " + ((CommandSucceededEvent) v).getResponse().toJson();
                    } else {
                        return "";
                    }
                })
                .collect(Collectors.joining("\n"));
        System.out.println("RESULTS: " + collect);
        // TODO: remove debugging

        assertEquals(
                Document.parse("{\"no_schema\" : \"no_schema\", \"matched\" : [ {\"qe\" : \"qe\"} ]}"),
                first);
    }

    @Test
    public void case5() {
        List<BsonDocument> pipeline = BsonArray.parse("[\n"
                + "   {\"$match\" : {\"csfle\" : \"csfle\"}},\n"
                + "   {\n"
                + "      \"$lookup\" : {\n"
                + "         \"from\" : \"csfle2\",\n"
                + "         \"as\" : \"matched\",\n"
                + "         \"pipeline\" : [ {\"$match\" : {\"csfle2\" : \"csfle2\"}}, {\"$project\" : {\"_id\" : 0}} ]\n"
                + "      }\n"
                + "   },\n"
                + "   {\"$project\" : {\"_id\" : 0}}\n"
                + "]").stream().map(stage -> stage.asDocument()).collect(Collectors.toList());

        assertEquals(
                Document.parse("{\"csfle\" : \"csfle\", \"matched\" : [ {\"csfle2\" : \"csfle2\"} ]}"),
                client.getDatabase("db").getCollection("csfle").aggregate(pipeline).first());
    }

    @Test
    public void case6() {
        List<BsonDocument> pipeline = BsonArray.parse("[\n"
                + "   {\"$match\" : {\"qe\" : \"qe\"}},\n"
                + "   {\n"
                + "      \"$lookup\" : {\n"
                + "         \"from\" : \"qe2\",\n"
                + "         \"as\" : \"matched\",\n"
                + "         \"pipeline\" : [ {\"$match\" : {\"qe2\" : \"qe2\"}}, {\"$project\" : {\"_id\" : 0, \"__safeContent__\" : 0}} ]\n"
                + "      }\n"
                + "   },\n"
                + "   {\"$project\" : {\"_id\" : 0, \"__safeContent__\" : 0}}\n"
                + "]").stream().map(stage -> stage.asDocument()).collect(Collectors.toList());

        assertEquals(
                Document.parse("{\"qe\" : \"qe\", \"matched\" : [ {\"qe2\" : \"qe2\"} ]}"),
                client.getDatabase("db").getCollection("qe").aggregate(pipeline).first());
    }

    @Test
    public void case7() {
        List<BsonDocument> pipeline = BsonArray.parse("[\n"
                + "    {\"$match\" : {\"no_schema\" : \"no_schema\"}},\n"
                + "    {\n"
                + "        \"$lookup\" : {\n"
                + "            \"from\" : \"no_schema2\",\n"
                + "            \"as\" : \"matched\",\n"
                + "            \"pipeline\" : [ {\"$match\" : {\"no_schema2\" : \"no_schema2\"}}, {\"$project\" : {\"_id\" : 0}} ]\n"
                + "        }\n"
                + "    },\n"
                + "    {\"$project\" : {\"_id\" : 0}}\n"
                + "]").stream().map(stage -> stage.asDocument()).collect(Collectors.toList());

        assertEquals(
                Document.parse("{\"no_schema\" : \"no_schema\", \"matched\" : [ {\"no_schema2\" : \"no_schema2\"} ]}"),
                client.getDatabase("db").getCollection("no_schema").aggregate(pipeline).first());
    }

    @Test
    public void case8() {
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
    public void case9() {
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
        // TODO
    }

    public static BsonDocument bsonDocumentFromPath(final String path) {
        try {
            return getTestDocument(new File(ClientSideEncryption25Lookup.class
                    .getResource("/client-side-encryption-data/lookup/" + path).toURI()));
        } catch (Exception e) {
            fail("Unable to load resource", e);
            return null;
        }
    }
}
