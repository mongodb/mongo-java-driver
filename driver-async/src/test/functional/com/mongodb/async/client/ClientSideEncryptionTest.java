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

package com.mongodb.async.client;

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.async.FutureResultCallback;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.ValidationOptions;
import com.mongodb.client.test.CollectionHelper;
import com.mongodb.event.CommandEvent;
import com.mongodb.internal.connection.TestCommandListener;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import util.JsonPoweredTestHelper;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.isNotAtLeastJava8;
import static com.mongodb.JsonTestServerVersionChecker.skipTest;
import static com.mongodb.async.client.Fixture.getMongoClient;
import static com.mongodb.client.CommandMonitoringTestHelper.assertEventsEquality;
import static com.mongodb.client.CommandMonitoringTestHelper.getExpectedEvents;
import static com.mongodb.client.CrudTestHelper.replaceTypeAssertionWithActual;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

// See https://github.com/mongodb/specifications/tree/master/source/client-side-encryption/tests
@RunWith(Parameterized.class)
public class ClientSideEncryptionTest {

    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private final String filename;
    private final BsonDocument specDocument;
    private final String description;
    private final BsonArray data;
    private final BsonDocument definition;
    private final boolean skipTest;
    private JsonPoweredCrudTestHelper helper;
    private TestCommandListener commandListener;
    private CollectionHelper<BsonDocument> collectionHelper;
    private MongoClient mongoClient;

    public ClientSideEncryptionTest(final String filename, final String description, final BsonDocument specDocument,
                                    final BsonArray data, final BsonDocument definition, final boolean skipTest) {
        this.filename = filename;
        this.specDocument = specDocument;
        this.description = description;
        this.data = data;
        this.definition = definition;
        this.skipTest = skipTest;
    }

    private boolean hasErrorContainsField(final BsonValue expectedResult) {
        return hasErrorField(expectedResult, "errorContains");
    }

    private boolean hasErrorCodeNameField(final BsonValue expectedResult) {
        return hasErrorField(expectedResult, "errorCodeName");
    }

    private boolean hasErrorField(final BsonValue expectedResult, final String key) {
        return expectedResult != null && expectedResult.isDocument() && expectedResult.asDocument().containsKey(key);
    }

    private String getErrorField(final BsonValue expectedResult, final String key) {
        if (hasErrorField(expectedResult, key)) {
            return expectedResult.asDocument().getString(key).getValue();
        } else {
            return "";
        }
    }

    private String getErrorContainsField(final BsonValue expectedResult) {
        return getErrorField(expectedResult, "errorContains");
    }

    private String getErrorCodeNameField(final BsonValue expectedResult) {
        return getErrorField(expectedResult, "errorCodeName");
    }


    @Before
    public void setUp() {
        assumeTrue("Client side encryption tests disabled",
                System.getProperty("org.mongodb.test.awsAccessKeyId") != null
                        && !System.getProperty("org.mongodb.test.awsAccessKeyId").isEmpty());
        assumeFalse("Client side encryption requires Java 8+", isNotAtLeastJava8());
        assumeFalse("runOn requirements not satisfied", skipTest);
        assumeFalse(definition.getString("skipReason", new BsonString("")).getValue(), definition.containsKey("skipReason"));

        String databaseName = specDocument.getString("database_name").getValue();
        String collectionName = specDocument.getString("collection_name").getValue();
        collectionHelper = new CollectionHelper<BsonDocument>(new BsonDocumentCodec(), new MongoNamespace(databaseName, collectionName));
        MongoDatabase database = getMongoClient().getDatabase(databaseName);
        MongoCollection<BsonDocument> collection = database.getCollection(collectionName, BsonDocument.class);

        FutureResultCallback<Void> callback = new FutureResultCallback<Void>();
        collection.drop(callback);
        callback.get(30, TimeUnit.SECONDS);

        /* Create the collection for auto encryption. */
        if (specDocument.containsKey("json_schema")) {
            callback = new FutureResultCallback<Void>();
            database.createCollection(collectionName, new CreateCollectionOptions()
                    .validationOptions(new ValidationOptions()
                            .validator(new BsonDocument("$jsonSchema", specDocument.getDocument("json_schema")))), callback);
            callback.get(30, TimeUnit.SECONDS);
        }

        /* Insert data into the collection */
        List<BsonDocument> documents = new ArrayList<BsonDocument>();
        if (!data.isEmpty()) {
            for (BsonValue document : data) {
                documents.add(document.asDocument());
            }
            callback = new FutureResultCallback<Void>();
            database.getCollection(collectionName, BsonDocument.class).insertMany(documents, callback);
            callback.get(30, TimeUnit.SECONDS);
        }

        /* Insert data into the "admin.datakeys" key vault. */
        collection = getMongoClient().getDatabase("admin").getCollection("datakeys", BsonDocument.class);
        callback = new FutureResultCallback<Void>();
        collection.drop(callback);
        callback.get(30, TimeUnit.SECONDS);

        BsonArray data = specDocument.getArray("key_vault_data", new BsonArray());
        if (!data.isEmpty()) {
            documents = new ArrayList<BsonDocument>();
            for (BsonValue document : data) {
                documents.add(document.asDocument());
            }
            callback = new FutureResultCallback<Void>();
            collection.insertMany(documents, callback);
            callback.get(30, TimeUnit.SECONDS);
        }

        commandListener = new TestCommandListener();
        BsonDocument clientOptions = definition.getDocument("clientOptions");
        BsonDocument cryptOptions = clientOptions.getDocument("autoEncryptOpts");
        BsonDocument kmsProviders = cryptOptions.getDocument("kmsProviders");
        boolean bypassAutoEncryption = cryptOptions.getBoolean("bypassAutoEncryption", BsonBoolean.FALSE).getValue();

        Map<String, BsonDocument> namespaceToSchemaMap = new HashMap<String, BsonDocument>();
        if (cryptOptions.containsKey("schemaMap")) {
            BsonDocument autoEncryptMapDocument = cryptOptions.getDocument("schemaMap");

            for (Map.Entry<String, BsonValue> entries : autoEncryptMapDocument.entrySet()) {
                final BsonDocument autoEncryptOptionsDocument = entries.getValue().asDocument();
                namespaceToSchemaMap.put(entries.getKey(), autoEncryptOptionsDocument);
            }
        }

        Map<String, Object> extraOptions = new HashMap<String, Object>();
        if (cryptOptions.containsKey("extraOptions")) {
            BsonDocument extraOptionsDocument = cryptOptions.getDocument("extraOptions");
            if (extraOptionsDocument.containsKey("mongocryptdSpawnArgs")) {
                List<String> mongocryptdSpawnArgsValue = new ArrayList<String>();
                for (BsonValue cur: extraOptionsDocument.getArray("mongocryptdSpawnArgs")) {
                    mongocryptdSpawnArgsValue.add(cur.asString().getValue());
                }
                extraOptions.put("mongocryptdSpawnArgs", mongocryptdSpawnArgsValue);
            }
            if (extraOptionsDocument.containsKey("mongocryptdBypassSpawn")) {
                extraOptions.put("mongocryptdBypassSpawn", extraOptionsDocument.getBoolean("mongocryptdBypassSpawn").getValue());
            }
            if (extraOptionsDocument.containsKey("mongocryptdURI")) {
                extraOptions.put("mongocryptdURI", extraOptionsDocument.getString("mongocryptdURI").getValue());
            }
        }

        if (System.getProperty("org.mongodb.test.mongocryptdSpawnPath") != null) {
            extraOptions.put("mongocryptdSpawnPath", System.getProperty("org.mongodb.test.mongocryptdSpawnPath"));
        }

        Map<String, Map<String, Object>> kmsProvidersMap = new HashMap<String, Map<String, Object>>();

        for (String kmsProviderKey : kmsProviders.keySet()) {
            BsonDocument kmsProviderOptions = kmsProviders.get(kmsProviderKey).asDocument();
            Map<String, Object> kmsProviderMap = new HashMap<String, Object>();

            if (kmsProviderKey.equals("aws")) {
                kmsProviderMap.put("accessKeyId", System.getProperty("org.mongodb.test.awsAccessKeyId"));
                kmsProviderMap.put("secretAccessKey", System.getProperty("org.mongodb.test.awsSecretAccessKey"));
                kmsProvidersMap.put("aws", kmsProviderMap);
            } else if (kmsProviderKey.equals("local")) {
                kmsProviderMap.put("key", kmsProviderOptions.getBinary("key").getData());
                kmsProvidersMap.put("local", kmsProviderMap);
            }
        }

        String keyVaultNamespace = "admin.datakeys";
        if (cryptOptions.containsKey("keyVaultNamespace")) {
            keyVaultNamespace = cryptOptions.getString("keyVaultNamespace").getValue();
        }

        mongoClient = MongoClients.create(Fixture.getMongoClientBuilderFromConnectionString()
                .autoEncryptionSettings(AutoEncryptionSettings.builder()
                        .keyVaultNamespace(keyVaultNamespace)
                        .kmsProviders(kmsProvidersMap)
                        .schemaMap(namespaceToSchemaMap)
                        .bypassAutoEncryption(bypassAutoEncryption)
                        .extraOptions(extraOptions)
                        .build())
                .addCommandListener(commandListener)
                .build());

        database = mongoClient.getDatabase(databaseName);
        helper = new JsonPoweredCrudTestHelper(description, database, database.getCollection("default", BsonDocument.class));
    }

    @After
    public void cleanUp() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    @Test
    public void shouldPassAllOutcomes() {
        for (BsonValue cur : definition.getArray("operations")) {
            BsonDocument operation = cur.asDocument();
            String operationName = operation.getString("name").getValue();
            BsonValue expectedResult = operation.get("result");
            try {
                BsonDocument actualOutcome = helper.getOperationResults(operation);
                if (expectedResult != null) {
                    BsonValue actualResult = actualOutcome.get("result");
                    assertEquals("Expected operation result differs from actual", expectedResult, actualResult);
                }

                assertFalse(String.format("Expected error '%s' but none thrown for operation %s",
                        getErrorContainsField(expectedResult), operationName), hasErrorContainsField(expectedResult));
                assertFalse(String.format("Expected error code '%s' but none thrown for operation %s",
                        getErrorCodeNameField(expectedResult), operationName), hasErrorCodeNameField(expectedResult));
            } catch (RuntimeException e) {
                boolean passedAssertion = false;
                if (hasErrorContainsField(expectedResult)) {
                    String expectedError = getErrorContainsField(expectedResult);
                    assertTrue(String.format("Expected '%s' but got '%s' for operation %s", expectedError, e.getMessage(),
                            operationName), e.getMessage().toLowerCase().contains(expectedError.toLowerCase()));
                    passedAssertion = true;
                }
                if (hasErrorCodeNameField(expectedResult)) {
                    String expectedErrorCodeName = getErrorCodeNameField(expectedResult);
                    if (e instanceof MongoCommandException) {
                        assertEquals(expectedErrorCodeName, ((MongoCommandException) e).getErrorCodeName());
                        passedAssertion = true;
                    } else if (e instanceof MongoWriteConcernException) {
                        assertEquals(expectedErrorCodeName, ((MongoWriteConcernException) e).getWriteConcernError().getCodeName());
                        passedAssertion = true;
                    }
                }
                if (!passedAssertion) {
                    throw e;
                }
            }

        }

        if (definition.containsKey("expectations")) {
            List<CommandEvent> expectedEvents = getExpectedEvents(definition.getArray("expectations"), "default", null);
            List<CommandEvent> events = commandListener.getCommandStartedEvents();

            assertEventsEquality(expectedEvents, events);
        }

        BsonDocument expectedOutcome = definition.getDocument("outcome", new BsonDocument());
        if (expectedOutcome.containsKey("collection")) {
            List<BsonDocument> collectionData = collectionHelper.find();
            List<BsonValue> expectedData = expectedOutcome.getDocument("collection").getArray("data").getValues();
            assertEquals(collectionData.size(), expectedData.size());
            int count = collectionData.size();
            for (int i = 0; i < count; i++) {
                BsonDocument actual = collectionData.get(i);
                BsonDocument expected = expectedData.get(i).asDocument();
                replaceTypeAssertionWithActual(expected, actual);
                assertEquals(expected, actual);
            }
        }
    }

    @Parameterized.Parameters(name = "{0}: {1}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        List<Object[]> data = new ArrayList<Object[]>();
        for (File file : JsonPoweredTestHelper.getTestFiles("/client-side-encryption")) {
            BsonDocument specDocument = JsonPoweredTestHelper.getTestDocument(file);
            for (BsonValue test : specDocument.getArray("tests")) {
                data.add(new Object[]{file.getName(), test.asDocument().getString("description").getValue(), specDocument,
                        specDocument.getArray("data", new BsonArray()), test.asDocument(), skipTest(specDocument, test.asDocument())});
            }
        }
        return data;
    }
}
