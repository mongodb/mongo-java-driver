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
import com.mongodb.MongoCommandException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.ValidationOptions;
import com.mongodb.client.test.CollectionHelper;
import com.mongodb.event.CommandEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.internal.connection.TestCommandListener;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;
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

import static com.mongodb.ClusterFixture.hasEncryptionTestsEnabled;
import static com.mongodb.JsonTestServerVersionChecker.skipTest;
import static com.mongodb.client.CommandMonitoringTestHelper.assertEventsEquality;
import static com.mongodb.client.CommandMonitoringTestHelper.getExpectedEvents;
import static com.mongodb.client.CrudTestHelper.replaceTypeAssertionWithActual;
import static com.mongodb.client.Fixture.getMongoClient;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

// See https://github.com/mongodb/specifications/tree/master/source/client-side-encryption/tests
@RunWith(Parameterized.class)
public abstract class AbstractClientSideEncryptionTest {

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

    public AbstractClientSideEncryptionTest(final String filename, final String description, final BsonDocument specDocument,
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

    private boolean hasErrorField(@Nullable final BsonValue expectedResult, final String key) {
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
        assumeTrue("Client side encryption tests disabled", hasEncryptionTestsEnabled());
        assumeFalse("runOn requirements not satisfied", skipTest);
        assumeFalse("Skipping count tests", filename.startsWith("count."));
        assumeFalse(definition.getString("skipReason", new BsonString("")).getValue(), definition.containsKey("skipReason"));

        String databaseName = specDocument.getString("database_name").getValue();
        String collectionName = specDocument.getString("collection_name").getValue();
        collectionHelper = new CollectionHelper<BsonDocument>(new BsonDocumentCodec(), new MongoNamespace(databaseName, collectionName));
        MongoDatabase database = getMongoClient().getDatabase(databaseName);
        database.drop();

        /* Create the collection for auto encryption. */
        if (specDocument.containsKey("json_schema")) {
            database.createCollection(collectionName, new CreateCollectionOptions()
                    .validationOptions(new ValidationOptions()
                            .validator(new BsonDocument("$jsonSchema", specDocument.getDocument("json_schema")))));
        }

        /* Insert data into the collection */
        List<BsonDocument> documents = new ArrayList<BsonDocument>();
        if (!data.isEmpty()) {
            for (BsonValue document : data) {
                documents.add(document.asDocument());
            }
            database.getCollection(collectionName, BsonDocument.class).insertMany(documents);
        }

        /* Insert data into the "keyvault.datakeys" key vault. */
        BsonArray data = specDocument.getArray("key_vault_data", new BsonArray());
        MongoCollection<BsonDocument> collection = getMongoClient().getDatabase("keyvault")
                .getCollection("datakeys", BsonDocument.class)
                .withWriteConcern(WriteConcern.MAJORITY);
        collection.drop();
        if (!data.isEmpty()) {
            documents = new ArrayList<BsonDocument>();
            for (BsonValue document : data) {
                documents.add(document.asDocument());
            }
            collection.insertMany(documents);
        }

        commandListener = new TestCommandListener();

        BsonDocument clientOptions = definition.getDocument("clientOptions");
        BsonDocument cryptOptions = clientOptions.getDocument("autoEncryptOpts");
        BsonDocument kmsProviders = cryptOptions.getDocument("kmsProviders");
        boolean bypassAutoEncryption = cryptOptions.getBoolean("bypassAutoEncryption", BsonBoolean.FALSE).getValue();
        boolean bypassQueryAnalysis = cryptOptions.getBoolean("bypassQueryAnalysis", BsonBoolean.FALSE).getValue();

        Map<String, BsonDocument> namespaceToSchemaMap = new HashMap<String, BsonDocument>();

        if (cryptOptions.containsKey("schemaMap")) {
            BsonDocument autoEncryptMapDocument = cryptOptions.getDocument("schemaMap");
            for (Map.Entry<String, BsonValue> entries : autoEncryptMapDocument.entrySet()) {
                final BsonDocument autoEncryptOptionsDocument = entries.getValue().asDocument();
                namespaceToSchemaMap.put(entries.getKey(), autoEncryptOptionsDocument);
            }
        }

        Map<String, BsonDocument> encryptedFieldsMap = new HashMap<>();
        if (cryptOptions.containsKey("encryptedFieldsMap")) {
            BsonDocument encryptedFieldsMapDocument = cryptOptions.getDocument("encryptedFieldsMap");
            for (Map.Entry<String, BsonValue> entries : encryptedFieldsMapDocument.entrySet()) {
                encryptedFieldsMap.put(entries.getKey(), entries.getValue().asDocument());
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

        Map<String, Map<String, Object>> kmsProvidersMap = new HashMap<>();
        for (String kmsProviderKey : kmsProviders.keySet()) {
            BsonDocument kmsProviderOptions = kmsProviders.get(kmsProviderKey).asDocument();
            Map<String, Object> kmsProviderMap = new HashMap<>();
            kmsProvidersMap.put(kmsProviderKey.startsWith("aws") ? "aws" : kmsProviderKey, kmsProviderMap);
            switch (kmsProviderKey) {
                case "aws":
                    kmsProviderMap.put("accessKeyId", System.getProperty("org.mongodb.test.awsAccessKeyId"));
                    kmsProviderMap.put("secretAccessKey", System.getProperty("org.mongodb.test.awsSecretAccessKey"));
                    break;
                case "awsTemporary":
                    kmsProviderMap.put("accessKeyId", System.getProperty("org.mongodb.test.tmpAwsAccessKeyId"));
                    kmsProviderMap.put("secretAccessKey", System.getProperty("org.mongodb.test.tmpAwsSecretAccessKey"));
                    kmsProviderMap.put("sessionToken", System.getProperty("org.mongodb.test.tmpAwsSessionToken"));
                    break;
                case "awsTemporaryNoSessionToken":
                    kmsProviderMap.put("accessKeyId", System.getProperty("org.mongodb.test.tmpAwsAccessKeyId"));
                    kmsProviderMap.put("secretAccessKey", System.getProperty("org.mongodb.test.tmpAwsSecretAccessKey"));
                    break;
                case "azure":
                    kmsProviderMap.put("tenantId", System.getProperty("org.mongodb.test.azureTenantId"));
                    kmsProviderMap.put("clientId", System.getProperty("org.mongodb.test.azureClientId"));
                    kmsProviderMap.put("clientSecret", System.getProperty("org.mongodb.test.azureClientSecret"));
                    break;
                case "gcp":
                    kmsProviderMap.put("email", System.getProperty("org.mongodb.test.gcpEmail"));
                    kmsProviderMap.put("privateKey", System.getProperty("org.mongodb.test.gcpPrivateKey"));
                    break;
                case "kmip":
                    kmsProviderMap.put("endpoint", System.getProperty("org.mongodb.test.kmipEndpoint", "localhost:5698"));
                    break;
                case "local":
                    kmsProviderMap.put("key", kmsProviderOptions.getBinary("key").getData());
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported KMS provider: " + kmsProviderKey);
            }
        }

        String keyVaultNamespace = "keyvault.datakeys";
        if (cryptOptions.containsKey("keyVaultNamespace")) {
            keyVaultNamespace = cryptOptions.getString("keyVaultNamespace").getValue();
        }

        createMongoClient(AutoEncryptionSettings.builder()
                .keyVaultNamespace(keyVaultNamespace)
                .kmsProviders(kmsProvidersMap)
                .schemaMap(namespaceToSchemaMap)
                .encryptedFieldsMap(encryptedFieldsMap)
                .bypassQueryAnalysis(bypassQueryAnalysis)
                .bypassAutoEncryption(bypassAutoEncryption)
                .extraOptions(extraOptions)
                .build(), commandListener);

        database = getDatabase(databaseName);
        helper = new JsonPoweredCrudTestHelper(description, database, database.getCollection(collectionName, BsonDocument.class));
    }

    protected abstract void createMongoClient(AutoEncryptionSettings build, CommandListener commandListener);

    protected abstract MongoDatabase getDatabase(String databaseName);


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
                boolean fle2Test = file.getName().startsWith("fle2"); // TODO enable fle2 test JAVA-4589
                for (BsonValue test : specDocument.getArray("tests")) {
                    data.add(new Object[]{file.getName(), test.asDocument().getString("description").getValue(), specDocument,
                            specDocument.getArray("data", new BsonArray()), test.asDocument(),
                            fle2Test || skipTest(specDocument, test.asDocument())});
                }
        }
        return data;
    }
}
