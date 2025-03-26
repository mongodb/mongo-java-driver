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
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.ValidationOptions;
import com.mongodb.client.test.CollectionHelper;
import com.mongodb.event.CommandEvent;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.internal.connection.TestCommandListener;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonUndefined;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import util.JsonPoweredTestHelper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.getEnv;
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

    @SuppressWarnings({"unused"})
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

    protected BsonDocument getDefinition() {
        return definition;
    }


    private boolean hasTimeoutError(@Nullable final BsonValue expectedResult) {
        return hasErrorField(expectedResult, "isTimeoutError");
    }

    private boolean hasErrorContainsField(@Nullable final BsonValue expectedResult) {
        return hasErrorField(expectedResult, "errorContains");
    }

    private boolean hasErrorCodeNameField(@Nullable final BsonValue expectedResult) {
        return hasErrorField(expectedResult, "errorCodeName");
    }

    private boolean hasErrorField(@Nullable final BsonValue expectedResult, final String key) {
        return expectedResult != null && expectedResult.isDocument() && expectedResult.asDocument().containsKey(key);
    }

    private String getErrorField(@Nullable final BsonValue expectedResult, final String key) {
        if (hasErrorField(expectedResult, key)) {
            return expectedResult.asDocument().getString(key).getValue();
        } else {
            return "";
        }
    }

    private String getErrorContainsField(@Nullable final BsonValue expectedResult) {
        return getErrorField(expectedResult, "errorContains");
    }

    private String getErrorCodeNameField(@Nullable final BsonValue expectedResult) {
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
        collectionHelper = new CollectionHelper<>(new BsonDocumentCodec(), new MongoNamespace(databaseName, collectionName));
        MongoDatabase database = getMongoClient().getDatabase(databaseName);
        database.drop();

        /* Create the collection for auto encryption. */
        CreateCollectionOptions createCollectionOptions = new CreateCollectionOptions();
        if (specDocument.containsKey("json_schema")) {
            createCollectionOptions.validationOptions(new ValidationOptions()
                            .validator(new BsonDocument("$jsonSchema", specDocument.getDocument("json_schema"))));
        }
        if (specDocument.containsKey("encrypted_fields")) {
            createCollectionOptions.encryptedFields(specDocument.getDocument("encrypted_fields"));
        }
        database.createCollection(collectionName, createCollectionOptions);

        /* Insert data into the collection */
        List<BsonDocument> documents = new ArrayList<>();
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
            documents = new ArrayList<>();
            for (BsonValue document : data) {
                documents.add(document.asDocument());
            }
            collection.insertMany(documents);
        }

        commandListener = new TestCommandListener();
        BsonDocument clientOptions = definition.getDocument("clientOptions", new BsonDocument());
        BsonDocument cryptOptions = clientOptions.getDocument("autoEncryptOpts", new BsonDocument());
        BsonDocument kmsProviders = cryptOptions.getDocument("kmsProviders", new BsonDocument());
        boolean bypassAutoEncryption = cryptOptions.getBoolean("bypassAutoEncryption", BsonBoolean.FALSE).getValue();
        boolean bypassQueryAnalysis = cryptOptions.getBoolean("bypassQueryAnalysis", BsonBoolean.FALSE).getValue();

        Map<String, BsonDocument> namespaceToSchemaMap = new HashMap<>();

        if (cryptOptions.containsKey("schemaMap")) {
            BsonDocument autoEncryptMapDocument = cryptOptions.getDocument("schemaMap");
            for (Map.Entry<String, BsonValue> entries : autoEncryptMapDocument.entrySet()) {
                BsonDocument autoEncryptOptionsDocument = entries.getValue().asDocument();
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

        Map<String, Object> extraOptions = new HashMap<>();
        cryptSharedLibPathSysPropValue().ifPresent(path -> extraOptions.put("cryptSharedLibPath", path));
        if (cryptOptions.containsKey("extraOptions")) {
            BsonDocument extraOptionsDocument = cryptOptions.getDocument("extraOptions");
            if (extraOptionsDocument.containsKey("mongocryptdSpawnArgs")) {
                List<String> mongocryptdSpawnArgsValue = new ArrayList<>();
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
                    kmsProviderMap.put("accessKeyId", getEnv("AWS_ACCESS_KEY_ID"));
                    kmsProviderMap.put("secretAccessKey", getEnv("AWS_SECRET_ACCESS_KEY"));
                    break;
                case "awsTemporary":
                    kmsProviderMap.put("accessKeyId", getEnv("AWS_TEMP_ACCESS_KEY_ID"));
                    kmsProviderMap.put("secretAccessKey", getEnv("AWS_TEMP_SECRET_ACCESS_KEY"));
                    kmsProviderMap.put("sessionToken", getEnv("AWS_TEMP_SESSION_TOKEN"));
                    break;
                case "awsTemporaryNoSessionToken":
                    kmsProviderMap.put("accessKeyId", getEnv("AWS_TEMP_ACCESS_KEY_ID"));
                    kmsProviderMap.put("secretAccessKey", getEnv("AWS_TEMP_SECRET_ACCESS_KEY"));
                    break;
                case "azure":
                    kmsProviderMap.put("tenantId", getEnv("AZURE_TENANT_ID"));
                    kmsProviderMap.put("clientId", getEnv("AZURE_CLIENT_ID"));
                    kmsProviderMap.put("clientSecret", getEnv("AZURE_CLIENT_SECRET"));
                    break;
                case "gcp":
                    kmsProviderMap.put("email", getEnv("GCP_EMAIL"));
                    kmsProviderMap.put("privateKey", getEnv("GCP_PRIVATE_KEY"));
                    break;
                case "kmip":
                    kmsProviderMap.put("endpoint", getEnv("org.mongodb.test.kmipEndpoint", "localhost:5698"));
                    break;
                case "local":
                case "local:name2":
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

        MongoClientSettings.Builder mongoClientSettingsBuilder = Fixture.getMongoClientSettingsBuilder()
                        .addCommandListener(commandListener);

        if (clientOptions.containsKey("timeoutMS")) {
            long timeoutMs = clientOptions.getInt32("timeoutMS").longValue();
            mongoClientSettingsBuilder.timeout(timeoutMs, TimeUnit.MILLISECONDS);
        }

        if (!kmsProvidersMap.isEmpty()) {
            mongoClientSettingsBuilder.autoEncryptionSettings(AutoEncryptionSettings.builder()
                    .keyVaultNamespace(keyVaultNamespace)
                    .kmsProviders(kmsProvidersMap)
                    .schemaMap(namespaceToSchemaMap)
                    .encryptedFieldsMap(encryptedFieldsMap)
                    .bypassQueryAnalysis(bypassQueryAnalysis)
                    .bypassAutoEncryption(bypassAutoEncryption)
                    .extraOptions(extraOptions)
                    .build());
        }
        createMongoClient(mongoClientSettingsBuilder.build());
        database = getDatabase(databaseName);
        helper = new JsonPoweredCrudTestHelper(description, database, database.getCollection(collectionName, BsonDocument.class));

        if (definition.containsKey("failPoint")) {
            collectionHelper.runAdminCommand(definition.getDocument("failPoint"));
        }
    }

    @After
    public void cleanUp() {
        if (collectionHelper != null && definition.containsKey("failPoint")) {
            collectionHelper.runAdminCommand(new BsonDocument("configureFailPoint",
                    definition.getDocument("failPoint").getString("configureFailPoint"))
                    .append("mode", new BsonString("off")));
        }
    }

    protected abstract void createMongoClient(MongoClientSettings settings);

    protected abstract MongoDatabase getDatabase(String databaseName);


    @Test
    public void shouldPassAllOutcomes() {
        for (BsonValue cur : definition.getArray("operations")) {
            BsonDocument operation = cur.asDocument();
            String operationName = operation.getString("name").getValue();
            BsonValue expectedResult = operation.get("result");
            try {
                BsonDocument actualOutcome = helper.getOperationResults(operation);
                assertFalse(String.format("Expected a timeout error but got: %s", actualOutcome.toJson()), hasTimeoutError(expectedResult));

                if (expectedResult != null) {
                    BsonValue actualResult = actualOutcome.get("result", new BsonString("No result or error"));
                    assertBsonValue("Expected operation result differs from actual", expectedResult, actualResult);
                }

                assertFalse(String.format("Expected error '%s' but none thrown for operation %s",
                        getErrorContainsField(expectedResult), operationName), hasErrorContainsField(expectedResult));
                assertFalse(String.format("Expected error code '%s' but none thrown for operation %s",
                        getErrorCodeNameField(expectedResult), operationName), hasErrorCodeNameField(expectedResult));
            } catch (Exception e) {
                boolean passedAssertion = false;
               if (hasTimeoutError(expectedResult) && e instanceof MongoOperationTimeoutException){
                   passedAssertion = true;
               }
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
            List<CommandEvent> expectedEvents = getExpectedEvents(definition.getArray("expectations"), specDocument.getString("database_name").getValue(), null);
            List<CommandStartedEvent> events = commandListener.getCommandStartedEvents();
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

    /**
     * If the operation returns a raw command response, eg from runCommand, then compare only the fields present in the expected result
     * document.
     * <p>
     * Otherwise, compare the method's return value to result using the same logic as the CRUD Spec Tests runner.
     */
    private void assertBsonValue(final String message, final BsonValue expectedResult, final BsonValue actualResult) {
        if (expectedResult.isDocument() && actualResult.isDocument()) {
            BsonDocument expectedResultDoc = expectedResult.asDocument();
            BsonDocument actualResultDoc = actualResult.asDocument();
            expectedResultDoc.keySet().forEach(k ->
                    assertEquals(message, expectedResultDoc.get(k), actualResultDoc.get(k, new BsonUndefined()))
            );
        } else if (expectedResult.isArray() && actualResult.isArray()) {
            BsonArray expectedResultArray = expectedResult.asArray();
            BsonArray actualResultArray = actualResult.asArray();
            assertEquals(expectedResultArray.size(), actualResultArray.size());
            for (int i = 0; i < expectedResultArray.size(); i++) {
                assertBsonValue(message + " Index: " + i, expectedResultArray.get(i), actualResultArray.get(i));
            }
        } else {
            assertEquals(message, expectedResult, actualResult);
        }
    }

    @Parameterized.Parameters(name = "{0}: {1}")
    public static Collection<Object[]> data() {
        List<Object[]> data = new ArrayList<>();
        for (BsonDocument specDocument : JsonPoweredTestHelper.getTestDocuments("/client-side-encryption/legacy")) {
            for (BsonValue test : specDocument.getArray("tests")) {
                BsonDocument testDocument = test.asDocument();
                data.add(new Object[]{specDocument.getString("fileName").getValue(),
                        testDocument.getString("description").getValue(), specDocument,
                        specDocument.getArray("data", new BsonArray()), testDocument,
                        skipTest(specDocument, testDocument)});
            }
        }
        return data;
    }

    static Optional<String> cryptSharedLibPathSysPropValue() {
        String value = getEnv("CRYPT_SHARED_LIB_PATH", "");
        return value.isEmpty() ? Optional.empty() : Optional.of(value);
    }
}
