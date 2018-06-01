/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client;

import com.mongodb.Block;
import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadConcernLevel;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.test.CollectionHelper;
import com.mongodb.connection.SocketSettings;
import com.mongodb.event.CommandEvent;
import com.mongodb.internal.connection.TestCommandListener;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DocumentCodec;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
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

import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.CommandMonitoringTestHelper.assertEventsEquality;
import static com.mongodb.client.CommandMonitoringTestHelper.getExpectedEvents;
import static com.mongodb.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

// See https://github.com/mongodb/specifications/tree/master/source/transactions/tests
@RunWith(Parameterized.class)
public class TransactionsTest {

    private final String filename;
    private final String description;
    private final String databaseName;
    private final BsonArray data;
    private final BsonDocument definition;
    private JsonPoweredCrudTestHelper helper;
    private final TestCommandListener commandListener;
    private MongoClient mongoClient;
    private CollectionHelper<Document> collectionHelper;
    private Map<String, ClientSession> sessionsMap;

    @BeforeClass
    public static void beforeClass() {
    }

    @AfterClass
    public static void afterClass() {
    }

    public TransactionsTest(final String filename, final String description, final BsonArray data, final BsonDocument definition) {
        this.filename = filename;
        this.description = description;
        this.databaseName = getDefaultDatabaseName();
        this.data = data;
        this.definition = definition;
        this.commandListener = new TestCommandListener();
    }

    @Before
    public void setUp() {
        assumeTrue(canRunTests());
        assumeTrue("Skipping test: " + definition.getString("skipReason", new BsonString("")).getValue(),
                !definition.containsKey("skipReason"));

        String collectionName = "test";
        collectionHelper = new CollectionHelper<Document>(new DocumentCodec(), new MongoNamespace(databaseName, collectionName));

        collectionHelper.killAllSessions();
        collectionHelper.create(collectionName, new CreateCollectionOptions(), WriteConcern.MAJORITY);

        if (!data.isEmpty()) {
            List<BsonDocument> documents = new ArrayList<BsonDocument>();
            for (BsonValue document : data) {
                documents.add(document.asDocument());
            }

            collectionHelper.insertDocuments(documents, WriteConcern.MAJORITY);
        }

        BsonDocument clientOptions = definition.getDocument("clientOptions", new BsonDocument());

        mongoClient = MongoClients.create(getMongoClientSettingsBuilder()
                .addCommandListener(commandListener)
                .applyToSocketSettings(new Block<SocketSettings.Builder>() {
                    @Override
                    public void apply(final SocketSettings.Builder builder) {
                        builder.readTimeout(5, TimeUnit.SECONDS);
                    }
                })
                .retryWrites(clientOptions.getBoolean("retryWrites", BsonBoolean.FALSE).getValue())
                .writeConcern(getWriteConcern(clientOptions))
                .readConcern(getReadConcern(clientOptions))
                .readPreference(getReadPreference(clientOptions))
                .build());

        MongoDatabase database = mongoClient.getDatabase(databaseName);
        helper = new JsonPoweredCrudTestHelper(description, database, database.getCollection(collectionName, BsonDocument.class));

        ClientSession sessionZero = createSession("session0");
        ClientSession sessionOne = createSession("session1");

        sessionsMap = new HashMap<String, ClientSession>();
        sessionsMap.put("session0", sessionZero);
        sessionsMap.put("session1", sessionOne);
    }

    private ReadConcern getReadConcern(final BsonDocument clientOptions) {
        if (clientOptions.containsKey("readConcernLevel")) {
             return new ReadConcern(ReadConcernLevel.fromString(clientOptions.getString("readConcernLevel").getValue()));
        } else {
            return ReadConcern.DEFAULT;
        }
    }

    private WriteConcern getWriteConcern(final BsonDocument clientOptions) {
        if (clientOptions.containsKey("w")) {
            if (clientOptions.isNumber("w")) {
                return new WriteConcern(clientOptions.getNumber("w").intValue());
            } else {
                return new WriteConcern(clientOptions.getString("w").getValue());
            }
        } else {
            return WriteConcern.ACKNOWLEDGED;
        }
    }

    private ReadPreference getReadPreference(final BsonDocument clientOptions) {
        if (clientOptions.containsKey("readPreference")) {
            return ReadPreference.valueOf(clientOptions.getString("readPreference").getValue());
        } else {
            return ReadPreference.primary();
        }
    }

    private ClientSession createSession(final String sessionName) {
        BsonDocument optionsDocument = definition.getDocument("sessionOptions", new BsonDocument())
                .getDocument(sessionName, new BsonDocument());
        ClientSessionOptions options = ClientSessionOptions.builder()
                .causallyConsistent(optionsDocument.getBoolean("causalConsistency", BsonBoolean.TRUE).getValue())
                .defaultTransactionOptions(createDefaultTransactionOptions(optionsDocument))
                .build();
        return mongoClient.startSession(options);
    }

    private TransactionOptions createDefaultTransactionOptions(final BsonDocument optionsDocument) {
        TransactionOptions.Builder builder = TransactionOptions.builder();
        if (optionsDocument.containsKey("defaultTransactionOptions")) {
            BsonDocument defaultTransactionOptionsDocument = optionsDocument.getDocument("defaultTransactionOptions");
            if (defaultTransactionOptionsDocument.containsKey("readConcern")) {
                builder.readConcern(helper.getReadConcern(defaultTransactionOptionsDocument));
            }
            if (defaultTransactionOptionsDocument.containsKey("writeConcern")) {
                builder.writeConcern(helper.getWriteConcern(defaultTransactionOptionsDocument));
            }
            if (defaultTransactionOptionsDocument.containsKey("readPreference")) {
                builder.readPreference(helper.getReadPreference(defaultTransactionOptionsDocument));
            }
        }
        return builder.build();
    }

    @After
    public void cleanUp() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    private void closeAllSessions() {
        for (ClientSession cur : sessionsMap.values()) {
            cur.close();
        }
    }

    @Test
    public void shouldPassAllOutcomes() {
        try {
            for (BsonValue cur : definition.getArray("operations")) {
                BsonDocument operation = cur.asDocument();
                String operationName = operation.getString("name").getValue();
                BsonValue expectedResult = operation.get("result", new BsonDocument());
                String receiver = operation.getString("object").getValue();
                ClientSession clientSession = receiver.startsWith("session") ? sessionsMap.get(receiver) : null;
                if (clientSession == null) {
                    clientSession = operation.getDocument("arguments").containsKey("session")
                            ? sessionsMap.get(operation.getDocument("arguments").getString("session").getValue()) : null;
                }
                BsonDocument sessionIdentifier = (clientSession == null) ? null : clientSession.getServerSession().getIdentifier();
                commandListener.addExpectedSessionNextStartedEvent(sessionIdentifier);
                try {
                    if (operationName.equals("startTransaction")) {
                        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
                        if (arguments.containsKey("options")) {
                            BsonDocument options = arguments.getDocument("options");
                            TransactionOptions.Builder builder = TransactionOptions.builder();
                            if (options.containsKey("writeConcern")) {
                                builder.writeConcern(helper.getWriteConcern(options));
                            }
                            if (options.containsKey("readConcern")) {
                                builder.readConcern(helper.getReadConcern(options));
                            }
                            if (options.containsKey("readPreference")) {
                                builder.readPreference(helper.getReadPreference(options));
                            }
                            nonNullClientSession(clientSession).startTransaction(builder.build());
                        } else {
                            nonNullClientSession(clientSession).startTransaction();
                        }
                    } else if (operationName.equals("commitTransaction")) {
                        nonNullClientSession(clientSession).commitTransaction();
                    } else if (operationName.equals("abortTransaction")) {
                        nonNullClientSession(clientSession).abortTransaction();
                    } else {
                        BsonDocument actualOutcome = helper.getOperationResults(operation, clientSession);
                        BsonValue actualResult = actualOutcome.get("result");

                        assertEquals("Expected operation result differs from actual", expectedResult, actualResult);
                    }
                    assertFalse(String.format("Expected error '%s' but none thrown", getErrorContainsField(expectedResult)),
                            hasErrorContainsField(expectedResult));
                    assertFalse(String.format("Expected error code '%s' but none thrown", getErrorCodeNameField(expectedResult)),
                            hasErrorCodeNameField(expectedResult));
                } catch (RuntimeException e) {
                    if (hasErrorContainsField(expectedResult)) {
                        String expectedError = getErrorContainsField(expectedResult);
                        assertTrue(String.format("Expected '%s' but got '%s'", expectedError, e.getMessage()),
                                e.getMessage().toLowerCase().contains(expectedError.toLowerCase()));
                    } else if (hasErrorCodeNameField(expectedResult) || (e instanceof MongoCommandException)
                            || (e instanceof MongoWriteConcernException)) {
                        String expectedErrorCodeName = getErrorCodeNameField(expectedResult);
                        if (e instanceof MongoCommandException) {
                            assertEquals(expectedErrorCodeName, ((MongoCommandException) e).getErrorCodeName());
                        } else if (e instanceof MongoWriteConcernException) {
                            assertEquals(expectedErrorCodeName, ((MongoWriteConcernException) e).getWriteConcernError().getCodeName());
                        } else {
                            throw e;
                        }
                    } else {
                        throw e;
                    }
                }
            }
        } finally {
            closeAllSessions();
        }

        if (definition.containsKey("expectations")) {
            // TODO: null operation may cause test failures, since it's used to grab the read preference
            // TODO: though read-pref.json doesn't declare expectations, so maybe not
            List<CommandEvent> expectedEvents = getExpectedEvents(definition.getArray("expectations"), databaseName, null);
            List<CommandEvent> events = commandListener.getCommandStartedEvents();

            assertEventsEquality(expectedEvents, events, commandListener.getSessions());
        }

        BsonDocument expectedOutcome = definition.getDocument("outcome", new BsonDocument());
        if (expectedOutcome.containsKey("collection")) {
            List<BsonDocument> collectionData = collectionHelper.find(new BsonDocumentCodec());
            assertEquals(expectedOutcome.getDocument("collection").getArray("data").getValues(), collectionData);
        }
    }

    private String getErrorContainsField(final BsonValue expectedResult) {
        return getErrorField(expectedResult, "errorContains");
    }

    private String getErrorCodeNameField(final BsonValue expectedResult) {
        return getErrorField(expectedResult, "errorCodeName");
    }

    private String getErrorField(final BsonValue expectedResult, final String key) {
        if (hasErrorField(expectedResult, key)) {
            return expectedResult.asDocument().getString(key).getValue();
        } else {
            return "";
        }
    }

    private boolean hasErrorContainsField(final BsonValue expectedResult) {
        return hasErrorField(expectedResult, "errorContains");
    }

    private boolean hasErrorCodeNameField(final BsonValue expectedResult) {
        return hasErrorField(expectedResult, "errorCodeName");
    }

    private boolean hasErrorField(final BsonValue expectedResult, final String key) {
        return expectedResult.isDocument() && expectedResult.asDocument().containsKey(key);
    }

    private ClientSession nonNullClientSession(@Nullable final ClientSession clientSession) {
        if (clientSession == null) {
            throw new IllegalArgumentException("clientSession can't be null in this context");
        }
        return clientSession;
    }

    @Parameterized.Parameters(name = "{0}: {1}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        List<Object[]> data = new ArrayList<Object[]>();
        for (File file : JsonPoweredTestHelper.getTestFiles("/transactions")) {
            BsonDocument testDocument = JsonPoweredTestHelper.getTestDocument(file);
            for (BsonValue test : testDocument.getArray("tests")) {
                data.add(new Object[]{file.getName(), test.asDocument().getString("description").getValue(),
                        testDocument.getArray("data"), test.asDocument()});
            }
        }
        return data;
    }

    private boolean canRunTests() {
        return serverVersionAtLeast(3, 7) && isDiscoverableReplicaSet();
    }
}
