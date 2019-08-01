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

package com.mongodb.async.client;

import com.mongodb.Block;
import com.mongodb.ClientSessionOptions;
import com.mongodb.ConnectionString;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadConcernLevel;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.async.FutureResultCallback;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.test.CollectionHelper;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ServerSettings;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
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

import static com.mongodb.ClusterFixture.getMultiMongosConnectionString;
import static com.mongodb.JsonTestServerVersionChecker.skipTest;
import static com.mongodb.async.client.Fixture.getConnectionString;
import static com.mongodb.async.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.async.client.Fixture.isSharded;
import static com.mongodb.client.CommandMonitoringTestHelper.assertEventsEquality;
import static com.mongodb.client.CommandMonitoringTestHelper.getExpectedEvents;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

// See https://github.com/mongodb/specifications/tree/master/source/transactions/tests
@RunWith(Parameterized.class)
public class TransactionsTest {

    private final String filename;
    private final String description;
    private final String databaseName;
    private final BsonArray data;
    private final BsonDocument definition;
    private final boolean skipTest;
    private JsonPoweredCrudTestHelper helper;
    private final TestCommandListener commandListener;
    private MongoClient mongoClient;
    private CollectionHelper<Document> collectionHelper;
    private Map<String, ClientSession> sessionsMap;
    private HashMap<String, BsonDocument> lsidMap;
    private boolean useMultipleMongoses = false;
    private ConnectionString connectionString;
    private final String collectionName = "test";

    private static final long MIN_HEARTBEAT_FREQUENCY_MS = 50L;

    public TransactionsTest(final String filename, final String description, final BsonArray data, final BsonDocument definition,
                            final boolean skipTest) {
        this.filename = filename;
        this.description = description;
        this.databaseName = getDefaultDatabaseName();
        this.data = data;
        this.definition = definition;
        this.commandListener = new TestCommandListener();
        this.skipTest = skipTest;
    }

    @Before
    public void setUp() {
        assumeFalse(skipTest);
        assumeTrue("Skipping test: " + definition.getString("skipReason", new BsonString("")).getValue(),
                !definition.containsKey("skipReason"));
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


        if (definition.containsKey("failPoint")) {
            collectionHelper.runAdminCommand(definition.getDocument("failPoint"));
        }

        final BsonDocument clientOptions = definition.getDocument("clientOptions", new BsonDocument());

        connectionString = getConnectionString();

        useMultipleMongoses = definition.getBoolean("useMultipleMongoses", BsonBoolean.FALSE).getValue();
        if (useMultipleMongoses) {
            assumeTrue(isSharded());
            connectionString = getMultiMongosConnectionString();
            assumeTrue("The system property org.mongodb.test.transaction.uri is not set.", connectionString != null);
        }

        MongoClientSettings.Builder builder = MongoClientSettings.builder().applyConnectionString(connectionString);

        if (System.getProperty("java.version").startsWith("1.6.")) {
            builder.applyToSslSettings(new Block<SslSettings.Builder>() {
                @Override
                public void apply(final SslSettings.Builder builder) {
                    builder.invalidHostNameAllowed(true);
                }
            });
        }
        builder.addCommandListener(commandListener)
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
                .retryWrites(clientOptions.getBoolean("retryWrites", BsonBoolean.FALSE).getValue())
                .retryReads(false)
                .applyToServerSettings(new Block<ServerSettings.Builder>() {
                    @Override
                    public void apply(final ServerSettings.Builder builder) {
                        builder.minHeartbeatFrequency(MIN_HEARTBEAT_FREQUENCY_MS, TimeUnit.MILLISECONDS);
                    }
                });

        if (clientOptions.containsKey("heartbeatFrequencyMS")) {
            builder.applyToServerSettings(new Block<ServerSettings.Builder>() {
                @Override
                public void apply(final ServerSettings.Builder builder) {
                    builder.heartbeatFrequency(clientOptions.getInt32("heartbeatFrequencyMS").intValue(), TimeUnit.MILLISECONDS);
                }
            });
        }

        mongoClient = MongoClients.create(builder.build());

        MongoDatabase database = mongoClient.getDatabase(databaseName);

        if (useMultipleMongoses) {
            // non-transactional distinct operation to avoid StaleDbVersion error
            runDistinctOnEachNode();
        }

        helper = new JsonPoweredCrudTestHelper(description, database, database.getCollection(collectionName, BsonDocument.class));

        ClientSession sessionZero = createSession("session0");
        ClientSession sessionOne = createSession("session1");

        sessionsMap = new HashMap<String, ClientSession>();
        sessionsMap.put("session0", sessionZero);
        sessionsMap.put("session1", sessionOne);
        lsidMap = new HashMap<String, BsonDocument>();
        lsidMap.put("session0", sessionZero.getServerSession().getIdentifier());
        lsidMap.put("session1", sessionOne.getServerSession().getIdentifier());
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
        final ClientSessionOptions options = ClientSessionOptions.builder()
                .causallyConsistent(optionsDocument.getBoolean("causalConsistency", BsonBoolean.TRUE).getValue())
                .defaultTransactionOptions(createDefaultTransactionOptions(optionsDocument))
                .build();
        return new MongoOperation<ClientSession>() {
            @Override
            public void execute() {
                mongoClient.startSession(options, getCallback());
            }
        }.get();
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
            if (defaultTransactionOptionsDocument.containsKey("maxCommitTimeMS")) {
                builder.maxCommitTime(defaultTransactionOptionsDocument.getNumber("maxCommitTimeMS").longValue(), MILLISECONDS);
            }

        }
        return builder.build();
    }

    private void runDistinctOnEachNode() {
        List<String> hosts = connectionString.getHosts();
        for (String host : hosts) {
            runDistinctOnHost(host);
        }
    }

    private void runDistinctOnHost(final String host) {
        MongoClient client = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .clusterSettings(ClusterSettings.builder()
                        .hosts(singletonList(new ServerAddress(host))).build()).build());
        DistinctIterable<BsonValue> iterable = client.getDatabase(databaseName).getCollection(collectionName)
                .distinct("_id", BsonValue.class);
        FutureResultCallback<List<BsonValue>> futureResultCallback = new FutureResultCallback<List<BsonValue>>();
        iterable.into(new BsonArray(), futureResultCallback);

        try {
            futureResultCallback.get();
        } catch (RuntimeException e) {
            throw e;
        } finally {
            client.close();
        }
    }

    @After
    public void cleanUp() {
        if (mongoClient != null) {
            mongoClient.close();
        }

        if (collectionHelper != null && definition.containsKey("failPoint")) {
            collectionHelper.runAdminCommand(new BsonDocument("configureFailPoint",
                    definition.getDocument("failPoint").getString("configureFailPoint"))
                    .append("mode", new BsonString("off")));
        }
    }

    private void closeAllSessions() {
        for (final ClientSession cur : sessionsMap.values()) {
            cur.close();
        }
    }

    @Test
    public void shouldPassAllOutcomes() {
        try {
            executeOperations(definition.getArray("operations"), false);
        } finally {
            closeAllSessions();
        }

        if (definition.containsKey("expectations")) {
            // TODO: null operation may cause test failures, since it's used to grab the read preference
            // TODO: though read-pref.json doesn't declare expectations, so maybe not
            List<CommandEvent> expectedEvents = getExpectedEvents(definition.getArray("expectations"), databaseName, null);
            List<CommandEvent> events = commandListener.getCommandStartedEvents();

            assertEventsEquality(expectedEvents, events, lsidMap);
        }

        BsonDocument expectedOutcome = definition.getDocument("outcome", new BsonDocument());
        if (expectedOutcome.containsKey("collection")) {
            List<BsonDocument> collectionData = collectionHelper.find(new BsonDocumentCodec());
            assertEquals(expectedOutcome.getDocument("collection").getArray("data").getValues(), collectionData);
        }
    }

    private void executeOperations(final BsonArray operations, final boolean throwExceptions) {
        TargetedFailPoint failPoint = null;
        try {
            for (BsonValue cur : operations) {
                final BsonDocument operation = cur.asDocument();
                String operationName = operation.getString("name").getValue();
                BsonValue expectedResult = operation.get("result");
                String receiver = operation.getString("object").getValue();
                final ClientSession clientSession = receiver.startsWith("session") ? sessionsMap.get(receiver)
                        : (operation.getDocument("arguments").containsKey("session")
                        ? sessionsMap.get(operation.getDocument("arguments").getString("session").getValue()) : null);
                try {
                    if (operationName.equals("startTransaction")) {
                        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
                        if (arguments.containsKey("options")) {
                            TransactionOptions transactionOptions = createTransactionOptions(arguments.getDocument("options"));
                            nonNullClientSession(clientSession).startTransaction(transactionOptions);
                        } else {
                            nonNullClientSession(clientSession).startTransaction();
                        }
                    } else if (operationName.equals("commitTransaction")) {
                        new MongoOperation<Void>() {
                            @Override
                            public void execute() {
                                nonNullClientSession(clientSession).commitTransaction(getCallback());
                            }
                        }.get();
                    } else if (operationName.equals("abortTransaction")) {
                        new MongoOperation<Void>() {
                            @Override
                            public void execute() {
                                nonNullClientSession(clientSession).abortTransaction(getCallback());
                            }
                        }.get();
                    } else if (operationName.equals("targetedFailPoint")) {
                        assertTrue(failPoint == null);
                        failPoint = new TargetedFailPoint(operation);
                        failPoint.executeFailPoint();
                    } else if (operationName.equals("assertSessionPinned")) {
                        if (isSharded()) {
                            final BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
                            assertNotNull(sessionsMap.get(arguments.getString("session").getValue()).getPinnedServerAddress());
                        }
                    } else if (operationName.equals("assertSessionUnpinned")) {
                        if (isSharded()) {
                            final BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
                            assertNull(sessionsMap.get(arguments.getString("session").getValue()).getPinnedServerAddress());
                        }
                    } else {
                        BsonDocument actualOutcome = helper.getOperationResults(operation, clientSession);
                        if (expectedResult != null) {
                            BsonValue actualResult = actualOutcome.get("result");
                            if (actualResult.isDocument()) {
                                if (((BsonDocument) actualResult).containsKey("recoveryToken")) {
                                    ((BsonDocument) actualResult).remove("recoveryToken");
                                }
                            }

                            assertEquals("Expected operation result differs from actual", expectedResult, actualResult);
                        }
                    }
                    assertFalse(String.format("Expected error '%s' but none thrown for operation %s",
                            getErrorContainsField(expectedResult), operationName), hasErrorContainsField(expectedResult));
                    assertFalse(String.format("Expected error code '%s' but none thrown for operation %s",
                            getErrorCodeNameField(expectedResult), operationName), hasErrorCodeNameField(expectedResult));
                } catch (RuntimeException e) {
                    boolean passedAssertion = false;
                    if (hasErrorLabelsContainField(expectedResult)) {
                        if (e instanceof MongoException) {
                            MongoException mongoException = (MongoException) e;
                            for (String curErrorLabel : getErrorLabelsContainField(expectedResult)) {
                                assertTrue(String.format("Expected error label '%s but found labels '%s' for operation %s",
                                        curErrorLabel, mongoException.getErrorLabels(), operationName),
                                        mongoException.hasErrorLabel(curErrorLabel));
                            }
                            passedAssertion = true;
                        }
                    }
                    if (hasErrorLabelsOmitField(expectedResult)) {
                        if (e instanceof MongoException) {
                            MongoException mongoException = (MongoException) e;
                            for (String curErrorLabel : getErrorLabelsOmitField(expectedResult)) {
                                assertFalse(String.format("Expected error label '%s omitted but found labels '%s' for operation %s",
                                        curErrorLabel, mongoException.getErrorLabels(), operationName),
                                        mongoException.hasErrorLabel(curErrorLabel));
                            }
                            passedAssertion = true;
                        }
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
                    if (!passedAssertion || throwExceptions) {
                        throw e;
                    }
                }
            }
        } finally {
            if (failPoint != null) {
                failPoint.disableFailPoint();
            }
        }
    }

    private TransactionOptions createTransactionOptions(final BsonDocument options) {
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
        if (options.containsKey("maxCommitTimeMS")) {
            builder.maxCommitTime(options.getNumber("maxCommitTimeMS").longValue(), MILLISECONDS);
        }
        return builder.build();
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

    private boolean hasErrorLabelsContainField(final BsonValue expectedResult) {
        return hasErrorField(expectedResult, "errorLabelsContain");
    }

    private List<String> getErrorLabelsContainField(final BsonValue expectedResult) {
        return getListOfStringsFromBsonArrays(expectedResult.asDocument(), "errorLabelsContain");
    }

    private boolean hasErrorLabelsOmitField(final BsonValue expectedResult) {
        return hasErrorField(expectedResult, "errorLabelsOmit");
    }

    private List<String> getErrorLabelsOmitField(final BsonValue expectedResult) {
        return getListOfStringsFromBsonArrays(expectedResult.asDocument(), "errorLabelsOmit");
    }


    private List<String> getListOfStringsFromBsonArrays(final BsonDocument expectedResult, final String arrayFieldName) {
        List<String> errorLabelContainsList = new ArrayList<String>();
        for (BsonValue cur : expectedResult.asDocument().getArray(arrayFieldName)) {
            errorLabelContainsList.add(cur.asString().getValue());
        }
        return errorLabelContainsList;
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
                        testDocument.getArray("data"), test.asDocument(), skipTest(testDocument, test.asDocument())});
            }
        }
        return data;
    }

    private class TargetedFailPoint {
        private final BsonDocument failPointDocument;
        private final MongoDatabase adminDB;
        private MongoClient mongoClient;

        TargetedFailPoint(final BsonDocument operation) {
            final BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
            final ClientSession clientSession = sessionsMap.get(arguments.getString("session").getValue());

            if (clientSession.getPinnedServerAddress() != null) {
                mongoClient = MongoClients.create(MongoClientSettings.builder()
                        .applyConnectionString(connectionString)
                        .clusterSettings(ClusterSettings.builder()
                        .hosts(singletonList(clientSession.getPinnedServerAddress())).build()).build());

                adminDB = mongoClient.getDatabase("admin");
            } else {
                adminDB = null;
            }
            failPointDocument = arguments.getDocument("failPoint");
        }

        public void executeFailPoint() {
            executeCommand(failPointDocument);
        }

        public void disableFailPoint() {
            executeCommand(new BsonDocument("configureFailPoint",
                    failPointDocument.getString("configureFailPoint"))
                    .append("mode", new BsonString("off")));
            if (mongoClient != null) {
                mongoClient.close();
            }
        }

        private void executeCommand(final BsonDocument doc) {
            if (adminDB != null) {
                FutureResultCallback<BsonDocument> futureResultCallback = new FutureResultCallback<BsonDocument>();
                adminDB.runCommand(doc, BsonDocument.class, futureResultCallback);
                futureResultCallback.get();
            } else {
                collectionHelper.runAdminCommand(doc);
            }
        }
    }
}
