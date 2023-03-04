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

import com.mongodb.ClientSessionOptions;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
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
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.test.CollectionHelper;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;
import com.mongodb.connection.StreamFactoryFactory;
import com.mongodb.event.CommandEvent;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.ConnectionPoolClearedEvent;
import com.mongodb.event.ConnectionPoolReadyEvent;
import com.mongodb.internal.connection.TestCommandListener;
import com.mongodb.internal.connection.TestConnectionPoolListener;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static com.mongodb.ClusterFixture.getConnectionString;
import static com.mongodb.ClusterFixture.getMultiMongosConnectionString;
import static com.mongodb.ClusterFixture.isDataLakeTest;
import static com.mongodb.ClusterFixture.isSharded;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.ClusterFixture.serverVersionLessThan;
import static com.mongodb.ClusterFixture.setDirectConnection;
import static com.mongodb.client.CommandMonitoringTestHelper.assertEventsEquality;
import static com.mongodb.client.CommandMonitoringTestHelper.getExpectedEvents;
import static com.mongodb.client.Fixture.getMongoClient;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static java.lang.Math.toIntExact;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
public abstract class AbstractUnifiedTest {
    private final String filename;
    private final String description;
    private final String databaseName;
    private final BsonArray data;
    private final BsonDocument definition;
    private final boolean skipTest;
    private final boolean createSessions;
    private JsonPoweredCrudTestHelper helper;
    private final TestCommandListener commandListener;
    private final TestConnectionPoolListener connectionPoolListener;
    private final TestServerListener serverListener;
    private MongoClient mongoClient;
    private CollectionHelper<Document> collectionHelper;
    private Map<String, ClientSession> sessionsMap;
    private Map<String, BsonDocument> lsidMap;
    private boolean useMultipleMongoses = false;
    private ConnectionString connectionString = null;
    private final String collectionName;
    private MongoDatabase database;
    private final Map<String, ExecutorService> executorServiceMap = new HashMap<>();
    private final Map<String, Future<Exception>> futureMap = new HashMap<>();

    private static final long MIN_HEARTBEAT_FREQUENCY_MS = 50L;

    /**
     * @param createSessions {@code true} means that {@code session0}, {@code session1} {@link ClientSession}s must be created as specified
     *                       <a href="https://github.com/mongodb/specifications/blob/master/source/transactions/tests/README.rst#use-as-integration-tests">here</a>,
     *                       otherwise these sessions are not created.
     *                       <p>
     *                       This parameter was introduced to work around a race condition in the test
     *                       {@code minPoolSize-error.json: "Network error on minPoolSize background creation"},
     *                       which occurs as a result of the test runner creating sessions concurrently with activities of
     *                       the connection pool background thread.
     */
    public AbstractUnifiedTest(final String filename, final String description, final String databaseName, final String collectionName,
                               final BsonArray data, final BsonDocument definition, final boolean skipTest, final boolean createSessions) {
        this.filename = filename;
        this.description = description;
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.data = data;
        this.definition = definition;
        this.commandListener = new TestCommandListener();
        this.connectionPoolListener = new TestConnectionPoolListener();
        this.serverListener = new TestServerListener();
        this.skipTest = skipTest;
        this.createSessions = createSessions;
    }

    protected abstract MongoClient createMongoClient(MongoClientSettings settings);

    @Nullable
    protected StreamFactoryFactory getStreamFactoryFactory() {
        return null;
    }

    protected final String getDescription() {
        return description;
    }

    protected final BsonDocument getDefinition() {
        return definition;
    }

    @Before
    public void setUp() {
        assumeFalse(skipTest);
        assumeTrue("Skipping test: " + definition.getString("skipReason", new BsonString("")).getValue(),
                !definition.containsKey("skipReason"));
        assumeFalse("Skipping test of count", filename.equals("count.json"));

        collectionHelper = new CollectionHelper<>(new DocumentCodec(), new MongoNamespace(databaseName, collectionName));

        collectionHelper.killAllSessions();

        if (!isDataLakeTest()) {
            try {
                collectionHelper.create(collectionName, new CreateCollectionOptions(), WriteConcern.MAJORITY);
            } catch (MongoCommandException e) {
                // Older sharded clusters sometimes reply with this error.  Work around it by retrying once.
                if (e.getErrorCode() == 11601 && isSharded() && serverVersionLessThan(4, 2)) {
                    collectionHelper.create(collectionName, new CreateCollectionOptions(), WriteConcern.MAJORITY);
                } else {
                    throw e;
                }
            }
        }

        if (!data.isEmpty()) {
            List<BsonDocument> documents = new ArrayList<>();
            for (BsonValue document : data) {
                documents.add(document.asDocument());
            }

            collectionHelper.insertDocuments(documents, WriteConcern.MAJORITY);
        }

        if (definition.containsKey("failPoint")) {
            collectionHelper.runAdminCommand(definition.getDocument("failPoint"));
        }

        BsonDocument clientOptions = definition.getDocument("clientOptions", new BsonDocument());

        connectionString = getConnectionString();
        useMultipleMongoses = definition.getBoolean("useMultipleMongoses", BsonBoolean.FALSE).getValue();
        if (useMultipleMongoses) {
            assumeTrue(isSharded());
            connectionString = getMultiMongosConnectionString();
            assumeTrue("The system property org.mongodb.test.transaction.uri is not set.", connectionString != null);
        }

        MongoClientSettings.Builder builder = getMongoClientSettingsBuilder()
                .applyConnectionString(connectionString)
                .addCommandListener(commandListener)
                .applyToClusterSettings(clusterSettingsBuilder -> {
                    if (clientOptions.containsKey("serverSelectionTimeoutMS")) {
                        clusterSettingsBuilder.serverSelectionTimeout(
                                clientOptions.getNumber("serverSelectionTimeoutMS").longValue(), MILLISECONDS);
                    }
                    if (clientOptions.containsKey("directConnection")) {
                        setDirectConnection(clusterSettingsBuilder);
                    }
                })
                .applyToSocketSettings(builder13 -> {
                    builder13.readTimeout(clientOptions.getInt32(
                            "socketTimeoutMS", new BsonInt32(toIntExact(SECONDS.toMillis(5)))).getValue(), MILLISECONDS);
                    if (clientOptions.containsKey("connectTimeoutMS")) {
                        builder13.connectTimeout(clientOptions.getNumber("connectTimeoutMS").intValue(), MILLISECONDS);
                    }
                })
                .writeConcern(getWriteConcern(clientOptions))
                .readConcern(getReadConcern(clientOptions))
                .readPreference(getReadPreference(clientOptions))
                .retryWrites(clientOptions.getBoolean("retryWrites", BsonBoolean.FALSE).getValue())
                .retryReads(false)
                .applyToConnectionPoolSettings(poolSettingsBuilder -> {
                    poolSettingsBuilder.addConnectionPoolListener(connectionPoolListener);
                    if (clientOptions.containsKey("minPoolSize")) {
                        poolSettingsBuilder.minSize(clientOptions.getInt32("minPoolSize").getValue());
                    }
                })
                .applyToServerSettings(builder12 -> {
                    builder12.heartbeatFrequency(50, MILLISECONDS);
                    builder12.minHeartbeatFrequency(MIN_HEARTBEAT_FREQUENCY_MS, MILLISECONDS);
                    builder12.addServerListener(serverListener);
                });
        if (clientOptions.containsKey("heartbeatFrequencyMS")) {
            builder.applyToServerSettings(builder1 -> builder1.heartbeatFrequency(clientOptions.getInt32("heartbeatFrequencyMS").intValue(), MILLISECONDS));
        }
        if (clientOptions.containsKey("appname")) {
            builder.applicationName(clientOptions.getString("appname").getValue());
        }
        if (clientOptions.containsKey("w")) {
            if (clientOptions.isString("w")) {
                builder.writeConcern(new WriteConcern(clientOptions.getString("w").getValue()));
            } else if (clientOptions.isNumber("w")) {
                builder.writeConcern(new WriteConcern(clientOptions.getNumber("w").intValue()));
            }
        }
        StreamFactoryFactory streamFactoryFactory = getStreamFactoryFactory();
        if (streamFactoryFactory != null) {
            builder.streamFactoryFactory(streamFactoryFactory);
        }
        mongoClient = createMongoClient(builder.build());

        database = mongoClient.getDatabase(databaseName);

        if (useMultipleMongoses) {
            // non-transactional distinct operation to avoid StaleDbVersion error
            runDistinctOnEachNode();
        }

        helper = new JsonPoweredCrudTestHelper(description, database, database.getCollection(collectionName, BsonDocument.class),
                null, mongoClient);

        sessionsMap = new HashMap<>();
        lsidMap = new HashMap<>();
        if (createSessions && serverVersionAtLeast(3, 6)) {
            ClientSession sessionZero = createSession("session0");
            ClientSession sessionOne = createSession("session1");

            sessionsMap.put("session0", sessionZero);
            sessionsMap.put("session1", sessionOne);
            lsidMap.put("session0", sessionZero.getServerSession().getIdentifier());
            lsidMap.put("session1", sessionOne.getServerSession().getIdentifier());
        }
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
                .applyToClusterSettings(builder -> builder.hosts(singletonList(new ServerAddress(host)))).build());
        client.getDatabase(databaseName).getCollection(collectionName).distinct("_id", BsonValue.class).into(new BsonArray());
        client.close();
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
        if (sessionsMap != null) {
            for (ClientSession cur : sessionsMap.values()) {
                cur.close();
            }
        }
    }

    private void shutdownAllExecutors() {
        for (ExecutorService cur : executorServiceMap.values()) {
            cur.shutdownNow();
        }
    }

    @Test
    public void shouldPassAllOutcomes() {
        try {
            executeOperations(definition.getArray("operations"), false);
        } finally {
            closeAllSessions();
            shutdownAllExecutors();
        }

        if (definition.containsKey("expectations")) {
            List<CommandEvent> expectedEvents = getExpectedEvents(definition.getArray("expectations"), databaseName, null);
            List<CommandEvent> events = commandListener.getCommandStartedEvents();

            assertTrue("Actual number of events is less than expected number of events", events.size() >= expectedEvents.size());
            assertEventsEquality(expectedEvents, events.subList(0, expectedEvents.size()), lsidMap);
        }

        BsonDocument expectedOutcome = definition.getDocument("outcome", new BsonDocument());
        if (expectedOutcome.containsKey("collection")) {
            BsonDocument collectionDocument = expectedOutcome.getDocument("collection");
            List<BsonDocument> collectionData;
            if (collectionDocument.containsKey("name")) {
                collectionData = new CollectionHelper<>(new DocumentCodec(),
                        new MongoNamespace(databaseName, collectionDocument.getString("name").getValue()))
                        .find(new BsonDocumentCodec());
            } else {
                collectionData = collectionHelper.find(new BsonDocumentCodec());
            }
            assertEquals(expectedOutcome.getDocument("collection").getArray("data").getValues(), collectionData);
        }
    }

    private void executeOperations(final BsonArray operations, final boolean throwExceptions) {
        FailPoint failPoint = null;
        ServerAddress currentPrimary = null;

        try {
            for (BsonValue cur : operations) {
                BsonDocument operation = cur.asDocument();
                String operationName = operation.getString("name").getValue();
                BsonValue expectedResult = operation.get("result");
                String receiver = operation.getString("object").getValue();

                ClientSession clientSession = receiver.startsWith("session") ? sessionsMap.get(receiver) : null;
                if (clientSession == null) {
                    clientSession = operation.getDocument("arguments", new BsonDocument()).containsKey("session")
                            ? sessionsMap.get(operation.getDocument("arguments").getString("session").getValue()) : null;
                }
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
                        nonNullClientSession(clientSession).commitTransaction();
                    } else if (operationName.equals("abortTransaction")) {
                        nonNullClientSession(clientSession).abortTransaction();
                    } else if (operationName.equals("withTransaction")) {
                        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());

                        TransactionOptions transactionOptions = null;
                        if (arguments.containsKey("options")) {
                            transactionOptions = createTransactionOptions(arguments.getDocument("options"));
                        }

                        if (transactionOptions == null) {
                            nonNullClientSession(clientSession).withTransaction(() -> {
                                executeOperations(arguments.getDocument("callback").getArray("operations"), true);
                                return null;
                            });
                        } else {
                            nonNullClientSession(clientSession).withTransaction(() -> {
                                executeOperations(arguments.getDocument("callback").getArray("operations"), true);
                                return null;
                            }, transactionOptions);
                        }
                    } else if (operationName.equals("targetedFailPoint")) {
                        assertNull(failPoint);
                        failPoint = new TargetedFailPoint(operation);
                        failPoint.executeFailPoint();
                    } else if (operationName.equals("configureFailPoint")) {
                        assertNull(failPoint);
                        failPoint = new FailPoint(operation);
                        failPoint.executeFailPoint();
                    } else if (operationName.equals("startThread")) {
                        String target = operation.getDocument("arguments").getString("name").getValue();
                        executorServiceMap.put(target, Executors.newSingleThreadExecutor());
                    } else if (operationName.equals("runOnThread")) {
                        String target = operation.getDocument("arguments").getString("name").getValue();
                        ExecutorService executorService = executorServiceMap.get(target);
                        Callable<Exception> callable = createCallable(operation.getDocument("arguments").getDocument("operation"));
                        futureMap.put(target, executorService.submit(callable));
                    } else if (operationName.equals("wait")) {
                        Thread.sleep(operation.getDocument("arguments").getNumber("ms").longValue());
                    } else if (operationName.equals("waitForThread")) {
                        String target = operation.getDocument("arguments").getString("name").getValue();
                        Exception exceptionFromFuture = futureMap.remove(target).get(5, SECONDS);
                        if (exceptionFromFuture != null) {
                            throw exceptionFromFuture;
                        }
                    } else if (operationName.equals("waitForEvent")) {
                        String event = operation.getDocument("arguments").getString("event").getValue();
                        int count = operation.getDocument("arguments").getNumber("count").intValue();
                        long timeoutMillis = SECONDS.toMillis(5);
                        switch (event) {
                            case "PoolClearedEvent":
                                connectionPoolListener.waitForEvent(ConnectionPoolClearedEvent.class, count, timeoutMillis, MILLISECONDS);
                                break;
                            case "PoolReadyEvent":
                                connectionPoolListener.waitForEvent(ConnectionPoolReadyEvent.class, count, timeoutMillis, MILLISECONDS);
                                break;
                            case "ServerMarkedUnknownEvent":
                                serverListener.waitForEvent(ServerType.UNKNOWN, count, timeoutMillis, MILLISECONDS);
                                break;
                            default:
                                throw new UnsupportedOperationException("Unsupported event type: " + event);
                        }
                    } else if (operationName.equals("assertEventCount")) {
                        String event = operation.getDocument("arguments").getString("event").getValue();
                        int expectedCount = operation.getDocument("arguments").getNumber("count").intValue();
                        int actualCount = -1;
                        switch (event) {
                            case "PoolClearedEvent":
                                actualCount = connectionPoolListener.countEvents(ConnectionPoolClearedEvent.class);
                                break;
                            case "PoolReadyEvent":
                                actualCount = connectionPoolListener.countEvents(ConnectionPoolReadyEvent.class);
                                break;
                            case "ServerMarkedUnknownEvent":
                                actualCount = serverListener.countEvents(ServerType.UNKNOWN);
                                break;
                            default:
                                throw new UnsupportedOperationException("Unsupported event type: " + event);
                        }
                        assertEquals(event + " counts not equal", expectedCount, actualCount);
                    } else if (operationName.equals("recordPrimary")) {
                        currentPrimary = getCurrentPrimary();
                    } else if (operationName.equals("waitForPrimaryChange")) {
                        long startTimeMillis = System.currentTimeMillis();
                        int timeoutMillis = operation.getDocument("arguments").getNumber("timeoutMS").intValue();
                        ServerAddress newPrimary = getCurrentPrimary();
                        while (newPrimary == null || newPrimary.equals(currentPrimary)) {
                            if (startTimeMillis + timeoutMillis <= System.currentTimeMillis()) {
                                fail("Timed out waiting for primary change");
                            }
                            //noinspection BusyWait
                            Thread.sleep(50);
                            newPrimary = getCurrentPrimary();
                        }
                    } else if (operationName.equals("runAdminCommand")) {
                        BsonDocument arguments = operation.getDocument("arguments");
                        BsonDocument command = arguments.getDocument("command");
                        if (arguments.containsKey("readPreference")) {
                            collectionHelper.runAdminCommand(command, helper.getReadPreference(arguments));
                        } else {
                            collectionHelper.runAdminCommand(command);
                        }
                    } else if (operationName.equals("assertSessionPinned")) {
                        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
                        assertNotNull(sessionsMap.get(arguments.getString("session").getValue()).getPinnedServerAddress());
                    } else if (operationName.equals("assertSessionUnpinned")) {
                        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
                        assertNull(sessionsMap.get(arguments.getString("session").getValue()).getPinnedServerAddress());
                    } else if (operationName.equals("assertSessionTransactionState")) {
                        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
                        ClientSession session = sessionsMap.get(arguments.getString("session").getValue());
                        String state = arguments.getString("state").getValue();
                        if (state.equals("starting") || state.equals("in_progress")) {
                            assertTrue(session.hasActiveTransaction());
                        } else {
                            assertFalse(session.hasActiveTransaction());
                        }
                    } else if (operationName.equals("endSession")) {
                        clientSession.close();
                    } else if (operation.getBoolean("error", BsonBoolean.FALSE).getValue()) {
                        try {
                            helper.getOperationResults(operation, clientSession);
                            fail("Error expected but none thrown");
                        } catch (Exception e) {
                            // Expected failure ignore
                        }
                    } else if (operationName.equals("assertDifferentLsidOnLastTwoCommands")) {
                        List<CommandEvent> events = lastTwoCommandEvents();
                        String eventsJson = commandListener.getCommandStartedEvents().stream()
                                .map(e -> ((CommandStartedEvent) e).getCommand().toJson())
                                .collect(Collectors.joining(", "));

                        assertNotEquals(eventsJson, ((CommandStartedEvent) events.get(0)).getCommand().getDocument("lsid"),
                                ((CommandStartedEvent) events.get(1)).getCommand().getDocument("lsid"));
                    } else if (operationName.equals("assertSameLsidOnLastTwoCommands")) {
                        List<CommandEvent> events = lastTwoCommandEvents();
                        String eventsJson = commandListener.getCommandStartedEvents().stream()
                                        .map(e -> ((CommandStartedEvent) e).getCommand().toJson())
                                        .collect(Collectors.joining(", "));
                        assertEquals(eventsJson, ((CommandStartedEvent) events.get(0)).getCommand().getDocument("lsid"),
                                ((CommandStartedEvent) events.get(1)).getCommand().getDocument("lsid"));
                    } else if (operationName.equals("assertSessionDirty")) {
                        assertNotNull(clientSession);
                        assertNotNull(clientSession.getServerSession());
                        assertTrue(clientSession.getServerSession().isMarkedDirty());
                    } else if (operationName.equals("assertSessionNotDirty")) {
                        assertNotNull(clientSession);
                        assertNotNull(clientSession.getServerSession());
                        assertFalse(clientSession.getServerSession().isMarkedDirty());
                    } else if (operationName.equals("assertCollectionExists")) {
                        assertCollectionExists(operation, true);
                    } else if (operationName.equals("assertCollectionNotExists")) {
                        assertCollectionExists(operation, false);
                    } else if (operationName.equals("assertIndexExists")) {
                        assertIndexExists(operation, true);
                    } else if (operationName.equals("assertIndexNotExists")) {
                        assertIndexExists(operation, false);
                    } else {
                        BsonDocument actualOutcome = helper.getOperationResults(operation, clientSession);
                        if (expectedResult != null) {
                            BsonValue actualResult = actualOutcome.get("result");
                            if (actualResult.isDocument()) {
                                ((BsonDocument) actualResult).remove("recoveryToken");
                            }

                            assertEquals("Expected operation result differs from actual", expectedResult, actualResult);
                        }
                    }
                    assertFalse(String.format("Expected error '%s' but none thrown for operation %s",
                            getErrorContainsField(expectedResult), operationName), hasErrorContainsField(expectedResult));
                    assertFalse(String.format("Expected error code '%s' but none thrown for operation %s",
                            getErrorCodeNameField(expectedResult), operationName), hasErrorCodeNameField(expectedResult));
                } catch (RuntimeException e) {
                    if (!assertExceptionState(e, expectedResult, operationName) || throwExceptions) {
                        throw e;
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        } finally {
            if (failPoint != null) {
                failPoint.disableFailPoint();
            }
        }
    }

    @Nullable
    private ServerAddress getCurrentPrimary() {
        for (ServerDescription serverDescription: mongoClient.getClusterDescription().getServerDescriptions()) {
            if (serverDescription.getType() == ServerType.REPLICA_SET_PRIMARY) {
                return serverDescription.getAddress();
            }
        }
        return null;
    }

    private Callable<Exception> createCallable(final BsonDocument operation) {
        return () -> {
            try {
                executeOperations(new BsonArray(singletonList(operation)), true);
                return null;
            } catch (Exception e) {
                if (operation.getBoolean("error", BsonBoolean.FALSE).getValue()) {
                    return null;
                }
                return e;
            } catch (Error e) {
                return new RuntimeException("Wrapping unexpected Error", e);
            }
        };
    }

    private void assertCollectionExists(final BsonDocument operation, final boolean shouldExist) {
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
        String databaseName = arguments.getString("database").getValue();
        String collection = arguments.getString("collection").getValue();
        assertEquals(shouldExist, collectionExists(databaseName, collection));
    }

    private boolean collectionExists(final String databaseName, final String collectionName) {
        return getMongoClient().getDatabase(databaseName).listCollectionNames().into(new ArrayList<>()).contains(collectionName);
    }

    private void assertIndexExists(final BsonDocument operation, final boolean shouldExist) {
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
        String db = arguments.getString("database").getValue();
        String collection = arguments.getString("collection").getValue();
        String index = arguments.getString("index").getValue();
        assertEquals(shouldExist, indexExists(db, collection, index));
    }

    private boolean indexExists(final String databaseName, final String collectionName, final String indexName) {
        ArrayList<Document> indexes = getMongoClient().getDatabase(databaseName).getCollection(collectionName).listIndexes()
                .into(new ArrayList<>());
        return indexes.stream().anyMatch(document -> document.get("name").equals(indexName));
    }

    private boolean assertExceptionState(final Exception e, final BsonValue expectedResult, final String operationName) {
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
        return passedAssertion;
    }

    private List<CommandEvent> lastTwoCommandEvents() {
        List<CommandEvent> events = commandListener.getCommandStartedEvents();
        assertTrue(events.size() >= 2);
        return events.subList(events.size() - 2, events.size());
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
        List<String> errorLabelContainsList = new ArrayList<>();
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

    private class FailPoint {
        private final BsonDocument failPointDocument;

        protected FailPoint(final BsonDocument operation) {
            this.failPointDocument = operation.getDocument("arguments").getDocument("failPoint");
        }

        public void executeFailPoint() {
            executeCommand(failPointDocument);
        }

        public void disableFailPoint() {
            executeCommand(new BsonDocument("configureFailPoint",
                    failPointDocument.getString("configureFailPoint"))
                    .append("mode", new BsonString("off")));
        }

        protected void executeCommand(final BsonDocument doc) {
            collectionHelper.runAdminCommand(doc);
        }
    }

    private class TargetedFailPoint extends FailPoint {
        private final MongoDatabase adminDB;
        private final MongoClient mongoClient;

        TargetedFailPoint(final BsonDocument operation) {
            super(operation);
            BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
            ClientSession clientSession = sessionsMap.get(arguments.getString("session").getValue());

            if (clientSession.getPinnedServerAddress() != null) {
                mongoClient = MongoClients.create(MongoClientSettings.builder()
                        .applyConnectionString(connectionString)
                        .applyToClusterSettings(builder -> builder.hosts(singletonList(clientSession.getPinnedServerAddress()))).build());

                adminDB = mongoClient.getDatabase("admin");
            } else {
                mongoClient = null;
                adminDB = null;
            }
        }

        public void disableFailPoint() {
            super.disableFailPoint();
            if (mongoClient != null) {
                mongoClient.close();
            }
        }

        protected void executeCommand(final BsonDocument doc) {
            if (adminDB != null) {
                adminDB.runCommand(doc);
            } else {
                super.executeCommand(doc);
            }
        }
    }
}
