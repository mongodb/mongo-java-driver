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

package com.mongodb.client.unified;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.Filters;
import com.mongodb.client.test.CollectionHelper;
import com.mongodb.event.CommandEvent;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.internal.connection.TestCommandListener;
import com.mongodb.internal.connection.TestConnectionPoolListener;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.AssumptionViolatedException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static com.mongodb.ClusterFixture.getServerVersion;
import static com.mongodb.client.Fixture.getMongoClient;
import static com.mongodb.client.Fixture.getMongoClientSettings;
import static com.mongodb.client.unified.RunOnRequirementsMatcher.runOnRequirementsMet;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static util.JsonPoweredTestHelper.getTestDocument;
import static util.JsonPoweredTestHelper.getTestFiles;

@RunWith(Parameterized.class)
public abstract class UnifiedTest {
    private final String schemaVersion;
    private final BsonArray runOnRequirements;
    private final BsonArray entitiesArray;
    private final BsonArray initialData;
    private final BsonDocument definition;
    private final AssertionContext context = new AssertionContext();
    private final Entities entities = new Entities();
    private final UnifiedCrudHelper crudHelper = new UnifiedCrudHelper(entities);
    private final UnifiedGridFSHelper gridFSHelper = new UnifiedGridFSHelper(entities);
    private final ValueMatcher valueMatcher = new ValueMatcher(entities, context);
    private final ErrorMatcher errorMatcher = new ErrorMatcher(context);
    private final EventMatcher eventMatcher = new EventMatcher(valueMatcher, context);
    private final List<FailPoint> failPoints = new ArrayList<>();

    public UnifiedTest(final String schemaVersion, @Nullable final BsonArray runOnRequirements, final BsonArray entitiesArray,
                       final BsonArray initialData, final BsonDocument definition) {
        this.schemaVersion = schemaVersion;
        this.runOnRequirements = runOnRequirements;
        this.entitiesArray = entitiesArray;
        this.initialData = initialData;
        this.definition = definition;
        this.context.push(ContextElement.ofTest(definition));
    }

    public Entities getEntities() {
        return entities;
    }

    @NotNull
    protected static Collection<Object[]> getTestData(final String directory) throws URISyntaxException, IOException {
        List<Object[]> data = new ArrayList<>();
        for (File file : getTestFiles("/" + directory + "/")) {
            BsonDocument fileDocument = getTestDocument(file);

            for (BsonValue cur : fileDocument.getArray("tests")) {
                data.add(UnifiedTest.createTestData(fileDocument, cur.asDocument()));
            }
        }
        return data;
    }

    @NotNull
    private static Object[] createTestData(final BsonDocument fileDocument, final BsonDocument testDocument) {
        return new Object[]{
                fileDocument.getString("description").getValue(),
                testDocument.getString("description").getValue(),
                fileDocument.getString("schemaVersion").getValue(),
                fileDocument.getArray("runOnRequirements", null),
                fileDocument.getArray("createEntities", new BsonArray()),
                fileDocument.getArray("initialData", new BsonArray()),
                testDocument};
    }

    protected abstract MongoClient createMongoClient(MongoClientSettings settings);

    @Before
    public void setUp() {
        assertTrue(schemaVersion.startsWith("1.0")
                || schemaVersion.startsWith("1.1")
                || schemaVersion.startsWith("1.2")
                || schemaVersion.startsWith("1.3")
                || schemaVersion.startsWith("1.4")
                || schemaVersion.startsWith("1.5"));
        if (runOnRequirements != null) {
            assumeTrue("Run-on requirements not met",
                    runOnRequirementsMet(runOnRequirements, getMongoClientSettings(), getServerVersion()));
        }
        if (definition.containsKey("runOnRequirements")) {
            assumeTrue("Run-on requirements not met",
                    runOnRequirementsMet(definition.getArray("runOnRequirements", new BsonArray()), getMongoClientSettings(),
                            getServerVersion()));
        }
        if (definition.containsKey("skipReason")) {
            throw new AssumptionViolatedException(definition.getString("skipReason").getValue());
        }
        entities.init(entitiesArray, this::createMongoClient);
        addInitialData();
    }

    @After
    public void cleanUp() {
        for (FailPoint failPoint : failPoints) {
            failPoint.disableFailPoint();
        }
        entities.close();
    }

    @Test
    public void shouldPassAllOutcomes() {
        BsonArray operations = definition.getArray("operations");
        for (int i = 0; i < operations.size(); i++) {
            BsonValue cur = operations.get(i);
            assertOperation(cur.asDocument(), i);
        }

        if (definition.containsKey("outcome")) {
            assertOutcome();
        }

        if (definition.containsKey("expectEvents")) {
            compareEvents(definition);
        }
    }

    private void compareEvents(final BsonDocument operation) {
        for (BsonValue cur : operation.getArray("expectEvents")) {
            BsonDocument curClientEvents = cur.asDocument();
            String client = curClientEvents.getString("client").getValue();
            String eventType = curClientEvents.getString("eventType", new BsonString("command")).getValue();
            if (eventType.equals("command")) {
                TestCommandListener listener = entities.getClientCommandListener(client);
                eventMatcher.assertCommandEventsEquality(client, curClientEvents.getArray("events"), listener.getEvents());
            } else if (eventType.equals("cmap")) {
                TestConnectionPoolListener listener = entities.getConnectionPoolListener(client);
                eventMatcher.assertConnectionPoolEventsEquality(client, curClientEvents.getArray("events"), listener.getEvents());
            } else {
                throw new UnsupportedOperationException("Unexpected event type: " + eventType);
            }
        }
    }

    private void assertOutcome() {
        for (BsonValue cur : definition.getArray("outcome")) {
            BsonDocument curDocument = cur.asDocument();
            MongoNamespace namespace = new MongoNamespace(curDocument.getString("databaseName").getValue(),
                    curDocument.getString("collectionName").getValue());
            List<BsonDocument> expectedOutcome = curDocument.getArray("documents").stream().map(BsonValue::asDocument).collect(toList());
            List<BsonDocument> actualOutcome = new CollectionHelper<>(new BsonDocumentCodec(), namespace).find();
            context.push(ContextElement.ofOutcome(namespace, expectedOutcome, actualOutcome));
            assertEquals(context.getMessage("Outcomes are not equal"), expectedOutcome, actualOutcome);
            context.pop();
        }
    }

    private void assertOperation(final BsonDocument operation, final int operationIndex) {
        OperationResult result = executeOperation(operation, operationIndex);
        context.push(ContextElement.ofCompletedOperation(operation, result, operationIndex));
        if (!operation.getBoolean("ignoreResultAndError", BsonBoolean.FALSE).getValue()) {
            if (operation.containsKey("expectResult")) {
                assertNotNull(context.getMessage("The operation expects a result but an exception occurred"), result.getResult());
                valueMatcher.assertValuesMatch(operation.get("expectResult"), result.getResult());
            } else if (operation.containsKey("expectError")) {
                assertNotNull(context.getMessage("The operation expects an error but no exception was thrown"), result.getException());
                errorMatcher.assertErrorsMatch(operation.getDocument("expectError"), result.getException());
            } else {
                assertNull(context.getMessage("The operation expects no error but an exception occurred"), result.getException());
            }
        }
        context.pop();
    }

    private OperationResult executeOperation(final BsonDocument operation, final int operationNum) {
        context.push(ContextElement.ofStartedOperation(operation, operationNum));
        String name = operation.getString("name").getValue();
        try {
            switch (name) {
                case "failPoint":
                    return executeFailPoint(operation);
                case "targetedFailPoint":
                    return executeTargetedFailPoint(operation);
                case "endSession":
                    return executeEndSession(operation);
                case "assertSessionDirty":
                    return executeAssertSessionDirty(operation);
                case "assertSessionNotDirty":
                    return executeAssertSessionNotDirty(operation);
                case "assertSessionPinned":
                    return executeAssertSessionPinned(operation);
                case "assertSessionUnpinned":
                    return executeAssertSessionUnpinned(operation);
                case "assertSameLsidOnLastTwoCommands":
                    return executeAssertSameLsidOnLastTwoCommands(operation);
                case "assertDifferentLsidOnLastTwoCommands":
                    return executeAssertDifferentLsidOnLastTwoCommands(operation);
                case "assertNumberConnectionsCheckedOut":
                    return executeAssertNumberConnectionsCheckedOut(operation);
                case "assertSessionTransactionState":
                    return executeAssertSessionTransactionState(operation);
                case "assertCollectionExists":
                    return executeAssertCollectionExists(operation);
                case "assertCollectionNotExists":
                    return executeAssertCollectionNotExists(operation);
                case "assertIndexExists":
                    return executeAssertIndexExists(operation);
                case "assertIndexNotExists":
                    return executeAssertIndexNotExists(operation);
                case "bulkWrite":
                    return crudHelper.executeBulkWrite(operation);
                case "insertOne":
                    return crudHelper.executeInsertOne(operation);
                case "insertMany":
                    return crudHelper.executeInsertMany(operation);
                case "updateOne":
                    return crudHelper.executeUpdateOne(operation);
                case "updateMany":
                    return crudHelper.executeUpdateMany(operation);
                case "replaceOne":
                    return crudHelper.executeReplaceOne(operation);
                case "deleteOne":
                    return crudHelper.executeDeleteOne(operation);
                case "deleteMany":
                    return crudHelper.executeDeleteMany(operation);
                case "aggregate":
                    return crudHelper.executeAggregate(operation);
                case "find":
                    return crudHelper.executeFind(operation);
                case "distinct":
                    return crudHelper.executeDistinct(operation);
                case "countDocuments":
                    return crudHelper.executeCountDocuments(operation);
                case "estimatedDocumentCount":
                    return crudHelper.executeEstimatedDocumentCount(operation);
                case "findOneAndUpdate":
                    return crudHelper.executeFindOneAndUpdate(operation);
                case "findOneAndReplace":
                    return crudHelper.executeFindOneAndReplace(operation);
                case "findOneAndDelete":
                    return crudHelper.executeFindOneAndDelete(operation);
                case "listDatabases":
                    return crudHelper.executeListDatabases(operation);
                case "listCollections":
                    return crudHelper.executeListCollections(operation);
                case "listIndexes":
                    return crudHelper.executeListIndexes(operation);
                case "dropCollection":
                    return crudHelper.executeDropCollection(operation);
                case "createCollection":
                    return crudHelper.executeCreateCollection(operation);
                case "createIndex":
                    return crudHelper.executeCreateIndex(operation);
                case "startTransaction":
                    return crudHelper.executeStartTransaction(operation);
                case "commitTransaction":
                    return crudHelper.executeCommitTransaction(operation);
                case "abortTransaction":
                    return crudHelper.executeAbortTransaction(operation);
                case "withTransaction":
                    return crudHelper.executeWithTransaction(operation, this::assertOperation);
                case "createFindCursor":
                    return crudHelper.createFindCursor(operation);
                case "createChangeStream":
                    return crudHelper.createChangeStreamCursor(operation);
                case "close":
                    return crudHelper.close(operation);
                case "iterateUntilDocumentOrError":
                    return crudHelper.executeIterateUntilDocumentOrError(operation);
                case "delete":
                    return gridFSHelper.executeDelete(operation);
                case "download":
                    return gridFSHelper.executeDownload(operation);
                case "upload":
                    return gridFSHelper.executeUpload(operation);
                case "runCommand":
                    return crudHelper.executeRunCommand(operation);
                case "loop":
                    return loop(operation);
                default:
                    throw new UnsupportedOperationException("Unsupported test operation: " + name);
            }
        } finally {
            context.pop();
        }
    }

    private OperationResult loop(final BsonDocument operation) {
        BsonDocument arguments = operation.getDocument("arguments");

        int numIterations = 0;
        int numSuccessfulOperations = 0;
        boolean storeFailures = arguments.containsKey("storeFailuresAsEntity");
        boolean storeErrors = arguments.containsKey("storeErrorsAsEntity");
        BsonArray failureDescriptionDocuments = new BsonArray();
        BsonArray errorDescriptionDocuments = new BsonArray();

        while (!terminateLoop()) {
            BsonArray array = arguments.getArray("operations");
            for (int i = 0; i < array.size(); i++) {
                BsonValue cur = array.get(i);
                try {
                    assertOperation(cur.asDocument().clone(), i);
                    numSuccessfulOperations++;
                } catch (AssertionError e) {
                    if (storeFailures) {
                        failureDescriptionDocuments.add(createDocumentFromException(e));
                    } else if (storeErrors) {
                        errorDescriptionDocuments.add(createDocumentFromException(e));
                    } else {
                        throw e;
                    }
                    break;
                } catch (RuntimeException e) {
                    if (storeErrors) {
                        errorDescriptionDocuments.add(createDocumentFromException(e));
                    } else if (storeFailures) {
                        failureDescriptionDocuments.add(createDocumentFromException(e));
                    } else {
                        throw e;
                    }
                    break;
                }
            }
            numIterations++;
        }

        if (arguments.containsKey("storeSuccessesAsEntity")) {
            entities.addSuccessCount(arguments.getString("storeSuccessesAsEntity").getValue(), numSuccessfulOperations);
        }
        if (arguments.containsKey("storeIterationsAsEntity")) {
            entities.addIterationCount(arguments.getString("storeIterationsAsEntity").getValue(), numIterations);
        }
        if (storeFailures) {
            entities.addFailureDocuments(arguments.getString("storeFailuresAsEntity").getValue(), failureDescriptionDocuments);
        }
        if (storeErrors) {
            entities.addErrorDocuments(arguments.getString("storeErrorsAsEntity").getValue(), errorDescriptionDocuments);
        }

        return OperationResult.NONE;
    }

    private BsonDocument createDocumentFromException(final Throwable throwable) {
        return new BsonDocument("error", new BsonString(throwable.toString()))
                .append("time", new BsonDouble(System.currentTimeMillis() / 1000.0));
    }

    protected boolean terminateLoop() {
        return true;
    }

    private OperationResult executeFailPoint(final BsonDocument operation) {
        FailPoint failPoint = FailPoint.untargeted(operation, entities);
        failPoint.executeFailPoint();
        failPoints.add(failPoint);
        return OperationResult.NONE;
    }

    private OperationResult executeTargetedFailPoint(final BsonDocument operation) {
        FailPoint failPoint = FailPoint.targeted(operation, entities);
        failPoint.executeFailPoint();
        failPoints.add(failPoint);
        return OperationResult.NONE;
    }

    private OperationResult executeEndSession(final BsonDocument operation) {
        ClientSession session = entities.getSession(operation.getString("object").getValue());
        session.close();
        return OperationResult.NONE;
    }

    private OperationResult executeAssertSessionDirty(final BsonDocument operation) {
        return executeAssertSessionDirtiness(operation, true);
    }

    private OperationResult executeAssertSessionNotDirty(final BsonDocument operation) {
        return executeAssertSessionDirtiness(operation, false);
    }

    private OperationResult executeAssertSessionDirtiness(final BsonDocument operation, final boolean expected) {
        ClientSession session = entities.getSession(operation.getDocument("arguments").getString("session").getValue());
        assertNotNull(session.getServerSession());
        assertEquals(expected, session.getServerSession().isMarkedDirty());
        return OperationResult.NONE;
    }

    private OperationResult executeAssertSessionPinned(final BsonDocument operation) {
        return executeAssertSessionPinniness(operation, true);
    }

    private OperationResult executeAssertSessionUnpinned(final BsonDocument operation) {
        return executeAssertSessionPinniness(operation, false);
    }

    private OperationResult executeAssertSessionPinniness(final BsonDocument operation, final boolean expected) {
        ClientSession session = entities.getSession(operation.getDocument("arguments").getString("session").getValue());
        assertNotNull(session.getServerSession());
        assertEquals(expected, session.getPinnedServerAddress() != null);
        return OperationResult.NONE;
    }

    private OperationResult executeAssertNumberConnectionsCheckedOut(final BsonDocument operation) {
        TestConnectionPoolListener listener = entities.getConnectionPoolListener(
                operation.getDocument("arguments").getString("client").getValue());
        assertEquals(context.getMessage("Number of checked out connections must match expected"),
                operation.getDocument("arguments").getNumber("connections").intValue(), listener.getNumConnectionsCheckedOut());
        return OperationResult.NONE;
    }

    private OperationResult executeAssertSameLsidOnLastTwoCommands(final BsonDocument operation) {
        return executeAssertLsidOnLastTwoCommands(operation, true);
    }

    private OperationResult executeAssertDifferentLsidOnLastTwoCommands(final BsonDocument operation) {
        return executeAssertLsidOnLastTwoCommands(operation, false);
    }

    private OperationResult executeAssertLsidOnLastTwoCommands(final BsonDocument operation, final boolean same) {
        TestCommandListener listener = entities.getClientCommandListener(
                operation.getDocument("arguments").getString("client").getValue());
        List<CommandEvent> events = lastTwoCommandEvents(listener);
        String eventsJson = listener.getCommandStartedEvents().stream()
                .map(e -> ((CommandStartedEvent) e).getCommand().toJson())
                .collect(Collectors.joining(", "));
        BsonDocument expected = ((CommandStartedEvent) events.get(0)).getCommand().getDocument("lsid");
        BsonDocument actual = ((CommandStartedEvent) events.get(1)).getCommand().getDocument("lsid");
        if (same) {
            assertEquals(eventsJson, expected, actual);
        } else {
            assertNotEquals(eventsJson, expected, actual);
        }
        return OperationResult.NONE;
    }

    private OperationResult executeAssertSessionTransactionState(final BsonDocument operation) {
        BsonDocument arguments = operation.getDocument("arguments");
        ClientSession session = entities.getSession(arguments.getString("session").getValue());
        String state = arguments.getString("state").getValue();
        //noinspection SwitchStatementWithTooFewBranches
        switch (state) {
            case "starting":
                assertTrue(session.hasActiveTransaction());
                break;
            default:
                throw new UnsupportedOperationException("Unsupported transaction state: " + state);
        }
        return OperationResult.NONE;
    }

    private OperationResult executeAssertCollectionExists(final BsonDocument operation) {
        assertTrue(collectionExists(operation));
        return OperationResult.NONE;
    }

    private OperationResult executeAssertCollectionNotExists(final BsonDocument operation) {
        assertFalse(collectionExists(operation));
        return OperationResult.NONE;
    }

    private boolean collectionExists(final BsonDocument operation) {
        BsonDocument arguments = operation.getDocument("arguments");
        String databaseName = arguments.getString("databaseName").getValue();
        String collectionName = arguments.getString("collectionName").getValue();
        return getMongoClient().getDatabase(databaseName)
                .listCollections().filter(Filters.eq("name", collectionName)).first() != null;
    }

    private OperationResult executeAssertIndexExists(final BsonDocument operation) {
        assertTrue(indexExists(operation));
        return OperationResult.NONE;
    }

    private OperationResult executeAssertIndexNotExists(final BsonDocument operation) {
        assertFalse(indexExists(operation));
        return OperationResult.NONE;
    }

    private boolean indexExists(final BsonDocument operation) {
        BsonDocument arguments = operation.getDocument("arguments");
        String databaseName = arguments.getString("databaseName").getValue();
        String collectionName = arguments.getString("collectionName").getValue();
        String indexName = arguments.getString("indexName").getValue();
        return getMongoClient().getDatabase(databaseName).getCollection(collectionName)
                .listIndexes(BsonDocument.class).into(new ArrayList<>()).stream()
                .anyMatch(document -> document.getString("name").getValue().equals(indexName));
    }

    private List<CommandEvent> lastTwoCommandEvents(final TestCommandListener listener) {
        List<CommandEvent> events = listener.getCommandStartedEvents();
        assertTrue(events.size() >= 2);
        return events.subList(events.size() - 2, events.size());
    }

    private void addInitialData() {
        for (BsonValue cur : initialData.getValues()) {
            BsonDocument curDataSet = cur.asDocument();
            CollectionHelper<BsonDocument> helper = new CollectionHelper<>(new BsonDocumentCodec(),
                    new MongoNamespace(curDataSet.getString("databaseName").getValue(),
                            curDataSet.getString("collectionName").getValue()));

            helper.create(WriteConcern.MAJORITY);

            BsonArray documentsArray = curDataSet.getArray("documents", new BsonArray());
            if (!documentsArray.isEmpty()) {
                helper.insertDocuments(documentsArray.stream().map(BsonValue::asDocument).collect(toList()),
                        WriteConcern.MAJORITY);
            }
        }
    }
}
