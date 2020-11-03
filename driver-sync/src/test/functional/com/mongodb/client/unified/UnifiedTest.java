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
import com.mongodb.connection.StreamFactoryFactory;
import com.mongodb.event.CommandEvent;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.internal.connection.TestCommandListener;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;
import org.junit.After;
import org.junit.AssumptionViolatedException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.mongodb.ClusterFixture.getServerVersion;
import static com.mongodb.client.Fixture.getMongoClient;
import static com.mongodb.client.unified.RunOnRequirementsMatcher.runOnRequirementsMet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
public abstract class UnifiedTest {
    private final String schemaVersion;
    private final BsonArray runOnRequirements;
    private final BsonArray entitiesArray;
    private final BsonArray initialData;
    private final BsonDocument definition;
    private final Entities entities = new Entities();
    private final UnifiedCrudHelper crudHelper = new UnifiedCrudHelper(entities);
    private final UnifiedGridFSHelper gridFSHelper = new UnifiedGridFSHelper(entities);
    private final ValueMatcher valueMatcher = new ValueMatcher(entities);
    private final EventMatcher eventMatcher = new EventMatcher(valueMatcher);
    private final List<FailPoint> failPoints = new ArrayList<>();

    public UnifiedTest(final String schemaVersion, @Nullable final BsonArray runOnRequirements, final BsonArray entitiesArray,
                       final BsonArray initialData, final BsonDocument definition) {
        this.schemaVersion = schemaVersion;
        this.runOnRequirements = runOnRequirements;
        this.entitiesArray = entitiesArray;
        this.initialData = initialData;
        this.definition = definition;
    }

    protected abstract MongoClient createMongoClient(MongoClientSettings settings);

    @Nullable
    protected StreamFactoryFactory getStreamFactoryFactory() {
        return null;
    }

    @Before
    public void setUp() {
        assertTrue(schemaVersion.startsWith("1.0"));
        if (runOnRequirements != null) {
            assumeTrue("Run-on requirements not met", runOnRequirementsMet(runOnRequirements, getServerVersion()));
        }
        if (definition.containsKey("runOnRequirements")) {
            assumeTrue("Run-on requirements not met",
                    runOnRequirementsMet(definition.getArray("runOnRequirements", new BsonArray()), getServerVersion()));
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
        for (BsonValue cur : definition.getArray("operations")) {
            assertOperation(cur.asDocument());
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
            TestCommandListener listener = entities.getClientCommandListener(curClientEvents.getString("client").getValue());
            eventMatcher.assertEventsEquality(curClientEvents.getArray("events"), listener.getEvents());
        }
    }

    private void assertOutcome() {
        for (BsonValue cur : definition.getArray("outcome")) {
            BsonDocument curDocument = cur.asDocument();
            MongoNamespace namespace = new MongoNamespace(curDocument.getString("databaseName").getValue(),
                    curDocument.getString("collectionName").getValue());
            List<BsonDocument> collectionData = new CollectionHelper<>(new BsonDocumentCodec(), namespace).find();
            assertEquals(curDocument.getArray("documents").stream().map(BsonValue::asDocument).collect(toList()),
                    collectionData);
        }
    }

    private void assertOperation(final BsonDocument operation) {
        OperationResult result = executeOperation(operation);
        if (operation.containsKey("expectResult")) {
            valueMatcher.assertValuesMatch(operation.get("expectResult"), requireNonNull(result.getResult()));
        } else if (operation.containsKey("expectError")) {
            ErrorMatcher.assertErrorsMatch(operation.getDocument("expectError"), requireNonNull(result.getException()));
        }
    }

    private OperationResult executeOperation(final BsonDocument operation) {
        String name = operation.getString("name").getValue();
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
            case "replaceOne":
                return crudHelper.executeReplaceOne(operation);
            case "deleteOne":
                return crudHelper.executeDeleteOne(operation);
            case "aggregate":
                return crudHelper.executeAggregate(operation);
            case "find":
                return crudHelper.executeFind(operation);
            case "findOneAndUpdate":
                return crudHelper.executeFindOneAndUpdate(operation);
            case "listDatabases":
                return crudHelper.executeListDatabases(operation);
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
            case "createChangeStream":
                return crudHelper.executeChangeStream(operation);
            case "iterateUntilDocumentOrError":
                return crudHelper.executeIterateUntilDocumentOrError(operation);
            case "delete":
                return gridFSHelper.executeDelete(operation);
            case "download":
                return gridFSHelper.executeDownload(operation);
            case "upload":
                return gridFSHelper.executeUpload(operation);
            default:
                throw new UnsupportedOperationException("Unsupported test operation: " + name);
        }
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
