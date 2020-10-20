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
import com.mongodb.client.MongoClient;
import com.mongodb.client.test.CollectionHelper;
import com.mongodb.connection.StreamFactoryFactory;
import com.mongodb.event.CommandEvent;
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

import java.util.List;

import static com.mongodb.ClusterFixture.getServerVersion;
import static com.mongodb.client.unified.EventMatcher.assertEventsEquality;
import static com.mongodb.client.unified.RunOnRequirementsMatcher.runOnRequirementsMet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
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
        entities.close();
    }

    @Test
    public void shouldPassAllOutcomes() {
        for (BsonValue cur : definition.getArray("operations")) {
            BsonDocument operation = cur.asDocument();
            OperationResult result = executeOperation(operation);
            if (operation.containsKey("expectResult")) {
                ValueMatcher.assertValuesMatch(operation.get("expectResult"), requireNonNull(result.getResult()));
            }
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
            TestCommandListener listener = entities.getClientCommandListeners().get(curClientEvents.getString("client").getValue());
            List<CommandEvent> expectedEvents = EventMatcher.getExpectedEvents(curClientEvents.getArray("events"));
            List<CommandEvent> events = listener.getEvents();

            assertEquals(String.format("Actual number of events (%d) is less than expected number of events (%d)",
                    events.size(), expectedEvents.size()), expectedEvents.size(), events.size());
            assertEventsEquality(expectedEvents, events.subList(0, expectedEvents.size()));
        }
    }

    private void assertOutcome() {
        for (BsonValue cur : definition.getArray("outcome")) {
            BsonDocument curDocument = cur.asDocument();
            MongoNamespace namespace = new MongoNamespace(curDocument.getString("databaseName").getValue(),
                    curDocument.getString("collectionName").getValue());
            List<BsonDocument> collectionData = new CollectionHelper<BsonDocument>(new BsonDocumentCodec(), namespace).find();
            assertEquals(curDocument.getArray("documents").stream().map(BsonValue::asDocument).collect(toList()),
                    collectionData);
        }
    }

    private OperationResult executeOperation(final BsonDocument operation) {
        String name = operation.getString("name").getValue();
        try {
            switch (name) {
                case "bulkWrite":
                    return crudHelper.executeBulkWrite(operation);
                case "insertMany":
                    return crudHelper.executeInsertMany(operation);
                case "replaceOne":
                    return crudHelper.executeReplaceOne(operation);
                case "aggregate":
                    return crudHelper.executeAggregate(operation);
                case "find":
                    return crudHelper.executeFind(operation);
                default:
                    throw new UnsupportedOperationException("Unsupported test operation: " + name);
            }
        } catch (UnsupportedOperationException e) {
            throw e;
        } catch (Exception e) {
            // TODO: need more robust error assertions
            if (!operation.containsKey("expectError")) {
                throw e;
            }
        }
        return new OperationResult();
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
