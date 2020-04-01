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

import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.test.CollectionHelper;
import com.mongodb.event.CommandEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.internal.connection.TestCommandListener;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
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
import java.util.List;

import static com.mongodb.ClusterFixture.getDefaultDatabaseName;
import static com.mongodb.ClusterFixture.isSharded;
import static com.mongodb.JsonTestServerVersionChecker.skipTest;
import static com.mongodb.client.CommandMonitoringTestHelper.assertEventsEquality;
import static com.mongodb.client.CommandMonitoringTestHelper.getExpectedEvents;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;

// See https://github.com/mongodb/specifications/tree/master/source/crud/tests
@RunWith(Parameterized.class)
public abstract class AbstractCrudTest {
    private final String filename;
    private final String description;
    private final String databaseName;
    private final String collectionName;
    private final BsonArray data;
    private final BsonDocument definition;
    private final boolean skipTest;
    private CollectionHelper<Document> collectionHelper;
    private MongoDatabase database;
    private MongoCollection<BsonDocument> collection;
    private JsonPoweredCrudTestHelper helper;
    private final TestCommandListener commandListener;

    public AbstractCrudTest(final String filename, final String description, final String databaseName, final String collectionName,
                            final BsonArray data, final BsonDocument definition, final boolean skipTest) {
        this.filename = filename;
        this.description = description;
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.data = data;
        this.definition = definition;
        this.skipTest = skipTest;
        this.commandListener = new TestCommandListener();
    }

    protected abstract void createMongoClient(CommandListener commandListener);

    protected abstract MongoDatabase getDatabase(String databaseName);


    @Before
    public void setUp() {
        assumeFalse(skipTest);
        assumeFalse(isSharded());

        collectionHelper = new CollectionHelper<>(new DocumentCodec(), new MongoNamespace(databaseName, collectionName));

        collectionHelper.killAllSessions();
        collectionHelper.create(collectionName, new CreateCollectionOptions(), WriteConcern.ACKNOWLEDGED);

        createMongoClient(commandListener);
        database = getDatabase(databaseName);

        collection = database.getCollection(collectionName, BsonDocument.class);
        helper = new JsonPoweredCrudTestHelper(description, database, collection);
        if (!data.isEmpty()) {
            List<BsonDocument> documents = new ArrayList<>();
            for (BsonValue document : data) {
                documents.add(document.asDocument());
            }

            collectionHelper.drop();
            if (documents.size() > 0) {
                collectionHelper.insertDocuments(documents, WriteConcern.MAJORITY);
            }
        }
        commandListener.reset();
    }

    @After
    public abstract void cleanUp();

    @Test
    public void shouldPassAllOutcomes() {
        assumeFalse(definition.getString("description").getValue().startsWith("Deprecated count"));

        BsonDocument expectedOutcome = definition.getDocument("outcome", null);
        // check if v1 test
        if (definition.containsKey("operation")) {
            runOperation(expectedOutcome, definition.getDocument("operation"),
                    expectedOutcome != null && expectedOutcome.containsKey("result") && expectedOutcome.isDocument("result")
                            ? expectedOutcome.get("result") : null);
        } else {  // v2 test
            BsonArray operations = definition.getArray("operations");
            for (BsonValue operation : operations) {
                runOperation(expectedOutcome, operation.asDocument(), operation.asDocument().get("result", null));
            }
        }
    }

    private void runOperation(final BsonDocument expectedOutcome, final BsonDocument operation, final BsonValue expectedResult) {
        BsonDocument outcome = null;
        boolean wasException = false;
        try {
            outcome = helper.getOperationResults(operation);
        } catch (Exception e) {
            wasException = true;
        }

        if (operation.getBoolean("error", BsonBoolean.FALSE).getValue()) {
            assertEquals(operation.containsKey("error"), wasException);
        }

        if (expectedResult != null) {
            // Hack to workaround the lack of upsertedCount
            BsonValue actualResult = outcome.get("result");
            if (actualResult.isDocument()
                    && actualResult.asDocument().containsKey("upsertedCount")
                    && actualResult.asDocument().getNumber("upsertedCount").intValue() == 0
                    && !expectedResult.asDocument().containsKey("upsertedCount")) {
                expectedResult.asDocument().append("upsertedCount", actualResult.asDocument().get("upsertedCount"));
            }
            // Hack to workaround the lack of insertedIds
            removeIdList("insertedIds", expectedResult, actualResult);
            removeIdList("upsertedIds", expectedResult, actualResult);

            removeZeroCountField("deletedCount", expectedResult, actualResult);
            removeZeroCountField("insertedCount", expectedResult, actualResult);

            assertEquals(description, expectedResult, actualResult);
        }
        if (definition.containsKey("expectations")) {
            List<CommandEvent> expectedEvents = getExpectedEvents(definition.getArray("expectations"), databaseName, null);
            List<CommandEvent> events = commandListener.getCommandStartedEvents();


            assertEventsEquality(expectedEvents, events.subList(0, expectedEvents.size()));
        }
        if (expectedOutcome != null && expectedOutcome.containsKey("collection")) {
            assertCollectionEquals(expectedOutcome.getDocument("collection"));
        }
    }

    private void removeZeroCountField(final String key, final BsonValue expectedResult, final BsonValue actualResult) {
        if (actualResult.isDocument()
                && actualResult.asDocument().containsKey(key)
                && actualResult.asDocument().getNumber(key).intValue() == 0
                && !expectedResult.asDocument().containsKey(key)) {
            actualResult.asDocument().remove(key);
        }
    }

    private void removeIdList(final String key, final BsonValue expectedResult, final BsonValue actualResult) {
        if (expectedResult.isDocument() && !expectedResult.asDocument().containsKey(key)) {
            actualResult.asDocument().remove(key);
        }
    }

    @Parameterized.Parameters(name = "{0}: {1}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        List<Object[]> data = new ArrayList<Object[]>();
        for (File file : JsonPoweredTestHelper.getTestFiles("/crud")) {
            BsonDocument testDocument = JsonPoweredTestHelper.getTestDocument(file);
            for (BsonValue test : testDocument.getArray("tests")) {
                data.add(new Object[]{file.getName(), test.asDocument().getString("description").getValue(),
                        testDocument.getString("database_name", new BsonString(getDefaultDatabaseName())).getValue(),
                        testDocument.getString("collection_name", new BsonString("test")).getValue(),
                        testDocument.getArray("data", new BsonArray()), test.asDocument(), skipTest(testDocument, test.asDocument())});
            }
        }
        return data;
    }

    private void assertCollectionEquals(final BsonDocument expectedCollection) {
        MongoCollection<BsonDocument> collectionToCompare = collection;
        if (expectedCollection.containsKey("name")) {
            collectionToCompare = database.getCollection(expectedCollection.getString("name").getValue(), BsonDocument.class);
        }
        assertEquals(description, expectedCollection.getArray("data"), collectionToCompare.find().into(new BsonArray()));
    }
}
