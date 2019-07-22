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

import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.FutureResultCallback;
import com.mongodb.client.CommandMonitoringTestHelper;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import com.mongodb.client.test.CollectionHelper;
import com.mongodb.event.CommandEvent;
import com.mongodb.internal.connection.TestCommandListener;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;
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
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import static com.mongodb.JsonTestServerVersionChecker.skipTest;
import static com.mongodb.async.client.Fixture.getMongoClientBuilderFromConnectionString;
import static com.mongodb.client.CommandMonitoringTestHelper.getExpectedEvents;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeNotNull;

// See https://github.com/mongodb/specifications/tree/master/source/retryable-writes/tests
@RunWith(Parameterized.class)
public class ChangeStreamsTest extends DatabaseTestCase {
    private final String filename;
    private final String description;
    private final MongoNamespace namespace;
    private final MongoNamespace namespace2;
    private final BsonDocument definition;
    private final boolean skipTest;

    private MongoClient mongoClient;
    private TestCommandListener commandListener;

    public ChangeStreamsTest(final String filename, final String description, final MongoNamespace namespace,
                             final MongoNamespace namespace2, final BsonDocument definition, final boolean skipTest) {
        this.filename = filename;
        this.description = description;
        this.namespace = namespace;
        this.namespace2 = namespace2;
        this.definition = definition;
        this.skipTest = skipTest;
    }

    @BeforeClass
    public static void beforeClass() {
    }

    @AfterClass
    public static void afterClass() {
    }

    @Before
    public void setUp() {
        assumeFalse(skipTest);
        CollectionHelper.dropDatabase(namespace.getDatabaseName(), WriteConcern.MAJORITY);
        CollectionHelper<BsonDocument> collectionHelper = new CollectionHelper<BsonDocument>(new BsonDocumentCodec(), namespace);
        collectionHelper.drop();
        collectionHelper.create();

        CollectionHelper.dropDatabase(namespace2.getDatabaseName(), WriteConcern.MAJORITY);
        CollectionHelper<BsonDocument> collectionHelper2 = new CollectionHelper<BsonDocument>(new BsonDocumentCodec(), namespace2);
        collectionHelper2.drop();
        collectionHelper2.create();

        if (definition.containsKey("failPoint")) {
            collectionHelper.runAdminCommand(definition.getDocument("failPoint"));
        }

        commandListener = new TestCommandListener();
        mongoClient = MongoClients.create(getMongoClientBuilderFromConnectionString().addCommandListener(commandListener).build());
    }

    @After
    public void cleanUp() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    @Test
    public void shouldPassAllOutcomes() {
        BsonDocument result = definition.getDocument("result");
        AsyncBatchCursor<ChangeStreamDocument<BsonDocument>> cursor = createCursor(result);
        if (cursor == null) {
            return;
        }

        try {
            handleOperations();

            checkStreamValues(result, cursor);
        } finally {
            cursor.close();
        }

        checkExpectations();
    }

    private void checkStreamValues(final BsonDocument result, final AsyncBatchCursor<ChangeStreamDocument<BsonDocument>> cursor){

        BsonArray expectedResults = result.getArray("success", new BsonArray());
        Queue<ChangeStreamDocument<BsonDocument>> results = null;

        try {
            results = getResults(cursor);
        } catch (MongoException e) {
            if (result.containsKey("error")) {
                final BsonDocument error = result.getDocument("error");
                assertTrue(e.getCode() == error.getInt32("code").intValue()
                        || !Collections.disjoint(e.getErrorLabels(), error.getArray("errorLabels")));
                return;
            } else {
                throw e;
            }
        }
        for (BsonValue expectedResult : expectedResults) {
            BsonDocument expected = expectedResult.asDocument();

            if (results.isEmpty()) {
                results = getResults(cursor);
            }
            ChangeStreamDocument<BsonDocument> actual = results.poll();
            assumeNotNull(actual);

            MongoNamespace expectedNamespace = null;
            if (expected.containsKey("ns")) {
                BsonDocument nsDocument = expected.getDocument("ns");
                expectedNamespace = nsDocument != null
                        ? new MongoNamespace(nsDocument.getString("db").getValue(), nsDocument.getString("coll").getValue())
                        : null;
            }
            assertEquals(expectedNamespace, actual.getNamespace());
            assertEquals(OperationType.fromString(expected.getString("operationType").getValue()), actual.getOperationType());
            if (actual.getFullDocument() != null) {
                actual.getFullDocument().remove("_id");
            }

            assertEquals(expected.get("fullDocument"), actual.getFullDocument());
        }
    }

    private Queue<ChangeStreamDocument<BsonDocument>> getResults(final AsyncBatchCursor<ChangeStreamDocument<BsonDocument>> cursor) {
        FutureResultCallback<List<ChangeStreamDocument<BsonDocument>>> callback =
                new FutureResultCallback<List<ChangeStreamDocument<BsonDocument>>>();
        cursor.next(callback);
        return new LinkedList<ChangeStreamDocument<BsonDocument>>(futureResult(callback));
    }

    @Nullable
    private AsyncBatchCursor<ChangeStreamDocument<BsonDocument>> createCursor(final BsonDocument result) {
        AsyncBatchCursor<ChangeStreamDocument<BsonDocument>> cursor;
        try {
            cursor = createChangeStreamCursor();
        } catch (MongoException e) {
            assertEquals(result.getDocument("error", new BsonDocument()).getInt32("code", new BsonInt32(-1)).getValue(), e.getCode());
            return null;
        }
        FutureResultCallback<List<ChangeStreamDocument<BsonDocument>>> callback =
                new FutureResultCallback<List<ChangeStreamDocument<BsonDocument>>>();
        cursor.tryNext(callback);
        assertNull(futureResult(callback));
        return cursor;
    }

    private void checkExpectations() {
        if (definition.containsKey("expectations") && definition.getArray("expectations").size() > 0) {

            String database = definition.getString("target").getValue().equals("client") ? "admin" : namespace.getDatabaseName();
            List<CommandEvent> expectedEvents = getExpectedEvents(definition.getArray("expectations"), database, new BsonDocument());
            List<CommandEvent> events = commandListener.getEvents();

            for (int i = 0; i < expectedEvents.size(); i++) {
                CommandEvent expectedEvent = expectedEvents.get(i);
                CommandEvent event = events.get(i);
                CommandMonitoringTestHelper.assertEventsEquality(singletonList(expectedEvent), singletonList(event));
            }
        }
    }


    private AsyncBatchCursor<ChangeStreamDocument<BsonDocument>> createChangeStreamCursor() {
        String target = definition.getString("target").getValue();
        List<BsonDocument> pipeline = new ArrayList<BsonDocument>();
        for (BsonValue bsonValue : definition.getArray("changeStreamPipeline", new BsonArray())) {
            pipeline.add(bsonValue.asDocument());
        }

        ChangeStreamIterable<BsonDocument> changeStreamIterable;

        if (target.equals("client")) {
            changeStreamIterable = mongoClient.watch(pipeline, BsonDocument.class);
        } else if (target.equals("database")) {
            changeStreamIterable = mongoClient.getDatabase(namespace.getDatabaseName()).watch(pipeline, BsonDocument.class);
        } else if (target.equals("collection")) {
            changeStreamIterable = mongoClient.getDatabase(namespace.getDatabaseName()).getCollection(namespace.getCollectionName())
                    .watch(pipeline, BsonDocument.class);
        } else {
            throw new IllegalArgumentException(format("Unknown target: %s", target));
        }

        BsonDocument options = definition.getDocument("changeStreamOptions", new BsonDocument());

        if (options.containsKey("batchSize")) {
            changeStreamIterable.batchSize(options.getNumber("batchSize").intValue());
        }


        FutureResultCallback<AsyncBatchCursor<ChangeStreamDocument<BsonDocument>>> callback =
                new FutureResultCallback<AsyncBatchCursor<ChangeStreamDocument<BsonDocument>>>();


        changeStreamIterable.batchCursor(callback);
        return futureResult(callback);
    }

    private void handleOperations() {
        for (BsonValue operations : definition.getArray("operations")) {
            BsonDocument op = operations.asDocument();
            MongoNamespace opNamespace = new MongoNamespace(op.getString("database").getValue(), op.getString("collection").getValue());
            createJsonPoweredCrudTestHelper(Fixture.getMongoClient(), opNamespace).getOperationResults(op);
        }
    }

    private JsonPoweredCrudTestHelper createJsonPoweredCrudTestHelper(final MongoClient localMongoClient, final MongoNamespace namespace) {
        return new JsonPoweredCrudTestHelper(description, localMongoClient.getDatabase(namespace.getDatabaseName()),
                localMongoClient.getDatabase(namespace.getDatabaseName()).getCollection(namespace.getCollectionName(), BsonDocument.class));
    }

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        List<Object[]> data = new ArrayList<Object[]>();
        for (File file : JsonPoweredTestHelper.getTestFiles("/change-streams")) {
            BsonDocument testDocument = JsonPoweredTestHelper.getTestDocument(file);
            MongoNamespace namespace = new MongoNamespace(testDocument.getString("database_name").getValue(),
                    testDocument.getString("collection_name").getValue());
            MongoNamespace namespace2 = new MongoNamespace(testDocument.getString("database2_name").getValue(),
                    testDocument.getString("collection2_name").getValue());
            for (BsonValue test : testDocument.getArray("tests")) {
                data.add(new Object[]{file.getName(), test.asDocument().getString("description").getValue(),
                        namespace, namespace2, test.asDocument(), skipTest(testDocument, test.asDocument())});
            }
        }
        return data;
    }

    <T> T futureResult(final FutureResultCallback<T> callback) {
        try {
            return callback.get(5, TimeUnit.SECONDS);
        } catch (Throwable t) {
            throw MongoException.fromThrowable(t);
        }
    }

}
