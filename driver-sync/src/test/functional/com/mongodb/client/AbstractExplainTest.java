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

import com.mongodb.ExplainVerbosity;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.internal.connection.TestCommandListener;
import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.Fixture.getDefaultDatabaseName;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

public abstract class AbstractExplainTest {
    private static final Logger LOGGER = Loggers.getLogger(AbstractExplainTest.class.getSimpleName());
    private MongoClient client;
    private TestCommandListener commandListener;

    protected abstract MongoClient createMongoClient(MongoClientSettings settings);

    @Before
    public void setUp() {
        commandListener = new TestCommandListener();
        client = createMongoClient(Fixture.getMongoClientSettingsBuilder().addCommandListener(commandListener).build());
    }

    @After
    public void tearDown() {
        client.close();
        commandListener.reset();
    }

    @Test
    public void testExplainOfFind() {
        MongoCollection<BsonDocument> collection = client.getDatabase(getDefaultDatabaseName())
                .getCollection("explainTest", BsonDocument.class);
        collection.drop();
        collection.insertOne(new BsonDocument("_id", new BsonInt32(1)));

        FindIterable<BsonDocument> iterable = collection.find()
                .filter(Filters.eq("_id", 1));

        Document explainDocument = iterable.explain();
        assertNotNull(explainDocument);
        assertTrue(explainDocument.containsKey("queryPlanner"));
        assertTrue(explainDocument.containsKey("executionStats"));

        explainDocument = iterable.explain(ExplainVerbosity.QUERY_PLANNER);
        assertNotNull(explainDocument);
        assertTrue(explainDocument.containsKey("queryPlanner"));
        assertFalse(explainDocument.containsKey("executionStats"));

        BsonDocument explainBsonDocument = iterable.explain(BsonDocument.class);
        assertNotNull(explainBsonDocument);
        assertTrue(explainBsonDocument.containsKey("queryPlanner"));
        assertTrue(explainBsonDocument.containsKey("executionStats"));

        explainBsonDocument = iterable.explain(BsonDocument.class, ExplainVerbosity.QUERY_PLANNER);
        assertNotNull(explainBsonDocument);
        assertTrue(explainBsonDocument.containsKey("queryPlanner"));
        assertFalse(explainBsonDocument.containsKey("executionStats"));
    }

    @Test
    public void testFindContainsMaxTimeMsInExplain() {
        //given
        MongoCollection<BsonDocument> collection = client.getDatabase(getDefaultDatabaseName())
                .getCollection("explainTest", BsonDocument.class);

        FindIterable<BsonDocument> iterable = collection.find()
                .maxTime(500, TimeUnit.MILLISECONDS);

        //when
        iterable.explain();

        //then
        assertExplainableCommandContainMaxTimeMS();
    }

    @Test
    public void testAggregateContainsMaxTimeMsInExplain() {
        //given
        MongoCollection<BsonDocument> collection = client.getDatabase(getDefaultDatabaseName())
                .getCollection("explainTest", BsonDocument.class);

        AggregateIterable<BsonDocument> iterable = collection.aggregate(
                singletonList(Aggregates.match(Filters.eq("_id", 1))))
                .maxTime(500, TimeUnit.MILLISECONDS);

        //when
        iterable.explain();

        //then
        assertExplainableCommandContainMaxTimeMS();
    }

    @Test
    public void testListSearchIndexesContainsMaxTimeMsInExplain() {
        //given
        assumeTrue(serverVersionAtLeast(6, 0));
        MongoCollection<BsonDocument> collection = client.getDatabase(getDefaultDatabaseName())
                .getCollection("explainTest", BsonDocument.class);

        ListSearchIndexesIterable<Document> iterable = collection.listSearchIndexes()
                .maxTime(500, TimeUnit.MILLISECONDS);

        //when
        try {
            iterable.explain();
        } catch (MongoCommandException throwable) {
            //we expect listSearchIndexes is only supported in Atlas Search in some deployments.
        }

        //then
        assertExplainableCommandContainMaxTimeMS();
    }

    @Test
    public void testExplainOfAggregateWithNewResponseStructure() {
        MongoCollection<BsonDocument> collection = client.getDatabase(getDefaultDatabaseName())
                .getCollection("explainTest", BsonDocument.class);
        collection.drop();
        collection.insertOne(new BsonDocument("_id", new BsonInt32(1)));

        AggregateIterable<BsonDocument> iterable = collection
                .aggregate(singletonList(Aggregates.match(Filters.eq("_id", 1))));

        Document explainDocument = getAggregateExplainDocument(iterable.explain());
        assertTrue(explainDocument.containsKey("queryPlanner"));
        assertTrue(explainDocument.containsKey("executionStats"));

        explainDocument = getAggregateExplainDocument(iterable.explain(ExplainVerbosity.QUERY_PLANNER));
        assertNotNull(explainDocument);
        assertTrue(explainDocument.containsKey("queryPlanner"));
        assertFalse(explainDocument.containsKey("executionStats"));

        BsonDocument explainBsonDocument = getAggregateExplainDocument(iterable.explain(BsonDocument.class));
        assertNotNull(explainBsonDocument);
        assertTrue(explainBsonDocument.containsKey("queryPlanner"));
        assertTrue(explainBsonDocument.containsKey("executionStats"));

        explainBsonDocument = getAggregateExplainDocument(iterable.explain(BsonDocument.class, ExplainVerbosity.QUERY_PLANNER));
        assertNotNull(explainBsonDocument);
        assertTrue(explainBsonDocument.containsKey("queryPlanner"));
        assertFalse(explainBsonDocument.containsKey("executionStats"));
    }

    @Test
    public void testExplainWithMaxTimeMS() {
        // Create a collection with the namespace explain-test.collection
        MongoCollection<Document> collection = client.getDatabase("explain-test")
                .getCollection("collection");
        collection.drop();

        // Insert a test document
        collection.insertOne(new Document("name", "john doe"));

        // Reset command listener to capture only the explain command
        commandListener.reset();

        client.getDatabase("admin")
                .runCommand(BsonDocument.parse("{"
                + "    configureFailPoint: \"failCommand\","
                + "    mode: {"
                + "        times: 1"
                + "    },"
                + "    data: {"
                + "        failCommands: [\"explain\"],"
                + "        blockConnection: true,"
                + "        blockTimeMS: 100"
                + "    }"
                + "}"));

        // Run an explained find with the query predicate { name: 'john doe' }
        // Add a $where clause with JavaScript sleep to introduce delay that exceeds timeout
        // Use a short timeout (50ms) with a longer sleep (100ms) to trigger timeout
        FindIterable<Document> findIterable = collection.find(
                Filters.and(
                    Filters.eq("name", "john doe")
                )
        );

        // Execute the explain with short timeoutMS - this should trigger a MongoOperationTimeoutException
        try {
            findIterable.explain(50L); // Use 50ms timeout to trigger timeout
            fail("Expected MongoOperationTimeoutException but explain completed successfully");
        } catch (MongoOperationTimeoutException e) {
            LOGGER.info("Reported error: ", e);

            // This is expected - the operation should timeout
            assertTrue("Exception message should mention timeout",
                    e.getMessage().toLowerCase().contains("operation exceeded the timeout limit") || e.getMessage().toLowerCase().contains("timeout"));

            // Verify that the explain command was sent with maxTimeMS before timing out
            // The command should have been started even though it timed out
            assertFalse("At least one command should have been started", commandListener.getCommandStartedEvents().isEmpty());

            CommandStartedEvent explainEvent = commandListener.getCommandStartedEvent("explain");
            // Confirm that the explain command had maxTimeMS set
            BsonDocument explainCommand = explainEvent.getCommand();
            assertTrue("Explain command should contain maxTimeMS when timeoutMS is specified",
                    explainCommand.containsKey("maxTimeMS"));

            // The maxTimeMS value should be derived from the timeoutMS (may be less due to RTT calculation)
            // Handle both INT32 and INT64 types
            long maxTimeMSValue;
            if (explainCommand.get("maxTimeMS").isInt32()) {
                maxTimeMSValue = explainCommand.getInt32("maxTimeMS").getValue();
            } else {
                maxTimeMSValue = explainCommand.getInt64("maxTimeMS").getValue();
            }
            assertTrue("maxTimeMS should be positive", maxTimeMSValue > 0);
            assertTrue("maxTimeMS should be <= timeoutMS", maxTimeMSValue <= 50);

            // Verify the explain command structure
            assertTrue("Explain command should contain explain field", explainCommand.containsKey("explain"));

            // The inner find command should contain the correct filter
            BsonDocument findCommand = explainCommand.getDocument("explain");
            assertTrue("Find command should contain filter", findCommand.containsKey("filter"));
            BsonDocument filter = findCommand.getDocument("filter");
            assertTrue("Filter should contain $and", filter.containsKey("$and"));
        }
    }

    // Post-MongoDB 7.0, sharded cluster responses move the explain plan document into a "shards" document, which a plan for each shard.
    // This method grabs the explain plan document from the first shard when this new structure is present.
    private static Document getAggregateExplainDocument(final Document rootAggregateExplainDocument) {
        assertNotNull(rootAggregateExplainDocument);
        Document aggregateExplainDocument = rootAggregateExplainDocument;
        if (rootAggregateExplainDocument.containsKey("shards")) {
            Document shardDocument = rootAggregateExplainDocument.get("shards", Document.class);
            String firstKey = shardDocument.keySet().iterator().next();
            aggregateExplainDocument = shardDocument.get(firstKey, Document.class);
        }
        return aggregateExplainDocument;
    }

    private static BsonDocument getAggregateExplainDocument(final BsonDocument rootAggregateExplainDocument) {
        assertNotNull(rootAggregateExplainDocument);
        BsonDocument aggregateExplainDocument = rootAggregateExplainDocument;
        if (rootAggregateExplainDocument.containsKey("shards")) {
            BsonDocument shardDocument = rootAggregateExplainDocument.getDocument("shards");
            String firstKey = shardDocument.getFirstKey();
            aggregateExplainDocument = shardDocument.getDocument(firstKey);
        }
        return aggregateExplainDocument;
    }

    private void assertExplainableCommandContainMaxTimeMS() {
        assertEquals(1, commandListener.getCommandStartedEvents().size());
        CommandStartedEvent explain = commandListener.getCommandStartedEvent("explain");
        BsonDocument explainCommand = explain.getCommand();
        BsonDocument explainableCommand = explainCommand.getDocument("explain");

        assertFalse(explainCommand.containsKey("maxTimeMS"));
        assertTrue(explainableCommand.containsKey("maxTimeMS"));
    }
}
