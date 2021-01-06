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
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.ClusterFixture.serverVersionLessThan;
import static com.mongodb.client.Fixture.getDefaultDatabaseName;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public abstract class AbstractExplainTest {

    private MongoClient client;

    protected abstract MongoClient createMongoClient(MongoClientSettings settings);

    @Before
    public void setUp() {
        client = createMongoClient(Fixture.getMongoClientSettings());
    }

    @After
    public void tearDown() {
        client.close();
    }

    @Test
    public void testExplainOfFind() {
        assumeTrue(serverVersionAtLeast(3, 0));

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
    public void testExplainOfAggregateWithNewResponseStructure() {
        // Aggregate explain is supported on earlier versions, but the structure of the response on which we're asserting in this test
        // changed radically in 4.2.
        assumeTrue(serverVersionAtLeast(4, 2));

        MongoCollection<BsonDocument> collection = client.getDatabase(getDefaultDatabaseName())
                .getCollection("explainTest", BsonDocument.class);
        collection.drop();
        collection.insertOne(new BsonDocument("_id", new BsonInt32(1)));

        AggregateIterable<BsonDocument> iterable = collection
                .aggregate(singletonList(Aggregates.match(Filters.eq("_id", 1))));

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
    public void testExplainOfAggregateWithOldResponseStructure() {
        // Aggregate explain is supported on earlier versions, but the structure of the response on which we're asserting in this test
        // changed radically in 4.2. So here we just assert that we got a non-error respinse
        assumeTrue(serverVersionAtLeast(3, 6));
        assumeTrue(serverVersionLessThan(4, 2));

        MongoCollection<BsonDocument> collection = client.getDatabase(getDefaultDatabaseName())
                .getCollection("explainTest", BsonDocument.class);
        collection.drop();
        collection.insertOne(new BsonDocument("_id", new BsonInt32(1)));

        AggregateIterable<BsonDocument> iterable = collection
                .aggregate(singletonList(Aggregates.match(Filters.eq("_id", 1))));

        Document explainDocument = iterable.explain();
        assertNotNull(explainDocument);

        explainDocument = iterable.explain(ExplainVerbosity.QUERY_PLANNER);
        assertNotNull(explainDocument);

        BsonDocument explainBsonDocument = iterable.explain(BsonDocument.class);
        assertNotNull(explainBsonDocument);

        explainBsonDocument = iterable.explain(BsonDocument.class, ExplainVerbosity.QUERY_PLANNER);
        assertNotNull(explainBsonDocument);
    }
}
