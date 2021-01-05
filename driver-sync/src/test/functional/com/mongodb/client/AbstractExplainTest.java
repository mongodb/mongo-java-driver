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
import com.mongodb.client.model.Filters;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.mongodb.client.Fixture.getDefaultDatabaseName;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
    public void testExplain() {
        FindIterable<BsonDocument> iterable = client.getDatabase(getDefaultDatabaseName())
                .getCollection("explainTest", BsonDocument.class).find()
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
        assertNotNull(explainDocument);
        assertTrue(explainBsonDocument.containsKey("queryPlanner"));
        assertTrue(explainBsonDocument.containsKey("executionStats"));

        explainBsonDocument = iterable.explain(BsonDocument.class, ExplainVerbosity.QUERY_PLANNER);
        assertNotNull(explainBsonDocument);
        assertTrue(explainBsonDocument.containsKey("queryPlanner"));
        assertFalse(explainBsonDocument.containsKey("executionStats"));
    }
}
