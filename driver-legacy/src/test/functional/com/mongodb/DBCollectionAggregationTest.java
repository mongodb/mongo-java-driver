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

package com.mongodb;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mongodb.ClusterFixture.clusterIsType;
import static com.mongodb.ClusterFixture.disableMaxTimeFailPoint;
import static com.mongodb.ClusterFixture.enableMaxTimeFailPoint;
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.isSharded;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.connection.ClusterType.REPLICA_SET;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;
import static org.junit.Assume.assumeTrue;

public class DBCollectionAggregationTest extends DatabaseTestCase {

    @Test
    public void testAggregationCursor() {
        List<DBObject> pipeline = prepareData();

        verify(pipeline, AggregationOptions.builder()
                                           .batchSize(1)
                                           .allowDiskUse(true)
                                           .build());

        verify(pipeline, AggregationOptions.builder()
                                           .batchSize(1)
                                           .build());
    }

    @Test
    public void testDollarOut() {
        String aggCollection = "aggCollection";
        database.getCollection(aggCollection)
            .drop();
        Assert.assertEquals(0, database.getCollection(aggCollection)
                                       .count());

        List<DBObject> pipeline = new ArrayList<DBObject>(prepareData());
        pipeline.add(new BasicDBObject("$out", aggCollection));
        verify(pipeline, AggregationOptions.builder()
                                           .build());
        assertEquals(2, database.getCollection(aggCollection)
            .count());
    }

    @Test
    public void testDollarOutOnSecondary() throws InterruptedException {
        assumeTrue(clusterIsType(REPLICA_SET));

        List<DBObject> pipeline = new ArrayList<DBObject>(prepareData());
        pipeline.add(new BasicDBObject("$out", "aggCollection"));
        AggregationOptions options = AggregationOptions.builder()
            .build();
        ServerAddress serverAddress = verify(pipeline, options, ReadPreference.secondary(), collection);
        assertEquals(2, database.getCollection("aggCollection")
            .count());
        assertEquals(Fixture.getPrimary(), serverAddress);
    }

    public List<DBObject> prepareData() {
        collection.remove(new BasicDBObject());

        DBObject foo = new BasicDBObject("name", "foo").append("count", 5);
        DBObject bar = new BasicDBObject("name", "bar").append("count", 2);
        DBObject baz = new BasicDBObject("name", "foo").append("count", 7);
        collection.insert(foo, bar, baz);

        DBObject projection = new BasicDBObject("name", 1).append("count", 1);

        DBObject group = new BasicDBObject().append("_id", "$name")
            .append("docsPerName", new BasicDBObject("$sum", 1))
            .append("countPerName", new BasicDBObject("$sum", "$count"));
        return Arrays.<DBObject>asList(new BasicDBObject("$project", projection), new BasicDBObject("$group", group));
    }

    @Test
    public void testExplain() {
        List<DBObject> pipeline = new ArrayList<DBObject>(prepareData());
        pipeline.add(new BasicDBObject("$out", "aggCollection"));
        CommandResult out = collection.explainAggregate(pipeline, AggregationOptions.builder()
            .allowDiskUse(true)
            .build());
        assertTrue(out.keySet()
                      .iterator()
                      .hasNext());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullOptions() {
        collection.aggregate(new ArrayList<DBObject>(), null);
    }

    @Test
    public void testMaxTime() {
        assumeThat(isSharded(), is(false));
        enableMaxTimeFailPoint();
        DBCollection collection = database.getCollection("testMaxTime");
        try {
            collection.aggregate(prepareData(), AggregationOptions.builder().maxTime(1, SECONDS).build());
            fail("Show have thrown");
        } catch (MongoExecutionTimeoutException e) {
            assertEquals(50, e.getCode());
        } finally {
            disableMaxTimeFailPoint();
        }
    }

    @Test
    public void testWriteConcern() {
        assumeThat(isDiscoverableReplicaSet(), is(true));
        assumeTrue(serverVersionAtLeast(3, 4));
        DBCollection collection = database.getCollection("testWriteConcern");
        collection.setWriteConcern(new WriteConcern(5));
        try {
            collection.aggregate(asList(new BasicDBObject("$out", "copy")), AggregationOptions.builder().build());
            fail("Should have thrown");
        } catch (WriteConcernException e) {
            assertEquals(100, e.getCode());
        }
    }

    private void verify(final List<DBObject> pipeline, final AggregationOptions options) {
        verify(pipeline, options, ReadPreference.primary());
    }

    private void verify(final List<DBObject> pipeline, final AggregationOptions options, final ReadPreference readPreference) {
        verify(pipeline, options, readPreference, collection);
    }

    private ServerAddress verify(final List<DBObject> pipeline, final AggregationOptions options, final ReadPreference readPreference,
        final DBCollection collection) {
        Cursor cursor = collection.aggregate(pipeline, options, readPreference);
        ServerAddress serverAddress;
        Map<String, DBObject> results;
        try {
            results = new HashMap<String, DBObject>();
            while (cursor.hasNext()) {
                DBObject next = cursor.next();
                results.put((String) next.get("_id"), next);
            }
        } finally {
            serverAddress = cursor.getServerAddress();
            cursor.close();
        }


        DBObject fooResult = results.get("foo");
        assertNotNull(fooResult);
        assertEquals(2, fooResult.get("docsPerName"));
        assertEquals(12, fooResult.get("countPerName"));

        DBObject barResult = results.get("bar");
        assertNotNull(barResult);
        assertEquals(1, barResult.get("docsPerName"));
        assertEquals(2, barResult.get("countPerName"));

        return serverAddress;
    }
}
