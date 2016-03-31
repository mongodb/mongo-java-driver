/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import com.mongodb.AggregationOptions.OutputMode;
import com.mongodb.util.TestCase;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

public class AggregationTest extends TestCase {

    private DBCollection collection;
    private DB database;

    @Before
    public void before() {
        database = getDatabase();
        collection = database.getCollection(getClass().getSimpleName() + System.nanoTime());
        collection.drop();
    }

    @Test
    public void testAggregation() {
        validate(buildPipeline());
    }

    @Test
    public void testOldAggregationWithOut() {
        checkServerVersion(2.6);
        List<DBObject> pipeline = new ArrayList<DBObject>(buildPipeline());
        pipeline.add(new BasicDBObject("$out", "aggCollection"));
        final AggregationOutput out = collection.aggregate(pipeline);
        assertFalse(out.results().iterator().hasNext());
        assertEquals(database.getCollection("aggCollection")
                .count(), 2);
    }

    @Test
    public void testExplain() {
        checkServerVersion(2.6);
        List<DBObject> pipeline = new ArrayList<DBObject>(buildPipeline());
        pipeline.add(new BasicDBObject("$out", "aggCollection"));
        final CommandResult out = collection.explainAggregate(pipeline, AggregationOptions.builder()
                .allowDiskUse(true)
                .outputMode(AggregationOptions.OutputMode.CURSOR)
                .build());
        assertTrue(out.keySet().iterator().hasNext());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullOptions() {
        collection.aggregate(new ArrayList<DBObject>(), (AggregationOptions) null);
    }

    private void validate(List<DBObject> pipeline) {
        final AggregationOutput out = collection.aggregate(pipeline);

        final Map<String, DBObject> results = new HashMap<String, DBObject>();
        for (DBObject result : out.results()) {
            results.put((String) result.get("_id"), result);
        }

        final DBObject fooResult = results.get("foo");
        assertNotNull(fooResult);
        assertEquals(fooResult.get("docsPerName"), 2);
        assertEquals(fooResult.get("countPerName"), 12);

        final DBObject barResult = results.get("bar");
        assertNotNull(barResult);
        assertEquals(barResult.get("docsPerName"), 1);
        assertEquals(barResult.get("countPerName"), 2);

        final DBObject aggregationCommand = out.getCommand();
        assertNotNull(aggregationCommand);
        assertEquals(aggregationCommand.get("aggregate"), collection.getName());
        assertNotNull(aggregationCommand.get("pipeline"));
    }

    private List<DBObject> buildPipeline() {
        final DBObject foo = new BasicDBObject("name", "foo").append("count", 5);
        final DBObject bar = new BasicDBObject("name", "bar").append("count", 2);
        final DBObject baz = new BasicDBObject("name", "foo").append("count", 7);
        collection.insert(foo, bar, baz);

        final DBObject projection = new BasicDBObject("name", 1).append("count", 1);

        final DBObject group = new BasicDBObject().append("_id", "$name")
                .append("docsPerName", new BasicDBObject("$sum", 1))
                .append("countPerName", new BasicDBObject("$sum", "$count"));

        return Arrays.<DBObject>asList(new BasicDBObject("$project", projection), new BasicDBObject("$group", group));
    }

    @Test
    public void testAggregationCursor() {
        checkServerVersion(2.6);
        final List<DBObject> pipeline = prepareData();

        verify(pipeline, AggregationOptions.builder()
                .batchSize(1)
                .outputMode(AggregationOptions.OutputMode.CURSOR)
                .allowDiskUse(true)
                .build());

        verify(pipeline, AggregationOptions.builder()
                                           .batchSize(1)
                                           .outputMode(AggregationOptions.OutputMode.INLINE)
                                           .allowDiskUse(true)
                                           .build());

        verify(pipeline, AggregationOptions.builder()
                .batchSize(1)
                .outputMode(AggregationOptions.OutputMode.CURSOR)
                .build());
    }

    @Test
    public void testInlineAndDollarOut() {
        checkServerVersion(2.6);
        String aggCollection = "aggCollection";
        database.getCollection(aggCollection)
                .drop();
        assertEquals(0, database.getCollection(aggCollection)
                                .count());
        final List<DBObject> pipeline = new ArrayList<DBObject>(prepareData());
        pipeline.add(new BasicDBObject("$out", aggCollection));

        final AggregationOutput out = collection.aggregate(pipeline);
        assertFalse(out.results()
                .iterator()
                .hasNext());
        assertEquals(database.getCollection(aggCollection)
                             .count(), 2);
    }

    @Test
    public void testDollarOut() {
        checkServerVersion(2.6);
        String aggCollection = "aggCollection";
        database.getCollection(aggCollection)
                .drop();
        assertEquals(database.getCollection(aggCollection).count(), 0);

        final List<DBObject> pipeline = new ArrayList<DBObject>(prepareData());
        pipeline.add(new BasicDBObject("$out", aggCollection));
        verify(pipeline, AggregationOptions.builder()
                                           .outputMode(AggregationOptions.OutputMode.CURSOR)
                                           .build());
        assertEquals(2, database.getCollection(aggCollection)
                .count());
    }

    @Test
    public void testDollarOutOnSecondary() throws UnknownHostException {
        checkServerVersion(2.6);
        assumeTrue(isReplicaSet(cleanupMongo));

        ServerAddress primary = new ServerAddress("localhost");
        MongoClient rsClient = new MongoClient(getMongoClientURI());
        DB rsDatabase = rsClient.getDB(database.getName());
        DBCollection aggCollection = rsDatabase.getCollection(collection.getName());
        aggCollection.drop();

        final List<DBObject> pipeline = new ArrayList<DBObject>(prepareData());
        pipeline.add(new BasicDBObject("$out", "aggCollection"));
        AggregationOptions options = AggregationOptions.builder()
                .outputMode(OutputMode.CURSOR)
                .build();
        Cursor cursor = verify(pipeline, options, ReadPreference.secondary(), aggCollection);
        assertEquals(2, rsDatabase.getCollection("aggCollection").count());
        assertEquals(primary.getSocketAddress(), cursor.getServerAddress().getSocketAddress());
    }

    @Test
    @Ignore
    public void testAggregateOnSecondary() throws UnknownHostException {
        checkServerVersion(2.6);
        assumeTrue(isReplicaSet(cleanupMongo));

        ServerAddress primary = new ServerAddress("localhost");
        ServerAddress secondary = new ServerAddress("localhost", 27018);
        MongoClient rsClient = new MongoClient(asList(primary, secondary));
        DB rsDatabase = rsClient.getDB(database.getName());
        rsDatabase.dropDatabase();
        DBCollection aggCollection = rsDatabase.getCollection(collection.getName());
        aggCollection.drop();

        final List<DBObject> pipeline = new ArrayList<DBObject>(prepareData());
        AggregationOptions options = AggregationOptions.builder()
                .outputMode(OutputMode.INLINE)
                .build();
        Cursor cursor = verify(pipeline, options, ReadPreference.secondary(), aggCollection);
        assertNotEquals(primary, cursor.getServerAddress());
    }

    @Test
    public void testMaxTime() {
        assumeFalse(isSharded(getMongoClient()));
        checkServerVersion(2.6);
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
    public void testMaxTimeWithCustomDecoderThatSwallowsAllFields() {
        assumeFalse(isSharded(getMongoClient()));
        checkServerVersion(2.6);
        enableMaxTimeFailPoint();
        DBCollection collection = database.getCollection("testMaxTime");
        collection.setDBDecoderFactory(new SubsitutingDBDecoderFactory(new BasicDBObject()));
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
    public void testInvalidPipelineThrowsError() {
        checkServerVersion(2.6);
        DBCollection collection = database.getCollection("testInvalidPipeline");
        List<DBObject> invalidPipeline = asList((DBObject) new BasicDBObject("name", "foo"));
        try {
            collection.aggregate(invalidPipeline);
            fail("Show have thrown");
        } catch (CommandFailureException e) {
            // continue
        }

        try {
            collection.aggregate(invalidPipeline, AggregationOptions.builder().build());
            fail("Show have thrown");
        } catch (CommandFailureException e) {
            // continue
        }
    }

    public List<DBObject> prepareData() {
        collection.remove(new BasicDBObject());

        final DBObject foo = new BasicDBObject("name", "foo").append("count", 5);
        final DBObject bar = new BasicDBObject("name", "bar").append("count", 2);
        final DBObject baz = new BasicDBObject("name", "foo").append("count", 7);
        collection.insert(foo, bar, baz);

        final DBObject projection = new BasicDBObject("name", 1).append("count", 1);

        final DBObject group = new BasicDBObject().append("_id", "$name")
                .append("docsPerName", new BasicDBObject("$sum", 1))
                .append("countPerName", new BasicDBObject("$sum", "$count"));
        return Arrays.<DBObject>asList(new BasicDBObject("$project", projection), new BasicDBObject("$group", group));
    }

    private void verify(final List<DBObject> pipeline, final AggregationOptions options) {
        verify(pipeline, options, ReadPreference.primary());
    }

    private void verify(final List<DBObject> pipeline, final AggregationOptions options, final ReadPreference readPreference) {
        verify(pipeline, options, readPreference, collection);
    }

    private Cursor verify(final List<DBObject> pipeline, final AggregationOptions options, final ReadPreference readPreference,
                               final DBCollection collection) {
        final Cursor cursor = collection.aggregate(pipeline, options, readPreference);

        final Map<String, DBObject> results = new HashMap<String, DBObject>();
        while (cursor.hasNext()) {
            DBObject next = cursor.next();
            results.put((String) next.get("_id"), next);
        }

        final DBObject fooResult = results.get("foo");
        assertNotNull(fooResult);
        assertEquals(fooResult.get("docsPerName"), 2);
        assertEquals(fooResult.get("countPerName"), 12);

        final DBObject barResult = results.get("bar");
        assertNotNull(barResult);
        assertEquals(barResult.get("docsPerName"), 1);
        assertEquals(barResult.get("countPerName"), 2);

        return cursor;
    }

}
