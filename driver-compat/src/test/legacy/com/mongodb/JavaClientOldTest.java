/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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
import org.junit.Ignore;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeTrue;
import static org.mongodb.Fixture.serverVersionAtLeast;

public class JavaClientOldTest extends DatabaseTestCase {

    @Test
    public void testAggregation() {
        DBObject foo = new BasicDBObject("name", "foo").append("count", 5);
        DBObject bar = new BasicDBObject("name", "bar").append("count", 2);
        DBObject baz = new BasicDBObject("name", "foo").append("count", 7);
        collection.insert(foo, bar, baz);

        DBObject projection = new BasicDBObject("name", 1).append("count", 1);

        DBObject group = new BasicDBObject().append("_id", "$name")
                                            .append("docsPerName", new BasicDBObject("$sum", 1))
                                            .append("countPerName", new BasicDBObject("$sum", "$count"));

        AggregationOutput out = collection.aggregate(Arrays.<DBObject>asList(new BasicDBObject("$project", projection),
                                                                             new BasicDBObject("$group", group)));

        Map<String, DBObject> results = new HashMap<String, DBObject>();
        for (final DBObject result : out.results()) {
            results.put((String) result.get("_id"), result);
        }

        DBObject fooResult = results.get("foo");
        assertNotNull(fooResult);
        assertEquals(2, fooResult.get("docsPerName"));
        assertEquals(12, fooResult.get("countPerName"));

        DBObject barResult = results.get("bar");
        assertNotNull(barResult);
        assertEquals(1, barResult.get("docsPerName"));
        assertEquals(2, barResult.get("countPerName"));

        DBObject aggregationCommand = out.getCommand();
        assertNotNull(aggregationCommand);
        assertEquals(collection.getName(), aggregationCommand.get("aggregate"));
        assertNotNull(aggregationCommand.get("pipeline"));
    }

    @Test
    public void testAggregationCursor() {
        assumeTrue(serverVersionAtLeast(asList(2, 5, 3)));

        List<DBObject> pipeline = prepareData();

        verify(pipeline, AggregationOptions.builder()
                                           .batchSize(1)
                                           .outputMode(AggregationOptions.OutputMode.CURSOR)
                                           .allowDiskUsage(true)
                                           .build());

        verify(pipeline, AggregationOptions.builder()
                                           .batchSize(1)
                                           .outputMode(AggregationOptions.OutputMode.INLINE)
                                           .allowDiskUsage(true)
                                           .build());

        verify(pipeline, AggregationOptions.builder()
                                           .batchSize(1)
                                           .outputMode(AggregationOptions.OutputMode.CURSOR)
                                           .build());
    }

    @Test
    public void inlineAndDollarOut() {
        assumeTrue(serverVersionAtLeast(asList(2, 5, 3)));
        String aggCollection = "aggCollection";
        database.getCollection(aggCollection)
                .drop();
        assertEquals(0, database.getCollection(aggCollection)
                                .count());
        List<DBObject> pipeline = new ArrayList<DBObject>(prepareData());
        pipeline.add(new BasicDBObject("$out", aggCollection));

        AggregationOutput out = collection.aggregate(pipeline);
        assertFalse(out.results()
                       .iterator()
                       .hasNext());
        assertEquals(2, database.getCollection(aggCollection)
                                .count());
    }

    @Test
    public void dollarOut() {
        assumeTrue(serverVersionAtLeast(asList(2, 5, 3)));
        String aggCollection = "aggCollection";
        database.getCollection(aggCollection)
                .drop();
        Assert.assertEquals(0, database.getCollection(aggCollection)
                                       .count());

        List<DBObject> pipeline = new ArrayList<DBObject>(prepareData());
        pipeline.add(new BasicDBObject("$out", aggCollection));
        verify(pipeline, AggregationOptions.builder()
                                           .outputMode(AggregationOptions.OutputMode.CURSOR)
                                           .build());
        assertEquals(2, database.getCollection(aggCollection)
                                .count());
    }

    @Test
    public void dollarOutOnSecondary() throws UnknownHostException {
        assumeTrue(serverVersionAtLeast(asList(2, 5, 3)));
        ServerAddress primary = new ServerAddress("localhost");
        Mongo mongo = new MongoClient(asList(primary,
                                             new ServerAddress("localhost", 27018),
                                             new ServerAddress("localhost", 27019)));

        if (isStandalone(mongo)) {
            return;
        }
        DB rsDatabase = mongo.getDB(database.getName());
        DBCollection aggCollection = rsDatabase.getCollection(collection.getName());
        aggCollection.drop();

        List<DBObject> pipeline = new ArrayList<DBObject>(prepareData());
        pipeline.add(new BasicDBObject("$out", "aggCollection"));
        AggregationOptions options = AggregationOptions.builder()
                                                       .outputMode(AggregationOptions.OutputMode.CURSOR)
                                                       .build();
        MongoCursor cursor = verify(pipeline, options, ReadPreference.secondary(), aggCollection);
        assertEquals(2, rsDatabase.getCollection("aggCollection")
                                  .count());
        assertEquals(primary, cursor.getServerAddress());
    }

    private boolean isStandalone(final Mongo mongo) {
        return mongo.getCluster().getDescription().getSecondaries() == null;
    }

    @Test
    @Ignore
    public void aggregateOnSecondary() throws UnknownHostException {
        Mongo mongo = new MongoClient(asList(new ServerAddress("localhost"),
                                             new ServerAddress("localhost", 27018),
                                             new ServerAddress("localhost", 27019)));

        if (isStandalone(mongo)) {
            return;
        }
        ServerAddress primary = new ServerAddress("localhost");
        ServerAddress secondary = new ServerAddress("localhost", 27018);
        MongoClient rsClient = new MongoClient(asList(primary, secondary));
        DB rsDatabase = rsClient.getDB(database.getName());
        rsDatabase.dropDatabase();
        DBCollection aggCollection = rsDatabase.getCollection(collection.getName());
        aggCollection.drop();

        List<DBObject> pipeline = new ArrayList<DBObject>(prepareData());
        AggregationOptions options = AggregationOptions.builder()
                                                       .outputMode(AggregationOptions.OutputMode.INLINE)
                                                       .build();
        MongoCursor cursor = verify(pipeline, options, ReadPreference.secondary(), aggCollection);
        assertNotEquals(primary, cursor.getServerAddress());
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
    public void testOldAggregationWithOut() {
        assumeTrue(serverVersionAtLeast(asList(2, 5, 3)));
        collection.drop();
        List<DBObject> pipeline = new ArrayList<DBObject>(prepareData());
        pipeline.add(new BasicDBObject("$out", "aggCollection"));
        AggregationOutput out = collection.aggregate(pipeline);
        assertFalse(out.results()
                       .iterator()
                       .hasNext());
        assertEquals(database.getCollection("aggCollection")
                             .count(), 2);
    }

    @Test
    public void testOldAggregationWithOutOnSecondary() throws UnknownHostException {
        assumeTrue(serverVersionAtLeast(asList(2, 5, 3)));
        collection.drop();
        List<DBObject> pipeline = new ArrayList<DBObject>(prepareData());
        pipeline.add(new BasicDBObject("$out", "aggCollection"));
        AggregationOutput out = collection.aggregate(pipeline, ReadPreference.secondary());
        assertFalse(out.results()
                       .iterator()
                       .hasNext());
        assertEquals(database.getCollection("aggCollection")
                             .count(), 2);
        assertEquals(new ServerAddress("localhost"), out.getCommandResult()
                                                        .getServerUsed());
    }

    private void verify(final List<DBObject> pipeline, final AggregationOptions options) {
        verify(pipeline, options, ReadPreference.primary());
    }

    private void verify(final List<DBObject> pipeline, final AggregationOptions options, final ReadPreference readPreference) {
        verify(pipeline, options, readPreference, collection);
    }

    private MongoCursor verify(final List<DBObject> pipeline, final AggregationOptions options, final ReadPreference readPreference,
                               final DBCollection collection) {
        MongoCursor cursor = collection.aggregate(pipeline, options, readPreference);

        Map<String, DBObject> results = new HashMap<String, DBObject>();
        while (cursor.hasNext()) {
            DBObject next = cursor.next();
            results.put((String) next.get("_id"), next);
        }


        DBObject fooResult = results.get("foo");
        assertNotNull(fooResult);
        assertEquals(2, fooResult.get("docsPerName"));
        assertEquals(12, fooResult.get("countPerName"));

        DBObject barResult = results.get("bar");
        assertNotNull(barResult);
        assertEquals(1, barResult.get("docsPerName"));
        assertEquals(2, barResult.get("countPerName"));

        return cursor;
    }
}
