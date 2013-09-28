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
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


public class JavaClientOldTest extends DatabaseTestCase {

    @Test
    public void testAggregation() {
        final DBObject foo = new BasicDBObject("name", "foo").append("count", 5);
        final DBObject bar = new BasicDBObject("name", "bar").append("count", 2);
        final DBObject baz = new BasicDBObject("name", "foo").append("count", 7);
        collection.insert(foo, bar, baz);

        final DBObject projection = new BasicDBObject("name", 1).append("count", 1);

        final DBObject group = new BasicDBObject().append("_id", "$name")
            .append("docsPerName", new BasicDBObject("$sum", 1))
            .append("countPerName", new BasicDBObject("$sum", "$count"));

        final AggregationOutput out = collection.aggregate(
            Arrays.<DBObject>asList(new BasicDBObject("$project", projection), new BasicDBObject("$group", group)));

        final Map<String, DBObject> results = new HashMap<String, DBObject>();
        for (DBObject result : out.results()) {
            results.put((String) result.get("_id"), result);
        }

        final DBObject fooResult = results.get("foo");
        assertNotNull(fooResult);
        assertEquals(2, fooResult.get("docsPerName"));
        assertEquals(12, fooResult.get("countPerName"));

        final DBObject barResult = results.get("bar");
        assertNotNull(barResult);
        assertEquals(1, barResult.get("docsPerName"));
        assertEquals(2, barResult.get("countPerName"));

        final DBObject aggregationCommand = out.getCommand();
        assertNotNull(aggregationCommand);
        assertEquals(collection.getName(), aggregationCommand.get("aggregate"));
        assertNotNull(aggregationCommand.get("pipeline"));
    }

    @Test
    public void testAggregationCursor() {
        assumeTrue(serverVersionAtLeast(asList(2, 5, 3)));

        final List<DBObject> pipeline = prepareData();
        
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

    @Test()
    public void dollarOut() {
        String aggCollection = "aggCollection";
        database.getCollection(aggCollection).drop();
        Assert.assertEquals(0, database.getCollection(aggCollection).count()); 
        
        final List<DBObject> pipeline = new ArrayList<DBObject>(prepareData());
        pipeline.add(new BasicDBObject("$out", aggCollection));
        verify(pipeline, AggregationOptions.builder()
            .outputMode(AggregationOptions.OutputMode.CURSOR)
            .build());
        Assert.assertEquals(2, database.getCollection(aggCollection).count()); 
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
        final MongoCursor out = collection.aggregate(pipeline, options, ReadPreference.primary());

        final Map<String, DBObject> results = new HashMap<String, DBObject>();
        while (out.hasNext()) {
            DBObject next = out.next();
            results.put((String) next.get("_id"), next);
        }


        final DBObject fooResult = results.get("foo");
        assertNotNull(fooResult);
        assertEquals(2, fooResult.get("docsPerName"));
        assertEquals(12, fooResult.get("countPerName"));

        final DBObject barResult = results.get("bar");
        assertNotNull(barResult);
        assertEquals(1, barResult.get("docsPerName"));
        assertEquals(2, barResult.get("countPerName"));
    }
}
