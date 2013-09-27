package com.mongodb;


import org.junit.Test;

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

        final DBObject group = new BasicDBObject().append("_id", "$name").append("docsPerName", new BasicDBObject("$sum", 1)).append(
            "countPerName", new BasicDBObject("$sum", "$count"));

        final AggregationOutput out = collection.aggregate(Arrays.<DBObject>asList(new BasicDBObject("$project", projection), 
            new BasicDBObject("$group", group)));

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
        final DBObject foo = new BasicDBObject("name", "foo").append("count", 5);
        final DBObject bar = new BasicDBObject("name", "bar").append("count", 2);
        final DBObject baz = new BasicDBObject("name", "foo").append("count", 7);
        collection.insert(foo, bar, baz);

        final DBObject projection = new BasicDBObject("name", 1).append("count", 1);

        final DBObject group = new BasicDBObject().append("_id", "$name").append("docsPerName", new BasicDBObject("$sum", 1)).append(
            "countPerName", new BasicDBObject("$sum", "$count"));

        verify(projection, group, AggregationOptions.builder()
            .batchSize(1)
            .outputMode(AggregationOptions.OutputMode.CURSOR)
            .allowDiskUsage(true)
            .build());
        verify(projection, group, AggregationOptions.builder()
            .batchSize(1)
            .outputMode(AggregationOptions.OutputMode.INLINE)
            .allowDiskUsage(true)
            .build());
        verify(projection, group, AggregationOptions.builder().batchSize(1).outputMode(AggregationOptions.OutputMode.CURSOR).build());
    }

    private void verify(final DBObject projection, final DBObject group, final AggregationOptions options) {
        List<DBObject> pipeline = Arrays.<DBObject>asList(new BasicDBObject("$project", projection), new BasicDBObject("$group", group));
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
