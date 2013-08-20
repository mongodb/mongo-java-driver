package com.mongodb;

import org.junit.Test;

import java.util.HashMap;
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

        final DBObject projFields = new BasicDBObject("name", 1).append("count", 1);

        final DBObject group = new BasicDBObject()
                .append("_id", "$name")
                .append("docsPerName", new BasicDBObject("$sum", 1))
                .append("countPerName", new BasicDBObject("$sum", "$count"));

        final AggregationOutput out = collection.aggregate(
                new BasicDBObject("$project", projFields),
                new BasicDBObject("$group", group)
        );

        final Map<String, DBObject> results = new HashMap<String, DBObject>();
        for (DBObject result : out.results()){
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
}
