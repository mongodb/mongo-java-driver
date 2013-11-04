package com.mongodb;

import com.mongodb.AggregationOptions.OutputMode;
import com.mongodb.util.TestCase;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;

public class AggregationTest extends TestCase {

    private DBCollection collection;
    private DB database;

    @BeforeTest
    public void collection() {
        cleanupDB = "com_mongodb_AggregationTest";
        database = cleanupMongo.getDB(cleanupDB);
        collection = database.getCollection(getClass().getSimpleName() + System.nanoTime());
    }

    public void checkServerVersion() {
        if (!serverIsAtLeastVersion(2.5)) {
            throw new SkipException("Server is too old for this test.");
        }
    }

    @BeforeMethod
    private void dropCollection() {
        collection.drop();
    }

    @AfterTest
    public void cleanup() {
        database.dropDatabase();
    }

    @Test
    public void testAggregation() {
        validate(buildPipeline());
    }

    @Test
    public void testOldAggregationWithOut() {
        checkServerVersion();
        List<DBObject> pipeline = new ArrayList<DBObject>(buildPipeline());
        pipeline.add(new BasicDBObject("$out", "aggCollection"));
        final AggregationOutput out = collection.aggregate(pipeline);
        assertFalse(out.results().iterator().hasNext());
        assertEquals(database.getCollection("aggCollection")
                .count(), 2);
    }

    @Test
    public void testExplain() {
        checkServerVersion();
        List<DBObject> pipeline = new ArrayList<DBObject>(buildPipeline());
        pipeline.add(new BasicDBObject("$out", "aggCollection"));
        final CommandResult out = collection.explainAggregate(pipeline, AggregationOptions.builder()
                .allowDiskUsage(true)
                .outputMode(AggregationOptions.OutputMode.CURSOR)
                .build());
        assertTrue(out.keySet().iterator().hasNext());
    }
    
    @Test(expectedExceptions = {IllegalArgumentException.class})
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
        checkServerVersion();
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

    @Test
    public void testInlineAndDollarOut() {
        checkServerVersion();
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
        checkServerVersion();
        String aggCollection = "aggCollection";
        database.getCollection(aggCollection)
                .drop();
        Assert.assertEquals(database.getCollection(aggCollection)
                .count(), 0);

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
        checkServerVersion();
        if (isStandalone(cleanupMongo)) {
            throw new SkipException("Test can only be run against replica sets.");
        }
        ServerAddress primary = new ServerAddress("localhost");
        MongoClient rsClient = new MongoClient(asList(primary, new ServerAddress("localhost", 27018)));
        DB rsDatabase = rsClient.getDB(database.getName());
        DBCollection aggCollection = rsDatabase.getCollection(collection.getName());
        aggCollection.drop();

        final List<DBObject> pipeline = new ArrayList<DBObject>(prepareData());
        pipeline.add(new BasicDBObject("$out", "aggCollection"));
        AggregationOptions options = AggregationOptions.builder()
                .outputMode(OutputMode.CURSOR)
                .build();
        MongoCursor cursor = verify(pipeline, options, ReadPreference.secondary(), aggCollection);
        assertEquals(2, rsDatabase.getCollection("aggCollection").count());
        assertEquals(primary, cursor.getServerAddress());
    }

    @Test(enabled = false)
    public void testAggregateOnSecondary() throws UnknownHostException {
        checkServerVersion();
        if (isStandalone(cleanupMongo)) {
            return;
        }
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
        MongoCursor cursor = verify(pipeline, options, ReadPreference.secondary(), aggCollection);
        assertNotEquals(primary, cursor.getServerAddress());
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

    private MongoCursor verify(final List<DBObject> pipeline, final AggregationOptions options, final ReadPreference readPreference,
                               final DBCollection collection) {
        final MongoCursor cursor = collection.aggregate(pipeline, options, readPreference);

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
