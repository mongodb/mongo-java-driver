/*
 * Copyright (c) 2008-2016 MongoDB, Inc.
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

import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mongodb.ClusterFixture.disableMaxTimeFailPoint;
import static com.mongodb.ClusterFixture.enableMaxTimeFailPoint;
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.isSharded;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.DBObjectMatchers.hasFields;
import static com.mongodb.DBObjectMatchers.hasSubdocument;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.everyItem;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;
import static org.junit.Assume.assumeTrue;

public class MapReduceTest extends DatabaseTestCase {

    private static final String MR_DATABASE = "output-" + System.nanoTime();
    private static final String DEFAULT_COLLECTION = "jmr1_out";
    private static final String DEFAULT_MAP = "function(){ for ( var i=0; i<this.x.length; i++ ){ emit( this.x[i] , 1 ); } }";
    private static final String DEFAULT_REDUCE
    = "function(key,values){ var sum=0; for( var i=0; i<values.length; i++ ) sum += values[i]; return sum;}";

    @Override
    public void setUp() {
        super.setUp();
        collection.save(new BasicDBObject("x", new String[]{"a", "b"}).append("s", 1));
        collection.save(new BasicDBObject("x", new String[]{"b", "c"}).append("s", 2));
        collection.save(new BasicDBObject("x", new String[]{"c", "d"}).append("s", 3));
        database.getCollection(DEFAULT_COLLECTION).drop();
    }

    @AfterClass
    public static void teardownTestSuite() {
        Fixture.getMongoClient().dropDatabase(MR_DATABASE);
    }


    @Test
    public void testMapReduceInline() {
        MapReduceOutput output = collection.mapReduce(DEFAULT_MAP,
                                                      DEFAULT_REDUCE,
                                                      null,
                                                      MapReduceCommand.OutputType.INLINE,
                                                      null,
                                                      ReadPreference.primaryPreferred());

        assertNotNull(output.results());
        assertThat(output.results(), everyItem(allOf(isA(DBObject.class), hasFields(new String[]{"_id", "value"}))));
    }

    @Test(expected = MongoExecutionTimeoutException.class)
    public void testMapReduceExecutionTimeout() {
        assumeThat(isSharded(), is(false));
        enableMaxTimeFailPoint();
        try {
            MapReduceCommand command = new MapReduceCommand(collection,
                                                            DEFAULT_MAP,
                                                            DEFAULT_REDUCE,
                                                            DEFAULT_COLLECTION,
                                                            MapReduceCommand.OutputType.INLINE,
                                                            new BasicDBObject());
            command.setMaxTime(1, SECONDS);
            collection.mapReduce(command);
        } finally {
            disableMaxTimeFailPoint();
        }
    }

    @Test
    public void testWriteConcern() {
        assumeThat(isDiscoverableReplicaSet(), is(true));
        assumeTrue(serverVersionAtLeast(3, 4));
        DBCollection collection = database.getCollection("testWriteConcernForMapReduce");
        collection.insert(new BasicDBObject("x", new String[]{"a", "b"}).append("s", 1));
        collection.setWriteConcern(new WriteConcern(5));
        try {
            String anotherCollectionName = "anotherCollection" + System.nanoTime();
            collection.mapReduce(DEFAULT_MAP, DEFAULT_REDUCE, anotherCollectionName, null);
            fail("Should have thrown");
        } catch (WriteConcernException e) {
            assertEquals(100, e.getCode());
        }
    }

    @Test
    public void testMapReduce() {
        MapReduceOutput output = collection.mapReduce(DEFAULT_MAP,
                                                      DEFAULT_REDUCE,
                                                      DEFAULT_COLLECTION,
                                                      null);

        assertNotNull(output.results());

        Map<String, Integer> map = new HashMap<String, Integer>();
        for (final DBObject r : output.results()) {
            map.put(r.get("_id").toString(), ((Number) (r.get("value"))).intValue());
        }

        assertEquals(4, map.size());
        assertEquals(1, map.get("a").intValue());
        assertEquals(2, map.get("b").intValue());
        assertEquals(2, map.get("c").intValue());
        assertEquals(1, map.get("d").intValue());
    }

    @Test
    @SuppressWarnings("deprecation") // This is for testing the old API, so it will use deprecated methods
    public void testMapReduceWithOutputToAnotherDatabase() {
        MapReduceCommand command = new MapReduceCommand(collection,
                                                        DEFAULT_MAP,
                                                        DEFAULT_REDUCE,
                                                        DEFAULT_COLLECTION,
                                                        MapReduceCommand.OutputType.REPLACE,
                                                        new BasicDBObject());
        command.setOutputDB(MR_DATABASE);
        MapReduceOutput output = collection.mapReduce(command);


        DB db = database.getMongo().getDB(MR_DATABASE);
        assertTrue(db.collectionExists(DEFAULT_COLLECTION));
        assertEquals(toList(output.results()), toList(db.getCollection(DEFAULT_COLLECTION).find()));
    }


    @Test
    public void testMapReduceInlineWScope() {
        MapReduceCommand command = new MapReduceCommand(collection,
                                                        "function(){ for (var i=0; i<this.x.length; i++ ){ if(this.x[i] != exclude) "
                                                        + "emit( this.x[i] , 1 ); } }",
                                                        DEFAULT_REDUCE,
                                                        null,
                                                        MapReduceCommand.OutputType.INLINE,
                                                        null);

        Map<String, Object> scope = new HashMap<String, Object>();
        scope.put("exclude", "a");
        command.setScope(scope);

        List<DBObject> resultsAsList = toList(collection.mapReduce(command).results());

        assertThat(resultsAsList, not(hasItem(hasSubdocument(new BasicDBObject("_id", "a")))));
        assertThat(resultsAsList, hasItem(hasSubdocument(new BasicDBObject("_id", "b"))));
    }

    @Test
    public void testOutputCollection() {
        String anotherCollectionName = "anotherCollection" + System.nanoTime();
        MapReduceOutput output = collection.mapReduce(DEFAULT_MAP,
                                                      DEFAULT_REDUCE,
                                                      anotherCollectionName, null);

        assertEquals(database.getCollection(anotherCollectionName).getFullName(), output.getOutputCollection().getFullName());
        assertTrue(database.collectionExists(anotherCollectionName));

        output.drop();

        assertFalse(database.collectionExists(anotherCollectionName));
    }


    @Test
    public void testOutputTypeMerge() {

        database.getCollection(DEFAULT_COLLECTION).insert(new BasicDBObject("z", 10));

        MapReduceOutput output = collection.mapReduce(DEFAULT_MAP,
                                                      DEFAULT_REDUCE,
                                                      DEFAULT_COLLECTION,
                                                      MapReduceCommand.OutputType.MERGE,
                                                      null);

        List<DBObject> documents = toList(output.results());

        assertThat(documents, hasItem(hasSubdocument(new BasicDBObject("z", 10))));
        assertThat(documents, hasItem(hasSubdocument(new BasicDBObject("_id", "a").append("value", 1.0))));
    }

    @Test
    public void testOutputTypeReduce() {
        //TODO: what exactly is this testing?
        collection.mapReduce(DEFAULT_MAP,
                             DEFAULT_REDUCE,
                             DEFAULT_COLLECTION,
                             MapReduceCommand.OutputType.REDUCE,
                             null);
    }

    @Test
    public void testMapReduceWithFinalize() {
        MapReduceCommand command = new MapReduceCommand(collection,
                                                        DEFAULT_MAP,
                                                        DEFAULT_REDUCE,
                                                        DEFAULT_COLLECTION,
                                                        MapReduceCommand.OutputType.REPLACE, new BasicDBObject()
        );
        command.setFinalize("function(key,reducedValue){ return reducedValue*5; }");

        List<DBObject> output = toList(collection.mapReduce(command).results());

        assertThat(output, hasItem(hasSubdocument(new BasicDBObject("_id", "b").append("value", 10.0))));
    }

    @Test
    public void testMapReduceWithQuery() {
        MapReduceCommand command = new MapReduceCommand(collection,
                                                        DEFAULT_MAP,
                                                        DEFAULT_REDUCE,
                                                        DEFAULT_COLLECTION,
                                                        MapReduceCommand.OutputType.REPLACE,
                                                        new BasicDBObject("x", "a"));

        MapReduceOutput output = collection.mapReduce(command);

        Map<String, Object> map = toMap(output.results());
        assertEquals(2, map.size());
        assertEquals(1.0, map.get("a"));
        assertEquals(1.0, map.get("b"));
    }

    @Test
    @Ignore("Not sure about the behavior of sort")
    public void testMapReduceWithSort() {
        collection.createIndex(new BasicDBObject("s", 1));

        MapReduceCommand command = new MapReduceCommand(collection,
                                                        DEFAULT_MAP,
                                                        DEFAULT_REDUCE,
                                                        DEFAULT_COLLECTION,
                                                        MapReduceCommand.OutputType.REPLACE,
                                                        new BasicDBObject("x", "a"));

        command.setSort(new BasicDBObject("s", -1));
        command.setLimit(1);

        MapReduceOutput output = collection.mapReduce(command);

        Map<String, Object> map = toMap(output.results());
        assertEquals(2, map.size());
        assertEquals(1.0, map.get("c"));
        assertEquals(1.0, map.get("d"));
    }

    @Test
    public void testMapReduceWithLimit() {
        MapReduceCommand command = new MapReduceCommand(collection,
                                                        DEFAULT_MAP,
                                                        DEFAULT_REDUCE,
                                                        DEFAULT_COLLECTION,
                                                        MapReduceCommand.OutputType.INLINE,
                                                        new BasicDBObject());

        command.setLimit(1);

        MapReduceOutput output = collection.mapReduce(command);

        Map<String, Object> map = toMap(output.results());
        assertEquals(2, map.size());
        assertEquals(1.0, map.get("a"));
        assertEquals(1.0, map.get("b"));
    }

    @Test
    public void shouldReturnStatisticsForInlineMapReduce() {
        MapReduceCommand command = new MapReduceCommand(collection,
                                                        DEFAULT_MAP,
                                                        DEFAULT_REDUCE,
                                                        DEFAULT_COLLECTION,
                                                        MapReduceCommand.OutputType.INLINE,
                                                        new BasicDBObject());

        //when
        MapReduceOutput output = collection.mapReduce(command);

        //then
        //duration is not working on the unstable server version
        //        assertThat(output.getDuration(), is(greaterThan(0)));
        assertThat(output.getEmitCount(), is(6));
        assertThat(output.getInputCount(), is(3));
        assertThat(output.getOutputCount(), is(4));
    }

    @Test
    public void shouldReturnStatisticsForMapReduceIntoACollection() {
        MapReduceCommand command = new MapReduceCommand(collection,
                                                        DEFAULT_MAP,
                                                        DEFAULT_REDUCE,
                                                        DEFAULT_COLLECTION,
                                                        MapReduceCommand.OutputType.REPLACE,
                                                        new BasicDBObject());

        //when
        MapReduceOutput output = collection.mapReduce(command);

        //then
        assertThat(output.getDuration(), is(greaterThanOrEqualTo(0)));
        assertThat(output.getEmitCount(), is(6));
        assertThat(output.getInputCount(), is(3));
        assertThat(output.getOutputCount(), is(4));
    }


    //TODO: test read preferences - always go to primary for non-inline.  Presumably do whatever if inline

    private List<DBObject> toList(final Iterable<DBObject> results) {
        List<DBObject> resultsAsList = new ArrayList<DBObject>();
        for (final DBObject result : results) {
            resultsAsList.add(result);
        }
        return resultsAsList;
    }

    private Map<String, Object> toMap(final Iterable<DBObject> result) {
        Map<String, Object> map = new HashMap<String, Object>();
        for (final DBObject document : result) {
            map.put((String) document.get("_id"), document.get("value"));
        }
        return map;
    }
}
