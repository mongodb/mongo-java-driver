/*
 * Copyright (c) 2008 - 2013 10gen, Incollection. <http://10gen.com>
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mongodb.DBObjectMatchers.hasFields;
import static com.mongodb.DBObjectMatchers.hasSubdocument;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.everyItem;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class MapReduceTest extends DatabaseTestCase {

    private static final String MR_DATABASE = "output-" + System.nanoTime();
    private static final String DEFAULT_COLLECTION = "jmr1_out";
    private static final String DEFAULT_MAP = "function(){ for ( var i=0; i<this.x.length; i++ ){ emit( this.x[i] , 1 ); } }";
    private static final String DEFAULT_REDUCE = "function(key,values){ var sum=0; for( var i=0; i<values.length; i++ ) sum += values[i]; return sum;}";

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
        final MapReduceOutput output = collection.mapReduce(
                DEFAULT_MAP,
                DEFAULT_REDUCE,
                null,
                MapReduceCommand.OutputType.INLINE,
                null,
                ReadPreference.primaryPreferred()
        );

        assertNotNull(output.results());
        assertThat(output.results(), everyItem(
                allOf(isA(DBObject.class), hasFields(new String[]{"_id", "value"}))
        ));
    }

    @Test
    public void testMapReduce() {
        final MapReduceOutput output = collection.mapReduce(
                DEFAULT_MAP,
                DEFAULT_REDUCE,
                DEFAULT_COLLECTION,
                null
        );

        assertNotNull(output.results());

        final Map<String, Integer> map = new HashMap<String, Integer>();
        for (DBObject r : output.results()) {
            map.put(r.get("_id").toString(), ((Number) (r.get("value"))).intValue());
        }

        assertEquals(4, map.size());
        assertEquals(1, map.get("a").intValue());
        assertEquals(2, map.get("b").intValue());
        assertEquals(2, map.get("c").intValue());
        assertEquals(1, map.get("d").intValue());
    }

    @Test
    public void testMapReduceWithOutputToAnotherDatabase() {
        final MapReduceCommand command = new MapReduceCommand(
                collection,
                DEFAULT_MAP,
                DEFAULT_REDUCE,
                DEFAULT_COLLECTION,
                MapReduceCommand.OutputType.REPLACE,
                new BasicDBObject()
        );
        command.setOutputDB(MR_DATABASE);
        final MapReduceOutput output = collection.mapReduce(command);

        final DB db = database.getMongo().getDB(MR_DATABASE);
        assertTrue(db.collectionExists(DEFAULT_COLLECTION));
        assertEquals(toList(output.results()), toList(db.getCollection(DEFAULT_COLLECTION).find()));
    }


    @Test
    public void testMapReduceInlineWScope() {
        final MapReduceCommand command = new MapReduceCommand(
                collection,
                "function(){ for (var i=0; i<this.x.length; i++ ){ if(this.x[i] != exclude) emit( this.x[i] , 1 ); } }",
                DEFAULT_REDUCE,
                null,
                MapReduceCommand.OutputType.INLINE,
                null
        );

        final Map<String, Object> scope = new HashMap<String, Object>();
        scope.put("exclude", "a");
        command.setScope(scope);

        MapReduceOutput output = collection.mapReduce(command);

        assertThat(output.results(), not(hasItem(hasSubdocument(new BasicDBObject("_id", "a")))));
        assertThat(output.results(), hasItem(hasSubdocument(new BasicDBObject("_id", "b"))));
    }

    @Test
    public void testDropOutputCollection() {
        final String anotherCollectionName = "anotherCollection" + System.nanoTime();
        final MapReduceOutput output = collection.mapReduce(
                DEFAULT_MAP,
                DEFAULT_REDUCE,
                anotherCollectionName, null
        );

        assertTrue(database.collectionExists(anotherCollectionName));

        output.drop();

        assertFalse(database.collectionExists(anotherCollectionName));
    }


    @Test
    public void testOutputTypeMerge() {

        database.getCollection(DEFAULT_COLLECTION).insert(new BasicDBObject("z", 10));

        final MapReduceOutput output = collection.mapReduce(
                DEFAULT_MAP,
                DEFAULT_REDUCE,
                DEFAULT_COLLECTION,
                MapReduceCommand.OutputType.MERGE,
                null
        );

        List<DBObject> documents = toList(output.results());

        assertThat(documents, hasItem(hasSubdocument(new BasicDBObject("z", 10))));
        assertThat(documents, hasItem(hasSubdocument(new BasicDBObject("_id", "a").append("value", 1.0))));
    }

    @Test
    public void testOutputTypeReduce() {
        final MapReduceOutput output = collection.mapReduce(
                DEFAULT_MAP,
                DEFAULT_REDUCE,
                DEFAULT_COLLECTION,
                MapReduceCommand.OutputType.REDUCE,
                null
        );
    }

    @Test
    public void testMapReduceWithFinalize() {
        final MapReduceCommand command = new MapReduceCommand(
                collection,
                DEFAULT_MAP,
                DEFAULT_REDUCE,
                DEFAULT_COLLECTION,
                MapReduceCommand.OutputType.REPLACE,
                new BasicDBObject()
        );
        command.setFinalize("function(key,reducedValue){ return reducedValue*5; }");

        final MapReduceOutput output = collection.mapReduce(command);

        assertThat(
                output.results(),
                hasItem(hasSubdocument(new BasicDBObject("_id", "b").append("value", 10.0)))
        );

    }

    @Test
    public void testMapReduceWithQuery() {
        final MapReduceCommand command = new MapReduceCommand(
                collection,
                DEFAULT_MAP,
                DEFAULT_REDUCE,
                DEFAULT_COLLECTION,
                MapReduceCommand.OutputType.REPLACE,
                new BasicDBObject("x", "a")
        );

        final MapReduceOutput output = collection.mapReduce(command);

        final Map<String, Object> map = toMap(output.results());
        assertEquals(2, map.size());
        assertEquals(1.0, map.get("a"));
        assertEquals(1.0, map.get("b"));
    }

    @Test
    @Ignore("Not sure about the behavior of sort")
    public void testMapReduceWithSort() {
        collection.ensureIndex(new BasicDBObject("s", 1));

        final MapReduceCommand command = new MapReduceCommand(
                collection,
                DEFAULT_MAP,
                DEFAULT_REDUCE,
                DEFAULT_COLLECTION,
                MapReduceCommand.OutputType.REPLACE,
                new BasicDBObject("x", "a")
        );

        command.setSort(new BasicDBObject("s", -1));
        command.setLimit(1);

        final MapReduceOutput output = collection.mapReduce(command);

        final Map<String, Object> map = toMap(output.results());
        assertEquals(2, map.size());
        assertEquals(1.0, map.get("c"));
        assertEquals(1.0, map.get("d"));
    }

    @Test
    public void testMapReduceWithLimit() {
        final MapReduceCommand command = new MapReduceCommand(
                collection,
                DEFAULT_MAP,
                DEFAULT_REDUCE,
                DEFAULT_COLLECTION,
                MapReduceCommand.OutputType.INLINE,
                new BasicDBObject()
        );

        command.setLimit(1);

        final MapReduceOutput output = collection.mapReduce(command);

        final Map<String, Object> map = toMap(output.results());
        assertEquals(2, map.size());
        assertEquals(1.0, map.get("a"));
        assertEquals(1.0, map.get("b"));
    }

    @Test
    public void testServerUsed() {
        final MapReduceCommand command = new MapReduceCommand(
                collection,
                DEFAULT_MAP,
                DEFAULT_REDUCE,
                DEFAULT_COLLECTION,
                MapReduceCommand.OutputType.REPLACE,
                new BasicDBObject()
        );
        final MapReduceOutput output = collection.mapReduce(command);

        assertNotNull(output.getServerUsed());
    }


    @Test
    public void testWithDefaultVerboseValue() {
        final MapReduceCommand command = new MapReduceCommand(
                collection,
                DEFAULT_MAP,
                DEFAULT_REDUCE,
                DEFAULT_COLLECTION,
                MapReduceCommand.OutputType.REPLACE,
                new BasicDBObject()
        );
        final MapReduceOutput output = collection.mapReduce(command);
        assertThat(
                output.getCommandResult(),
                hasFields(new String[]{"timing"})
        );
    }

    @Test
    public void testWithVerboseFalse() {
        final MapReduceCommand command = new MapReduceCommand(
                collection,
                DEFAULT_MAP,
                DEFAULT_REDUCE,
                DEFAULT_COLLECTION,
                MapReduceCommand.OutputType.REPLACE,
                new BasicDBObject()
        );
        command.setVerbose(false);

        final MapReduceOutput output = collection.mapReduce(command);

        assertThat(
                output.getCommandResult(),
                not(hasFields(new String[]{"timing"}))
        );
    }

    @Test
    public void testMapReduceOutputLegacyConstructor() {
        final MapReduceOutput realOutput = collection.mapReduce(new MapReduceCommand(
                collection,
                DEFAULT_MAP,
                DEFAULT_REDUCE,
                DEFAULT_COLLECTION,
                MapReduceCommand.OutputType.REPLACE,
                new BasicDBObject()
        ));

        final MapReduceOutput output = new MapReduceOutput(
                collection,
                realOutput.getCommand(),
                realOutput.getCommandResult()
        );

        assertEquals(realOutput.getCommand(), output.getCommand());
        assertEquals(realOutput.getCommandResult(), output.getCommandResult());
        assertEquals(realOutput.getOutputCollection(), output.getOutputCollection());
        assertEquals(realOutput.getServerUsed(), output.getServerUsed());
    }

    private List<DBObject> toList(final Iterable<DBObject> results) {
        return results instanceof DBCursor ? ((DBCursor) results).toArray() : (List<DBObject>) results;
    }

    private Map<String, Object> toMap(final Iterable<DBObject> result) {
        final Map<String, Object> map = new HashMap<String, Object>();
        for (DBObject document : result) {
            map.put((String) document.get("_id"), document.get("value"));
        }
        return map;
    }
}
