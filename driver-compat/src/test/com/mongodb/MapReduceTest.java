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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class MapReduceTest extends DatabaseTestCase {

    private static final String MR_DATABASE = "output-" + System.nanoTime();
    public static final String MR_COLLECTION = "jmr1_out";

    @Override
    public void setUp() {
        super.setUp();
        collection.save(new BasicDBObject("x", new String[]{"a", "b"}));
        collection.save(new BasicDBObject("x", new String[]{"b", "c"}));
        collection.save(new BasicDBObject("x", new String[]{"c", "d"}));
    }

    @AfterClass
    public static void teardownTestSuite() {
        Fixture.getMongoClient().dropDatabase(MR_DATABASE);
    }


    @Test
    public void testMapReduceInline() {
        final MapReduceOutput mapReduceOutput = collection.mapReduce(
                "function(){ for ( var i=0; i<this.x.length; i++ ){ emit( this.x[i] , 1 ); } }",
                "function(key,values){ var sum=0; for( var i=0; i<values.length; i++ ) sum += values[i]; return sum;}",
                null,
                MapReduceCommand.OutputType.INLINE,
                null,
                ReadPreference.primaryPreferred()
        );

        assertNotNull(mapReduceOutput.results());
        assertThat(mapReduceOutput.results(), everyItem(
                allOf(isA(DBObject.class), hasFields(new String[]{"_id", "value"}))
        ));
    }

    @Test
    public void testMapReduce() {
        final MapReduceOutput mapReduceOutput = collection.mapReduce(
                "function(){ for ( var i=0; i<this.x.length; i++ ){ emit( this.x[i] , 1 ); } }",
                "function(key,values){ var sum=0; for( var i=0; i<values.length; i++ ) sum += values[i]; return sum;}",
                MR_COLLECTION, null
        );

        assertNotNull(mapReduceOutput.results());

        final Map<String, Integer> map = new HashMap<String, Integer>();
        for (DBObject r : mapReduceOutput.results()) {
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
        final MapReduceCommand mapReduceCommand = new MapReduceCommand(
                collection,
                "function(){ for ( var i=0; i<this.x.length; i++ ){ emit( this.x[i] , 1 ); } }",
                "function(key,values){ var sum=0; for( var i=0; i<values.length; i++ ) sum += values[i]; return sum;}",
                MR_COLLECTION,
                MapReduceCommand.OutputType.REPLACE,
                new BasicDBObject()
        );
        mapReduceCommand.setOutputDB(MR_DATABASE);
        final MapReduceOutput mapReduceOutput = collection.mapReduce(mapReduceCommand);

        final DB db = database.getMongo().getDB(MR_DATABASE);
        assertTrue(db.collectionExists(MR_COLLECTION));
        assertEquals(toList(mapReduceOutput.results()), toList(db.getCollection(MR_COLLECTION).find()));
    }


    @Test
    public void testMapReduceInlineWScope() {
        final MapReduceCommand mapReduceCommand = new MapReduceCommand(
                collection,
                "function(){ for ( var i=0; i<this.x.length; i++ ){ if(this.x[i] != exclude) emit( this.x[i] , 1 ); } }",
                "function(key,values){ var sum=0; for( var i=0; i<values.length; i++ ) sum += values[i]; return sum;}",
                null,
                MapReduceCommand.OutputType.INLINE,
                null
        );

        final Map<String, Object> scope = new HashMap<String, Object>();
        scope.put("exclude", "a");
        mapReduceCommand.setScope(scope);

        MapReduceOutput mapReduceOutput = collection.mapReduce(mapReduceCommand);

        assertThat(mapReduceOutput.results(), not(hasItem(hasSubdocument(new BasicDBObject("_id", "a")))));
        assertThat(mapReduceOutput.results(), hasItem(hasSubdocument(new BasicDBObject("_id", "b"))));
    }

    private List<DBObject> toList(final Iterable<DBObject> results) {
        return results instanceof DBCursor ? ((DBCursor) results).toArray() : (List<DBObject>) results;
    }
}
