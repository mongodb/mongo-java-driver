/**
 *      Copyright (C) 2008 10gen Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.mongodb;

import com.mongodb.util.TestCase;
import org.testng.annotations.Test;

import java.net.UnknownHostException;
import java.util.Arrays;

public class DBTest extends TestCase {

    public DBTest() {
        super();
        cleanupDB = "com_mongodb_unittest_DBTest";
        _db = cleanupMongo.getDB(cleanupDB);
    }

    @Test(groups = {"basic"})
    public void testCreateCollection() {
        _db.getCollection("foo1").drop();
        _db.getCollection("foo2").drop();
        _db.getCollection("foo3").drop();
        _db.getCollection("foo4").drop();

        BasicDBObject o1 = new BasicDBObject("capped", false);
        DBCollection c = _db.createCollection("foo1", o1);

        DBObject o2 = BasicDBObjectBuilder.start().add("capped", true)
                .add("size", 100000).add("max", 10).get();
        c = _db.createCollection("foo2", o2);
        for (int i = 0; i < 30; i++) {
            c.insert(new BasicDBObject("x", i));
        }
        assertTrue(c.find().count() <= 10);

        DBObject o3 = BasicDBObjectBuilder.start().add("capped", true)
                .add("size", 1000).add("max", 2).get();
        c = _db.createCollection("foo3", o3);
        for (int i = 0; i < 30; i++) {
            c.insert(new BasicDBObject("x", i));
        }
        assertEquals(c.find().count(), 2);

        try {
            DBObject o4 = BasicDBObjectBuilder.start().add("capped", true)
                    .add("size", -20).get();
            c = _db.createCollection("foo4", o4);
        } catch (MongoException e) {
            return;
        }
        assertEquals(0, 1);
    }

    @Test(groups = {"basic"})
    public void testForCollectionExistence() {
        _db.getCollection("foo1").drop();
        _db.getCollection("foo2").drop();
        _db.getCollection("foo3").drop();
        _db.getCollection("foo4").drop();

        assertFalse(_db.collectionExists("foo1"));

        BasicDBObject o1 = new BasicDBObject("capped", false);
        DBCollection c = _db.createCollection("foo1", o1);

        assertTrue(_db.collectionExists("foo1"), "Collection 'foo' was supposed to be created, but 'collectionExists' did not return true.");
        assertTrue(_db.collectionExists("FOO1"));
        assertTrue(_db.collectionExists("fOo1"));

        _db.getCollection("foo1").drop();

        assertFalse(_db.collectionExists("foo1"));
    }

    @Test(groups = {"basic"})
    public void testReadPreferenceObedience() {
        DBObject obj = new BasicDBObject("mapreduce", 1).append("out", "myColl");
        assertEquals(ReadPreference.primary(), _db.getCommandReadPreference(obj, ReadPreference.secondary()));

        obj = new BasicDBObject("mapreduce", 1).append("out", new BasicDBObject("replace", "myColl"));
        assertEquals(ReadPreference.primary(), _db.getCommandReadPreference(obj, ReadPreference.secondary()));

        obj = new BasicDBObject("mapreduce", 1).append("out", new BasicDBObject("inline", 1));
        assertEquals(ReadPreference.secondary(), _db.getCommandReadPreference(obj, ReadPreference.secondary()));

        obj = new BasicDBObject("mapreduce", 1).append("out", new BasicDBObject("inline", null));
        assertEquals(ReadPreference.primary(), _db.getCommandReadPreference(obj, ReadPreference.secondary()));

        obj = new BasicDBObject("getnonce", 1);
        assertEquals(ReadPreference.primaryPreferred(), _db.getCommandReadPreference(obj, ReadPreference.secondary()));

        obj = new BasicDBObject("authenticate", 1);
        assertEquals(ReadPreference.primaryPreferred(), _db.getCommandReadPreference(obj, ReadPreference.secondary()));

        obj = new BasicDBObject("count", 1);
        assertEquals(ReadPreference.secondary(), _db.getCommandReadPreference(obj, ReadPreference.secondary()));

        obj = new BasicDBObject("count", 1);
        assertEquals(ReadPreference.secondary(), _db.getCommandReadPreference(obj, ReadPreference.secondary()));

        obj = new BasicDBObject("serverStatus", 1);
        assertEquals(ReadPreference.primary(), _db.getCommandReadPreference(obj, ReadPreference.secondary()));

        obj = new BasicDBObject("count", 1);
        assertEquals(ReadPreference.primary(), _db.getCommandReadPreference(obj, null));

        obj = new BasicDBObject("collStats", 1);
        assertEquals(ReadPreference.secondaryPreferred(), _db.getCommandReadPreference(obj, ReadPreference.secondaryPreferred()));

        obj = new BasicDBObject("text", 1);
        assertEquals(ReadPreference.secondaryPreferred(), _db.getCommandReadPreference(obj, ReadPreference.secondaryPreferred()));
    }

    @Test(groups = {"basic"})
    public void testEnsureConnection() throws UnknownHostException {

        Mongo m = new MongoClient(Arrays.asList(new ServerAddress("localhost")));

        if (isStandalone(m)) {
            return;
        }
        try {
            DB db = m.getDB("com_mongodb_unittest_DBTest");
            db.requestStart();
            try {
                db.requestEnsureConnection();
            } finally {
                db.requestDone();
            }
        } finally {
            m.close();
        }
    }

    @Test(groups = {"basic"})
    public void whenRequestStartCallsAreNestedThenTheConnectionShouldBeReleaseOnLastCallToRequestEnd() throws UnknownHostException {
        Mongo m = new MongoClient(Arrays.asList(new ServerAddress("localhost")),
                MongoClientOptions.builder().connectionsPerHost(1).maxWaitTime(10).build());
        DB db = m.getDB("com_mongodb_unittest_DBTest");

        try {
            db.requestStart();
            try {
                db.command(new BasicDBObject("ping", 1));
                db.requestStart();
                try {
                    db.command(new BasicDBObject("ping", 1));
                } finally {
                    db.requestDone();
                }
            } finally {
                db.requestDone();
            }
        } finally {
            m.close();
        }
    }

    @Test(groups = {"basic"})
    public void whenRequestDoneIsCalledWithoutFirstCallingRequestStartNoExceptionIsThrown() throws UnknownHostException {
        _db.requestDone();
    }


    /*public static class Person extends DBObject {
        
        public Person(){

        }

        Person( String name ){
            _name = name;
        }

        public String getName(){
            return _name;
        }

        public void setName(String name){
            _name = name;
        }

        String _name;
    }

    public DBTest()
        throws IOException {
        _db = new Mongo( "127.0.0.1" , "dbbasetest" );        
    }
    

    @Test
    public void test1(){
        DBCollection c = _db.getCollection( "persen.test1" );
        c.drop();
        c.setObjectClass( Person.class );
        
        Person p = new Person( "eliot" );
        c.save( p );

        DBObject out = c.findOne();
        assertEquals( "eliot" , out.get( "Name" ) );
        assertTrue( out instanceof Person , "didn't come out as Person" );
    }
    */

    final DB _db;

    public static void main(String args[])
            throws Exception {
        (new DBTest()).runConsole();
    }
}
