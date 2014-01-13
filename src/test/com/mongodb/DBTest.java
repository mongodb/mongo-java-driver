/*
 * Copyright (c) 2008 MongoDB, Inc.
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

import com.mongodb.util.TestCase;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DBTest extends TestCase {

    @Test
    public void testCreateCollection() {
        getDatabase().getCollection("foo1").drop();
        getDatabase().getCollection("foo2").drop();
        getDatabase().getCollection("foo3").drop();
        getDatabase().getCollection("foo4").drop();

        BasicDBObject o1 = new BasicDBObject("capped", false);
        DBCollection c = getDatabase().createCollection("foo1", o1);

        DBObject o2 = BasicDBObjectBuilder.start().add("capped", true)
                .add("size", 100000).add("max", 10).get();
        c = getDatabase().createCollection("foo2", o2);
        for (int i = 0; i < 30; i++) {
            c.insert(new BasicDBObject("x", i));
        }
        assertTrue(c.find().count() <= 10);

        DBObject o3 = BasicDBObjectBuilder.start().add("capped", true)
                .add("size", 1000).add("max", 2).get();
        c = getDatabase().createCollection("foo3", o3);
        for (int i = 0; i < 30; i++) {
            c.insert(new BasicDBObject("x", i));
        }
        assertEquals(c.find().count(), 2);

        try {
            DBObject o4 = BasicDBObjectBuilder.start().add("capped", true)
                    .add("size", -20).get();
            c = getDatabase().createCollection("foo4", o4);
        } catch (MongoException e) {
            return;
        }
        assertEquals(0, 1);
    }

    @Test
    public void testForCollectionExistence() {
        getDatabase().getCollection("foo1").drop();
        getDatabase().getCollection("foo2").drop();
        getDatabase().getCollection("foo3").drop();
        getDatabase().getCollection("foo4").drop();

        assertFalse(getDatabase().collectionExists("foo1"));

        BasicDBObject o1 = new BasicDBObject("capped", false);
        DBCollection c = getDatabase().createCollection("foo1", o1);

        assertTrue(getDatabase().collectionExists("foo1"));
        assertTrue(getDatabase().collectionExists("FOO1"));
        assertTrue(getDatabase().collectionExists("fOo1"));

        getDatabase().getCollection("foo1").drop();

        assertFalse(getDatabase().collectionExists("foo1"));
    }

    @Test
    public void testReadPreferenceObedience() throws UnknownHostException {
        if (isStandalone(cleanupMongo)) {
            return;
        }

        Mongo mongo =  new MongoClient(new MongoClientURI("mongodb://localhost:27017,localhost:27018,localhost:27019"));


        DB db = mongo.getDB(getDatabase().getName());

        DBObject obj = new BasicDBObject("mapreduce", 1).append("out", "myColl");
        assertEquals(ReadPreference.primary(), db.getCommandReadPreference(obj, ReadPreference.secondary()));

        obj = new BasicDBObject("mapreduce", 1).append("out", new BasicDBObject("replace", "myColl"));
        assertEquals(ReadPreference.primary(), db.getCommandReadPreference(obj, ReadPreference.secondary()));

        obj = new BasicDBObject("mapreduce", 1).append("out", new BasicDBObject("inline", 1));
        assertEquals(ReadPreference.secondary(), db.getCommandReadPreference(obj, ReadPreference.secondary()));

        obj = new BasicDBObject("mapreduce", 1).append("out", new BasicDBObject("inline", null));
        assertEquals(ReadPreference.primary(), db.getCommandReadPreference(obj, ReadPreference.secondary()));

        obj = new BasicDBObject("getnonce", 1);
        assertEquals(ReadPreference.primaryPreferred(), db.getCommandReadPreference(obj, ReadPreference.secondary()));

        obj = new BasicDBObject("authenticate", 1);
        assertEquals(ReadPreference.primaryPreferred(), db.getCommandReadPreference(obj, ReadPreference.secondary()));

        obj = new BasicDBObject("count", 1);
        assertEquals(ReadPreference.secondary(), db.getCommandReadPreference(obj, ReadPreference.secondary()));

        obj = new BasicDBObject("count", 1);
        assertEquals(ReadPreference.secondary(), db.getCommandReadPreference(obj, ReadPreference.secondary()));

        obj = new BasicDBObject("serverStatus", 1);
        assertEquals(ReadPreference.primary(), db.getCommandReadPreference(obj, ReadPreference.secondary()));

        obj = new BasicDBObject("count", 1);
        assertEquals(ReadPreference.primary(), db.getCommandReadPreference(obj, null));

        obj = new BasicDBObject("collStats", 1);
        assertEquals(ReadPreference.secondaryPreferred(), db.getCommandReadPreference(obj, ReadPreference.secondaryPreferred()));

        obj = new BasicDBObject("text", 1);
        assertEquals(ReadPreference.secondaryPreferred(), db.getCommandReadPreference(obj, ReadPreference.secondaryPreferred()));
    }

    @Test
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

    @Test
    public void whenRequestStartCallsAreNestedThenTheConnectionShouldBeReleaseOnLastCallToRequestEnd() throws UnknownHostException {
        Mongo m = new MongoClient(Arrays.asList(new ServerAddress("localhost")),
                MongoClientOptions.builder().connectionsPerHost(1).maxWaitTime(10000).build());
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

    @Test
    public void whenRequestDoneIsCalledWithoutFirstCallingRequestStartNoExceptionIsThrown() throws UnknownHostException {
        getDatabase().requestDone();
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenDBNameContainsSpacesThenThrowException(){
        cleanupMongo.getDB("foo bar");
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenDBNameIsEmptyThenThrowException(){
        cleanupMongo.getDB("");
    }

}
