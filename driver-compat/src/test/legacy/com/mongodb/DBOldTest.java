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

import category.ReplicaSet;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.UnknownHostException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mongodb.Fixture.isDiscoverableReplicaSet;

public class DBOldTest extends DatabaseTestCase {
    @Test
    public void testForCollectionExistence() {
        database.getCollection("foo1").drop();
        database.getCollection("foo2").drop();
        database.getCollection("foo3").drop();
        database.getCollection("foo4").drop();

        assertFalse(database.collectionExists("foo1"));

        BasicDBObject o1 = new BasicDBObject("capped", false);
        database.createCollection("foo1", o1);

        assertTrue("Collection 'foo' was supposed to be created, but 'collectionExists' did not return true.",
                   database.collectionExists("foo1"));
        assertTrue(database.collectionExists("FOO1"));
        assertTrue(database.collectionExists("fOo1"));

        database.getCollection("foo1").drop();

        assertFalse(database.collectionExists("foo1"));
    }

    //This protected method was not ported to the new DB.  If the functionality is still required, we need a better
    //way of testing it
    //        @Test
    //        public void testReadPreferenceObedience() {
    //            DBObject obj = new BasicDBObject("mapreduce", 1).append("out", "myColl");
    //            assertEquals(ReadPreference.primary(), database.getCommandReadPreference(obj,
    //    ReadPreference.secondary()));
    //
    //            obj = new BasicDBObject("mapreduce", 1).append("out", new BasicDBObject("replace", "myColl"));
    //            assertEquals(ReadPreference.primary(), database.getCommandReadPreference(obj,
    //    ReadPreference.secondary()));
    //
    //            obj = new BasicDBObject("mapreduce", 1).append("out", new BasicDBObject("inline", 1));
    //            assertEquals(ReadPreference.secondary(), database.getCommandReadPreference(obj,
    //    ReadPreference.secondary()));
    //
    //            obj = new BasicDBObject("mapreduce", 1).append("out", new BasicDBObject("inline", null));
    //            assertEquals(ReadPreference.primary(), database.getCommandReadPreference(obj,
    //    ReadPreference.secondary()));
    //
    //            obj = new BasicDBObject("getnonce", 1);
    //            assertEquals(ReadPreference.primaryPreferred(), database.getCommandReadPreference(obj,
    //    ReadPreference.secondary()));
    //
    //            obj = new BasicDBObject("authenticate", 1);
    //            assertEquals(ReadPreference.primaryPreferred(), database.getCommandReadPreference(obj,
    //    ReadPreference.secondary()));
    //
    //            obj = new BasicDBObject("count", 1);
    //            assertEquals(ReadPreference.secondary(), database.getCommandReadPreference(obj,
    //    ReadPreference.secondary()));
    //
    //            obj = new BasicDBObject("count", 1);
    //            assertEquals(ReadPreference.secondary(), database.getCommandReadPreference(obj,
    //    ReadPreference.secondary()));
    //
    //            obj = new BasicDBObject("serverStatus", 1);
    //            assertEquals(ReadPreference.primary(), database.getCommandReadPreference(obj,
    //    ReadPreference.secondary()));
    //
    //            obj = new BasicDBObject("count", 1);
    //            assertEquals(ReadPreference.primary(), database.getCommandReadPreference(obj, null));
    //        }

    @Test
    public void testEnsureConnection() throws UnknownHostException {
        //it doesn't fail, I'll give you that....
        database.requestStart();
        try {
            database.requestEnsureConnection();
        } finally {
            database.requestDone();
        }
    }

    @Test
    @Category(ReplicaSet.class)
    public void testRequestPinning() throws UnknownHostException {
        if (!isDiscoverableReplicaSet()) {
            return;
        }
        database.requestStart();
        try {
            DBCursor cursorBefore = collection.find().setReadPreference(ReadPreference.secondary());
            cursorBefore.hasNext();

            for (int i = 0; i < 100; i++) {
                DBCursor cursorAfter = collection.find().setReadPreference(ReadPreference.secondary()).batchSize(-1);
                cursorAfter.hasNext();
                assertEquals(cursorBefore.getServerAddress(), cursorAfter.getServerAddress());
            }

        } finally {
            database.requestDone();
        }
    }
}
