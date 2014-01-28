/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;

/**
 * Tests aspect of the DB - not really driver tests
 */
public class DBTests extends TestCase {
    @Test
    public void testGetCollectionNames() throws MongoException {
        String name = "testGetCollectionNames";
        DBCollection c = getDatabase().getCollection(name);
        c.drop();
        assertFalse(getDatabase().getCollectionNames().contains(name));
        c.save(new BasicDBObject("x", 1));
        assertTrue(getDatabase().getCollectionNames().contains(name));

    }


    @Test
    public void testRename() throws MongoException {
        String namea = "testRenameA";
        String nameb = "testRenameB";
        DBCollection a = getDatabase().getCollection(namea);
        DBCollection b = getDatabase().getCollection(nameb);

        a.drop();
        b.drop();

        assertEquals(0, a.find().count());
        assertEquals(0, b.find().count());

        a.save(new BasicDBObject("x", 1));
        assertEquals(1, a.find().count());
        assertEquals(0, b.find().count());

        DBCollection b2 = a.rename(nameb);
        assertEquals(0, a.find().count());
        assertEquals(1, b.find().count());
        assertEquals(1, b2.find().count());

        assertEquals(b.getName(), b2.getName());

    }

    @Test
    public void testRenameAndDrop() throws MongoException {
        String namea = "testRenameA";
        String nameb = "testRenameB";
        DBCollection a = getDatabase().getCollection(namea);
        DBCollection b = getDatabase().getCollection(nameb);

        a.drop();
        b.drop();

        assertEquals(0, a.find().count());
        assertEquals(0, b.find().count());

        a.save(new BasicDBObject("x", 1));
        b.save(new BasicDBObject("x", 1));
        assertEquals(1, a.find().count());
        assertEquals(1, b.find().count());

        DBCollection b2 = a.rename(nameb, true);
        assertEquals(0, a.find().count());
        assertEquals(1, b.find().count());
        assertEquals(1, b2.find().count());

        assertEquals(b.getName(), b2.getName());
    }

//    @Test
//    public void testCommandToSecondary() throws MongoException, UnknownHostException {
//        Mongo mongo = new Mongo(Arrays.asList(new ServerAddress("127.0.0.1"), new ServerAddress("127.0.0.1", 27018)));
//
//        try {
//            if (isStandalone(mongo)) {
//                return;
//            }
//
//            String primary = getPrimaryAsString(mongo);
//
//            DB db = mongo.getDB("secondaryTest");
//            db.setReadPreference(ReadPreference.SECONDARY);
//            CommandResult result = db.command("ping");
//            assertNotEquals(primary, result.get("serverUsed"));
//        } finally {
//            mongo.close();
//        }
//    }

    @Test
    public void testGetCollectionNamesToSecondary() throws MongoException, UnknownHostException {
        if (!isReplicaSet(cleanupMongo)) {
            return;
        }

        Mongo mongo = new MongoClient(Arrays.asList(new ServerAddress("127.0.0.1"), new ServerAddress("127.0.0.1", 27018)));

        try {
            String secondary = getASecondaryAsString(mongo);
            mongo.close();
            mongo = new MongoClient(secondary);
            DB db = mongo.getDB("secondaryTest");
            db.setReadPreference(ReadPreference.secondary());
            db.getCollectionNames();
        } finally {
            mongo.close();
        }
    }



    @Test
    @SuppressWarnings("deprecation")
    public void testTurnOffSlaveOk() throws MongoException, UnknownHostException {
        MongoOptions mongoOptions = new MongoOptions();

        mongoOptions.slaveOk = true;

        Mongo mongo = new Mongo("localhost", mongoOptions);
        try {
        mongo.addOption(Bytes.QUERYOPTION_PARTIAL);
        mongo.addOption(Bytes.QUERYOPTION_AWAITDATA);

        int isSlaveOk = mongo.getOptions() & Bytes.QUERYOPTION_SLAVEOK;

        assertEquals(Bytes.QUERYOPTION_SLAVEOK, isSlaveOk);

        mongo.setOptions(mongo.getOptions() & (~Bytes.QUERYOPTION_SLAVEOK));

        assertEquals(Bytes.QUERYOPTION_AWAITDATA | Bytes.QUERYOPTION_PARTIAL, mongo.getOptions());
        } finally {
            mongo.close();
        }
    }

    @Test
    public void shouldTimeOutCommand() {
        assumeFalse(isSharded(getMongoClient()));
        checkServerVersion(2.5);
        enableMaxTimeFailPoint();
        try {
            CommandResult res = getDatabase().command(new BasicDBObject("isMaster", 1).append("maxTimeMS", 1));
            res.throwOnError();
            fail("Show have thrown");
        } catch (MongoExecutionTimeoutException e) {
            assertEquals(50, e.getCode());
        } finally {
            disableMaxTimeFailPoint();
        }
    }
}
