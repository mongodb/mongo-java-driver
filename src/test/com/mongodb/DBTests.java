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

/**
 * Tests aspect of the DB - not really driver tests
 */
public class DBTests extends TestCase {

    final Mongo _mongo;
    final DB _db;

    public DBTests() {
        _mongo = cleanupMongo;
        cleanupDB = "java_com_mongodb_unittest_DBTests";
        _db = cleanupMongo.getDB(cleanupDB);
    }

    @Test
    public void testGetCollectionNames() throws MongoException {
        String name = "testGetCollectionNames";
        DBCollection c = _db.getCollection(name);
        c.drop();
        assertFalse(_db.getCollectionNames().contains(name));
        c.save(new BasicDBObject("x", 1));
        assertTrue(_db.getCollectionNames().contains(name));

    }


    @Test
    public void testRename() throws MongoException {
        String namea = "testRenameA";
        String nameb = "testRenameB";
        DBCollection a = _db.getCollection(namea);
        DBCollection b = _db.getCollection(nameb);

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
        DBCollection a = _db.getCollection(namea);
        DBCollection b = _db.getCollection(nameb);

        a.drop();
        b.drop();

        assertEquals(0, a.find().count());
        assertEquals(0, b.find().count());

        a.save(new BasicDBObject("x", 1));
        b.save(new BasicDBObject("x", 1));
        assertEquals(1, a.find().count());
        assertEquals(1, b.find().count());

        try {
            DBCollection b2 = a.rename(nameb);
            assertTrue(false, "Rename to existing collection must fail");
        } catch (MongoException e) {
            assertEquals(e.getCode(), 10027);
        }

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
        Mongo mongo = new MongoClient(Arrays.asList(new ServerAddress("127.0.0.1"), new ServerAddress("127.0.0.1", 27018)));

        try {
            if (isStandalone(mongo)) {
                return;
            }

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
}
