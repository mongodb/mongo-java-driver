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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests aspect of the DB - not really driver tests
 */
public class DBGeneralOldTests {

    private static DB database;

    @BeforeClass
    public static void setupTestSuite() throws UnknownHostException {
        final MongoClient mongoClient = new MongoClient("127.0.0.1");
        database = mongoClient.getDB(DBCollectionOldTest.class.getSimpleName());
    }

    @AfterClass
    public static void teardownTestSuite() {
        database.dropDatabase();
    }

    @Test
    public void testGetCollectionNames() {
        String name = "testGetCollectionNames";
        DBCollection c = database.getCollection(name);
        c.drop();
        assertFalse(database.getCollectionNames().contains(name));
        c.save(new BasicDBObject("x", 1));
        assertTrue(database.getCollectionNames().contains(name));

    }


    @Test
    @Ignore("Not supported yet, API not ported yet")
    public void testRename() {
        String namea = "testRenameA";
        String nameb = "testRenameB";
        DBCollection a = database.getCollection(namea);
        DBCollection b = database.getCollection(nameb);

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
    @Ignore("Not supported yet, API not ported yet")
    public void testRenameAndDrop() {
        String namea = "testRenameA";
        String nameb = "testRenameB";
        DBCollection a = database.getCollection(namea);
        DBCollection b = database.getCollection(nameb);

        a.drop();
        b.drop();

        assertEquals(0, a.find().count());
        assertEquals(0, b.find().count());

        a.save(new BasicDBObject("x", 1));
        b.save(new BasicDBObject("x", 1));
        assertEquals(1, a.find().count());
        assertEquals(1, b.find().count());

        try {
            a.rename(nameb);
            fail("Rename to existing collection must fail");
        } catch (MongoException e) {
            assertEquals(e.getCode(), 10027);
        }

        DBCollection b2 = a.rename(nameb, true);
        assertEquals(0, a.find().count());
        assertEquals(1, b.find().count());
        assertEquals(1, b2.find().count());

        assertEquals(b.getName(), b2.getName());

    }

    @Test
    @Ignore("Not supported yet, API not ported yet")
    public void testGetCollectionNamesToSecondary() throws UnknownHostException {
        Mongo mongo = new Mongo(Arrays.asList(new ServerAddress("127.0.0.1"),
                                             new ServerAddress("127.0.0.1", 27018)));

        try {
            if (isStandalone(mongo)) {
                return;
            }

            String secondary = getASecondaryAsString(mongo);
            mongo.close();
            mongo = new Mongo(secondary);
            DB db = mongo.getDB("secondaryTest");
            db.setReadPreference(ReadPreference.secondary());
            db.getCollectionNames();
        } finally {
            mongo.close();
        }
    }

    //    @Test
    //    @SuppressWarnings("deprecation")
    //    public void testTurnOffSlaveOk() throws MongoException, UnknownHostException {
    //        MongoOptions mongoOptions = new MongoOptions();
    //
    //        mongoOptions.slaveOk = true;
    //
    //        Mongo mongo = new Mongo("localhost", mongoOptions);
    //        try {
    //            mongo.addOption(Bytes.QUERYOPTION_PARTIAL);
    //            mongo.addOption(Bytes.QUERYOPTION_AWAITDATA);
    //
    //            int isSlaveOk = mongo.getOptions() & Bytes.QUERYOPTION_SLAVEOK;
    //
    //            assertEquals(Bytes.QUERYOPTION_SLAVEOK, isSlaveOk);
    //
    //            mongo.setOptions(mongo.getOptions() & (~Bytes.QUERYOPTION_SLAVEOK));
    //
    //            assertEquals(Bytes.QUERYOPTION_AWAITDATA | Bytes.QUERYOPTION_PARTIAL, mongo.getOptions());
    //        } finally {
    //            mongo.close();
    //        }
    //    }

    protected boolean isStandalone(Mongo mongo) {
        return runReplicaSetStatusCommand(mongo) == null;
    }

    protected CommandResult runReplicaSetStatusCommand(final Mongo pMongo) {
        // Check to see if this is a replica set... if not, get out of here.
        final CommandResult result = pMongo.getDB("admin").command(new BasicDBObject("replSetGetStatus", 1));

        final String errorMsg = result.getErrorMessage();

        if (errorMsg != null && errorMsg.contains("--replSet")) {
            System.err.println("---- SecondaryReadTest: This is not a replica set - not testing secondary reads");
            return null;
        }

        return result;
    }

    protected String getASecondaryAsString(Mongo mongo) {
        return getMemberNameByState(mongo, "secondary");
    }

    @SuppressWarnings({ "unchecked" })
    protected String getMemberNameByState(Mongo mongo, String stateStrToMatch) {
        CommandResult replicaSetStatus = runReplicaSetStatusCommand(mongo);

        for (final BasicDBObject member : (List<BasicDBObject>) replicaSetStatus.get("members")) {
            String hostnameAndPort = member.getString("name");
            if (!hostnameAndPort.contains(":")) {
                hostnameAndPort = hostnameAndPort + ":27017";
            }

            final String stateStr = member.getString("stateStr");

            if (stateStr.equalsIgnoreCase(stateStrToMatch)) {
                return hostnameAndPort;
            }
        }

        throw new IllegalStateException("No member found in state " + stateStrToMatch);
    }
}
