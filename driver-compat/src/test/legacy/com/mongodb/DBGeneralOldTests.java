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

import com.mongodb.diagnostics.Loggers;
import com.mongodb.diagnostics.logging.Logger;
import org.junit.Ignore;
import org.junit.Test;
import org.mongodb.Document;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests aspect of the DB - not really driver tests.  Should be migrated into the appropriate unit or functional tests.
 */
public class DBGeneralOldTests extends DatabaseTestCase {
    private static final Logger LOGGER = Loggers.getLogger("test");

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
    public void testRename() {
        String namea = "testRenameA";
        String nameb = "testRenameB";
        DBCollection firstCollection = database.getCollection(namea);
        DBCollection secondCollection = database.getCollection(nameb);

        firstCollection.drop();
        secondCollection.drop();

        assertEquals(0, firstCollection.find().count());
        assertEquals(0, secondCollection.find().count());

        firstCollection.save(new BasicDBObject("x", 1));
        assertEquals(1, firstCollection.find().count());
        assertEquals(0, secondCollection.find().count());

        DBCollection renamedFirstCollection = firstCollection.rename(nameb);
        assertEquals(0, firstCollection.find().count());
        assertEquals(1, secondCollection.find().count());
        assertEquals(1, renamedFirstCollection.find().count());

        assertEquals(secondCollection.getName(), renamedFirstCollection.getName());
    }

    @Test
    @Ignore("Not sure exactly what behaviour this test is asserting.  Needs re-writing")
    public void testGetCollectionNamesToSecondary() throws UnknownHostException {
        Mongo mongo = new MongoClient(Arrays.asList(new ServerAddress("127.0.0.1"),
                                                    new ServerAddress("127.0.0.1", 27018)));

        try {
            if (isStandalone(mongo)) {
                return;
            }

            String secondary = getMemberNameByState(mongo, "secondary");
            mongo.close();
            mongo = new MongoClient(secondary);
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

    protected boolean isStandalone(final Mongo mongo) {
        return runReplicaSetStatusCommand(mongo) == null;
    }

    protected CommandResult runReplicaSetStatusCommand(final Mongo pMongo) {
        // Check to see if this is a replica set... if not, get out of here.
        CommandResult result = pMongo.getDB("admin").command(new BasicDBObject("replSetGetStatus", 1));

        String errorMsg = result.getErrorMessage();

        if (errorMsg != null && errorMsg.contains("--replSet")) {
            LOGGER.warn("SecondaryReadTest: This is not a replica set - not testing secondary reads");
            return null;
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    protected String getMemberNameByState(final Mongo mongo, final String stateStrToMatch) {
        CommandResult replicaSetStatus = runReplicaSetStatusCommand(mongo);

        for (final Document member : (List<Document>) replicaSetStatus.get("members")) {
            String hostnameAndPort = (String) member.get("name");
            if (!hostnameAndPort.contains(":")) {
                hostnameAndPort = hostnameAndPort + ":27017";
            }

            String stateStr = (String) member.get("stateStr");

            if (stateStr.equalsIgnoreCase(stateStrToMatch)) {
                return hostnameAndPort;
            }
        }

        throw new IllegalStateException("No member found in state " + stateStrToMatch);
    }
}
