/**
 * Copyright (C) 2008 10gen Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb;

// Mongo
import com.mongodb.*;
import org.bson.types.*;
import com.mongodb.util.*;

import org.testng.annotations.Test;


// Java
import java.util.*;
import java.util.concurrent.*;

public class SecondaryReadTest extends TestCase {


    private static final int INSERT_COUNT = 1000;

    private static final int ITERATION_COUNT = 100;

    private static final int TOTAL_COUNT = INSERT_COUNT * ITERATION_COUNT;

    private static final double MAX_DEVIATION_PERCENT = 1.0;

    @Test(groups = {"basic"})
    public void testSecondaryReads1() throws Exception {

        final Mongo mongo = loadMongo();

        final CommandResult result = serverStatusCmd(mongo);

        // If the result is null, this is not a replica set.
        if (result == null) return;

        final List<TestHost> testHosts = new ArrayList<TestHost>();
        final String primaryHostnameAndPort = extractHosts(result, testHosts);
        final DBCollection col = loadCleanDbCollection(mongo);

        final List<ObjectId> insertedIds = insertTestData(col);

        // Get the opcounter/query data for the hosts.
        loadQueryCount(testHosts, true);

        final int secondaryCount = getSecondaryCount(testHosts);

        // Perform some reads on the secondaries
        col.setReadPreference(ReadPreference.SECONDARY);

        for (int idx=0; idx < ITERATION_COUNT; idx++) {
            for (ObjectId id : insertedIds) {
                final BasicDBObject doc = (BasicDBObject)col.findOne(new BasicDBObject("_id", id));
                if (doc == null) throw new IllegalStateException("Doc not found");
                if (!doc.getObjectId("_id").equals(id)) throw new IllegalStateException("Ids are off");
            }
        }

        loadQueryCount(testHosts, false);

        verifySecondaryCounts(secondaryCount, testHosts);
   }

    @Test(groups = {"basic"})
    public void testSecondaryReads2() throws Exception {

        final Mongo mongo = loadMongo();

        final CommandResult result = serverStatusCmd(mongo);

        // If the result is null, this is not a replica set.
        if (result == null) return;

        final List<TestHost> testHosts = new ArrayList<TestHost>();
        final String primaryHostnameAndPort = extractHosts(result, testHosts);
        final DBCollection col = loadCleanDbCollection(mongo);

        final List<ObjectId> insertedIds = insertTestData(col);

        // Get the opcounter/query data for the hosts.
        loadQueryCount(testHosts, true);

        final int secondaryCount = getSecondaryCount(testHosts);

        // Perform some reads on the secondaries
        mongo.setReadPreference(ReadPreference.SECONDARY);

        for (int idx=0; idx < ITERATION_COUNT; idx++) {
            for (ObjectId id : insertedIds) {
                final BasicDBObject doc = (BasicDBObject)col.findOne(new BasicDBObject("_id", id));
                if (doc == null) throw new IllegalStateException("Doc not found");
                if (!doc.getObjectId("_id").equals(id)) throw new IllegalStateException("Ids are off");
            }
        }

        loadQueryCount(testHosts, false);

        verifySecondaryCounts(secondaryCount, testHosts);
   }

    @Test(groups = {"basic"})
    public void testSecondaryReads3() throws Exception {

        final Mongo mongo = loadMongo();

        final CommandResult result = serverStatusCmd(mongo);

        // If the result is null, this is not a replica set.
        if (result == null) return;

        final List<TestHost> testHosts = new ArrayList<TestHost>();
        final String primaryHostnameAndPort = extractHosts(result, testHosts);
        final DBCollection col = loadCleanDbCollection(mongo);

        final List<ObjectId> insertedIds = insertTestData(col);

        // Get the opcounter/query data for the hosts.
        loadQueryCount(testHosts, true);

        final int secondaryCount = getSecondaryCount(testHosts);

        // Perform some reads on the secondaries
        col.getDB().setReadPreference(ReadPreference.SECONDARY);

        for (int idx=0; idx < ITERATION_COUNT; idx++) {
            for (ObjectId id : insertedIds) {
                final BasicDBObject doc = (BasicDBObject)col.findOne(new BasicDBObject("_id", id));
                if (doc == null) throw new IllegalStateException("Doc not found");
                if (!doc.getObjectId("_id").equals(id)) throw new IllegalStateException("Ids are off");
            }
        }

        loadQueryCount(testHosts, false);

        verifySecondaryCounts(secondaryCount, testHosts);
   }


    private Mongo loadMongo() throws Exception {
        return new Mongo(new MongoURI("mongodb://127.0.0.1:27017,127.0.0.1:27018"));
    }

    private CommandResult serverStatusCmd(final Mongo pMongo) {
        // Check to see if this is a replica set... if not, get out of here.
        final CommandResult result = pMongo.getDB("admin").command(new BasicDBObject("replSetGetStatus", 1));

        final String errorMsg = result.getErrorMessage();

        if (errorMsg != null && errorMsg.indexOf("--replSet") != -1) {
            System.err.println("---- SecondaryReadTest: This is not a replica set - not testing secondary reads");
            return null;
        }

        return result;
    }

    @SuppressWarnings({"unchecked"})
    private String extractHosts(final CommandResult pResult, final List<TestHost> pHosts) {
        String primaryHostnameAndPort = null;
        // Extract the repl set members.

        for (final BasicDBObject member : (List<BasicDBObject>)pResult.get("members")) {
            String hostnameAndPort = member.getString("name");
            if (hostnameAndPort.indexOf(":") == -1) hostnameAndPort = hostnameAndPort + ":27017";

            final String stateStr = member.getString("stateStr");

            if (stateStr.equals("PRIMARY")) primaryHostnameAndPort = hostnameAndPort;

            pHosts.add(new TestHost(hostnameAndPort, stateStr));
        }

        if (primaryHostnameAndPort == null) throw new IllegalStateException("No primary defined");

        return primaryHostnameAndPort;
    }

    private DBCollection loadCleanDbCollection(final Mongo pMongo) {
        pMongo.getDB("com_mongodb_unittest_SecondaryReadTest").dropDatabase();
        final DB db = pMongo.getDB("com_mongodb_unittest_SecondaryReadTest");
        return db.getCollection("testBalance");
    }

    private List<ObjectId> insertTestData(final DBCollection pCol) throws Exception {
        final ArrayList<ObjectId> insertedIds = new ArrayList<ObjectId>();

        // Insert some test data.
        for (int idx=0; idx < INSERT_COUNT; idx++) {
            final ObjectId id = ObjectId.get();
            WriteResult writeResult = pCol.insert(new BasicDBObject("_id", id), WriteConcern.REPLICAS_SAFE);
            writeResult.getLastError().throwOnError();
            insertedIds.add(id);
        }

        // Make sure everything is inserted.
        while (true) {
            final long count = pCol.count();
            if (count == INSERT_COUNT) break;
            Thread.sleep(1000);
        }

        return insertedIds;
    }

    private int getSecondaryCount(final List<TestHost> pHosts) {
        int secondaryCount = 0;
        for (final TestHost testHost : pHosts) if (testHost.stateStr.equals("SECONDARY")) secondaryCount++;
        return secondaryCount;
    }

    private void verifySecondaryCounts(final int pSecondaryCount, final List<TestHost> pHosts) {

        // Verify the counts.
        final int expectedPerSecondary = TOTAL_COUNT / pSecondaryCount;

        for (final TestHost testHost : pHosts) {

            if (!testHost.stateStr.equals("SECONDARY")) continue;

            final long queriesExecuted = testHost.getQueriesExecuted();

            if (expectedPerSecondary == queriesExecuted) continue;

            if (queriesExecuted > expectedPerSecondary) {
                final double deviation = (double)100 - (((double)expectedPerSecondary / (double)queriesExecuted) * (double)100);
                //System.out.println("------ deviation: " + deviation);
                assertEquals(true, (deviation <= MAX_DEVIATION_PERCENT));
            } else {
                final double deviation = (double)100 - (((double)queriesExecuted / (double)expectedPerSecondary) * (double)100);
                //System.out.println("------ deviation: " + deviation);
                assertEquals(true, (deviation <= MAX_DEVIATION_PERCENT));
            }
        }

        /*
        for (final TestHost testHost : pHosts) {
            System.out.println("--- host: " + testHost.hostnameAndPort + " - queries: " + testHost.queriesBefore + " - after: " + testHost.queriesAfter);
        }
        */

    }

    private static void loadQueryCount(final List<TestHost> pHosts, final boolean pBefore) throws Exception {
        for (final TestHost testHost : pHosts) {
            final Mongo mongoHost = new Mongo(new MongoURI("mongodb://"+testHost.hostnameAndPort));
            try {
                final CommandResult serverStatusResult
                = mongoHost.getDB("com_mongodb_unittest_SecondaryReadTest").command(new BasicDBObject("serverStatus", 1));

                final BasicDBObject opcounters = (BasicDBObject)serverStatusResult.get("opcounters");

                if (pBefore) testHost.queriesBefore = opcounters.getLong("query");
                else testHost.queriesAfter = opcounters.getLong("query");

            } finally { mongoHost.close(); }
        }
    }

    private static class TestHost {
        private final String hostnameAndPort;
        private final String stateStr;

        private long queriesBefore;
        private long queriesAfter;

        public long getQueriesExecuted() { return queriesAfter - queriesBefore; }

        private TestHost(final String pHostnameAndPort, final String pStateStr) {
            hostnameAndPort = pHostnameAndPort;
            stateStr = pStateStr;
        }
    }
}

