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
    @SuppressWarnings({"unchecked"})
    public void testSecondaryReads() throws Exception {

        final Mongo mongo = new Mongo(new MongoURI("mongodb://127.0.0.1:27017,127.0.0.1:27018"));

        // Check to see if this is a replica set... if not, get out of here.
        final CommandResult result = mongo.getDB("admin").command(new BasicDBObject("replSetGetStatus", 1));

        final String errorMsg = result.getErrorMessage();

        if (errorMsg != null && errorMsg.indexOf("--replSet") != -1) {
            System.err.println("---- SecondaryReadTest: This is not a replica set - not testing secondary reads");
            return;
        }

        String primaryHostnameAndPort = null;

        // Extract the repl set members.
        final List<TestHost> testHosts = new ArrayList<TestHost>();
        for (final BasicDBObject member : (List<BasicDBObject>)result.get("members")) {
            String hostnameAndPort = member.getString("name");
            if (hostnameAndPort.indexOf(":") == -1) hostnameAndPort = hostnameAndPort + ":27017";

            final String stateStr = member.getString("stateStr");

            if (stateStr.equals("PRIMARY")) primaryHostnameAndPort = hostnameAndPort;

            testHosts.add(new TestHost(hostnameAndPort, stateStr));
        }

        if (primaryHostnameAndPort == null) throw new IllegalStateException("No primary defined");

        mongo.getDB("com_mongodb_unittest_SecondaryReadTest").dropDatabase();
        final DB db = mongo.getDB("com_mongodb_unittest_SecondaryReadTest");
        final DBCollection col = db.getCollection("testBalance");

        final ArrayList<ObjectId> insertedIds = new ArrayList<ObjectId>();

        // Insert some test data.
        for (int idx=0; idx < INSERT_COUNT; idx++) {
            final ObjectId id = ObjectId.get();
            WriteResult writeResult = col.insert(new BasicDBObject("_id", id), WriteConcern.REPLICAS_SAFE);
            writeResult.getLastError().throwOnError();
            insertedIds.add(id);
        }

        // Make sure everything is inserted.
        while (true) {
            final long count = col.count();
            if (count == INSERT_COUNT) break;
            Thread.sleep(1000);
        }

        // Get the opcounter/query data for the hosts.
        loadQueryCount(testHosts, true);

        int secondaryCount = 0;

        for (final TestHost testHost : testHosts) if (testHost.stateStr.equals("SECONDARY")) secondaryCount++;

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

        /*
        for (final TestHost testHost : testHosts) {
            System.out.println("--- host: " + testHost.hostnameAndPort + " - queries: " + testHost.queriesBefore + " - after: " + testHost.queriesAfter);
        }
        */

        // Verify the counts.
        final int expectedPerSecondary = TOTAL_COUNT / secondaryCount;

        for (final TestHost testHost : testHosts) {

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

