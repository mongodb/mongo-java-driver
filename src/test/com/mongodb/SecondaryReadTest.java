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

import com.mongodb.util.TestCase;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

// Java

public class SecondaryReadTest extends TestCase {


    private static final int TOTAL_COUNT = 5000;

    private static final double MAX_DEVIATION_PERCENT = 5.0;

    /**
     * Assert that the percentage of reads to each secondary does not deviate by more than 1 %
     */
    @Test(groups = {"basic"})
    public void testSecondaryReadBalance() throws Exception {

        final Mongo mongo = loadMongo();

        try {
            if (isStandalone(mongo)) {
                return;
            }

            final List<TestHost> testHosts = extractHosts(mongo);

            final DBCollection col = loadCleanDbCollection(mongo);

            // Get the opcounter/query data for the hosts.
            loadQueryCount(testHosts, true);

            final int secondaryCount = getSecondaryCount(testHosts);

            // Perform some reads on the secondaries
            col.setReadPreference(ReadPreference.secondary());

            for (int idx=0; idx < TOTAL_COUNT; idx++) {
                col.findOne();
            }

            loadQueryCount(testHosts, false);

            verifySecondaryCounts(secondaryCount, testHosts);
        } finally { if (mongo != null) mongo.close(); }
   }

    /**
     * Assert that secondary reads actually are routed to a secondary
     */
    @Test(groups = {"basic"})
    public void testSecondaryReadCursor() throws Exception {
        final Mongo mongo = loadMongo();
        try {
            if (isStandalone(mongo)) {
                return;
            }

            final List<TestHost> testHosts = extractHosts(mongo);

            final DBCollection col = loadCleanDbCollection(mongo);

            insertTestData(col, new WriteConcern(getSecondaryCount(testHosts) + 1, 10000));

            // Get the opcounter/query data for the hosts.
            loadQueryCount(testHosts, true);

            // Perform some reads on the secondaries
            col.setReadPreference(ReadPreference.secondary());

            final DBCursor cur = col.find();

            cur.hasNext();

            ServerAddress curServerAddress = cur.getServerAddress();

            assertTrue(serverIsSecondary(curServerAddress, testHosts));

        } finally { if (mongo != null) mongo.close(); }
    }

  /*
    @Test(groups = {"basic"})
    public void testSecondaryCalls() throws Exception{
    	final Mongo mongo = loadMongo();
    	
        try {
            if (isStandalone(mongo)) {
                return;
            }
            
            final List<TestHost> testHosts = extractHosts(mongo);
            final DBCollection col = loadCleanDbCollection(mongo);
            final DB db = col.getDB();
       
            insertTestData(col, new WriteConcern(getSecondaryCount(testHosts) + 1));

            //whole DB is secondary
            db.setReadPreference(ReadPreference.SECONDARY);
            
            col.count();
            confirmSecondary(db, extractHosts(mongo));
            col.findOne();
            confirmSecondary(db, extractHosts(mongo));
            col.distinct("value");
            confirmSecondary(db, extractHosts(mongo));
            
            
            //DB is primary, Collection is secondary
            db.setReadPreference(ReadPreference.PRIMARY);
            db.setReadPreference(ReadPreference.SECONDARY);
            
            col.count();
            confirmSecondary(db, extractHosts(mongo));
            col.findOne();
            confirmSecondary(db, extractHosts(mongo));
            col.distinct("value");
            confirmSecondary(db, extractHosts(mongo));
            
            
        } finally { if (mongo != null) mongo.close(); }

    }
    */
    
    private void confirmSecondary(DB db, List<TestHost> pHosts) throws Exception{
    	String server = db.getLastError().getString("serverUsed");
    	String[] ipPort = server.split("[/:]");
    	
    	ServerAddress servAddress = new ServerAddress(ipPort[0], Integer.parseInt(ipPort[2]));
    	
    	assertTrue(serverIsSecondary(servAddress, pHosts));
    	
    }
    
    private boolean serverIsSecondary(final ServerAddress pServerAddr, final List<TestHost> pHosts) {
        for (final TestHost h : pHosts) {
            if (!h.stateStr.equals("SECONDARY"))
                continue;
            final int portIdx = h.hostnameAndPort.indexOf(":");
            final int port = Integer.parseInt(h.hostnameAndPort.substring(portIdx+1, h.hostnameAndPort.length()));
            final String hostname = h.hostnameAndPort.substring(0, portIdx);

            if (pServerAddr.getPort() == port && hostname.equals(pServerAddr.getHost()))
                return true;
        }

        return false;
    }

    private Mongo loadMongo() throws Exception {
        return new MongoClient(new MongoClientURI(
                "mongodb://127.0.0.1:27017,127.0.0.1:27018,127.0.0.1:27019/?connectTimeoutMS=30000;socketTimeoutMS=30000;maxpoolsize=5;autoconnectretry=true"));
    }

    @SuppressWarnings({"unchecked"})
    private List<TestHost> extractHosts(Mongo mongo) {
        CommandResult result = runReplicaSetStatusCommand(mongo);

        List<TestHost> pHosts = new ArrayList<TestHost>();

        // Extract the repl set members.
        for (final BasicDBObject member : (List<BasicDBObject>) result.get("members")) {
            String hostnameAndPort = member.getString("name");
            if (!hostnameAndPort.contains(":")) {
                hostnameAndPort = hostnameAndPort + ":27017";
            }

            final String stateStr = member.getString("stateStr");

            pHosts.add(new TestHost(hostnameAndPort, stateStr));
        }

        return pHosts;
    }

    private DBCollection loadCleanDbCollection(final Mongo pMongo) {
        getDatabase(pMongo).dropDatabase();
        final DB db = getDatabase(pMongo);;
        return db.getCollection("testBalance");
    }
    
    private DB getDatabase(final Mongo pMongo) {
    	return pMongo.getDB("com_mongodb_unittest_SecondaryReadTest");
    }

    private void insertTestData(final DBCollection pCol, WriteConcern writeConcern) throws Exception {
        // Insert some test data.
        for (int idx=0; idx < 1000; idx++) {
            WriteConcern curWriteConcern = (idx < 999) ? WriteConcern.NONE : writeConcern;
            WriteResult writeResult = pCol.insert(new BasicDBObject(), curWriteConcern);
            writeResult.getLastError().throwOnError();
        }
    }

    private int getSecondaryCount(final List<TestHost> pHosts) {
        int secondaryCount = 0;
        for (final TestHost testHost : pHosts)
            if (testHost.stateStr.equals("SECONDARY"))
                secondaryCount++;
        return secondaryCount;
    }

    private void verifySecondaryCounts(final int pSecondaryCount, final List<TestHost> pHosts) {

        // Verify the counts.
        final int expectedPerSecondary = TOTAL_COUNT / pSecondaryCount;

        for (final TestHost testHost : pHosts) {

            if (!testHost.stateStr.equals("SECONDARY")) continue;

            final long queriesExecuted = testHost.getQueriesExecuted();

            final double deviation;
            if (queriesExecuted > expectedPerSecondary) {
                deviation = (double)100 - (((double)expectedPerSecondary / (double)queriesExecuted) * (double)100);
            } else {
                deviation = (double)100 - (((double)queriesExecuted / (double)expectedPerSecondary) * (double)100);
            }
            assertLess(deviation, MAX_DEVIATION_PERCENT);
        }
    }

    private static void loadQueryCount(final List<TestHost> pHosts, final boolean pBefore) throws Exception {
        for (final TestHost testHost : pHosts) {
            final Mongo mongoHost = new MongoClient(new MongoClientURI("mongodb://"+testHost.hostnameAndPort+"/?connectTimeoutMS=30000;socketTimeoutMS=30000;maxpoolsize=5;autoconnectretry=true"));
            try {
                final CommandResult serverStatusResult
                = mongoHost.getDB("com_mongodb_unittest_SecondaryReadTest").command(new BasicDBObject("serverStatus", 1));

                final BasicDBObject opcounters = (BasicDBObject)serverStatusResult.get("opcounters");

                if (pBefore) testHost.queriesBefore = opcounters.getLong("query");
                else testHost.queriesAfter = opcounters.getLong("query");

            } finally { if (mongoHost != null) mongoHost.close(); }
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

