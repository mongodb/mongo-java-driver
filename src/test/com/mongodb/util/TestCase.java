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

package com.mongodb.util;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Fixture;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ReadPreference;
import org.junit.Before;
import org.junit.BeforeClass;

import java.net.UnknownHostException;
import java.util.List;

import static org.junit.Assume.assumeTrue;

public class TestCase {

    public static final String DEFAULT_URI = "mongodb://localhost:27017";
    public static final String MONGODB_URI_SYSTEM_PROPERTY_NAME = "org.mongodb.test.uri";
    private static MongoClientURI mongoClientURI;

    public TestCase(){
        cleanupMongo = staticMongoClient;
    }

    private static final String cleanupDB = "mongo-java-driver-test";
    public Mongo cleanupMongo = null;
    protected DBCollection collection;

    private static MongoClient staticMongoClient;

    public static synchronized MongoClientURI getMongoClientURI() {
        if (mongoClientURI == null) {
            String mongoURIString = getMongoClientURIString();
            mongoClientURI = new MongoClientURI(mongoURIString);
        }
        return mongoClientURI;
    }

    @BeforeClass
    public static void testCaseBeforeClass() {
        if (staticMongoClient == null) {
            try {
                staticMongoClient = new MongoClient(getMongoClientURI());
                staticMongoClient.dropDatabase(cleanupDB);
                Runtime.getRuntime().addShutdownHook(new ShutdownHook());
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Before
    public void testCaseBefore() {
        collection = getDatabase().getCollection(getClass().getName());
        collection.drop();
    }

    static class ShutdownHook extends Thread {
        @Override
        public void run() {
            if (staticMongoClient != null) {
                staticMongoClient.dropDatabase(cleanupDB);
                staticMongoClient.close();
                staticMongoClient = null;
            }
        }
    }

    protected static MongoClient getMongoClient() {
        return staticMongoClient;
    }

    protected static DB getDatabase() {
        return staticMongoClient.getDB(cleanupDB);
    }

    /**
     *
     * @param version  must be a major version, e.g. 1.8, 2,0, 2.2
     * @return true if server is at least specified version
     */
    protected boolean serverIsAtLeastVersion(double version) {
        return serverIsAtLeastVersion(version, cleanupMongo);
    }

    protected boolean serverIsAtLeastVersion(double version, Mongo mongo) {
        String serverVersion = (String) mongo.getDB("admin").command("serverStatus").get("version");
        return Double.parseDouble(serverVersion.substring(0, 3)) >= version;
    }

    protected void checkServerVersion(double version) {
        assumeTrue(serverIsAtLeastVersion(version));
    }

    /**
     *
     * @param mongo the connection
     * @return true if connected to a standalone server
     */
    protected static boolean isStandalone(Mongo mongo) {
        return !isReplicaSet(mongo) && !isSharded(mongo);
    }

    protected static boolean isReplicaSet(Mongo mongo) {
        return runIsMaster(mongo).get("setName") != null;
    }

    protected static boolean isSharded(Mongo mongo) {
        CommandResult isMasterResult = runIsMaster(mongo);
        Object msg = isMasterResult.get("msg");
        return msg != null && msg.equals("isdbgrid");
    }

    protected static String getMongoClientURIString() {
        String mongoURIProperty = System.getProperty(MONGODB_URI_SYSTEM_PROPERTY_NAME);
        return mongoURIProperty == null || mongoURIProperty.length() == 0? DEFAULT_URI : mongoURIProperty;
    }

    @SuppressWarnings({"unchecked"})
    protected String getASecondaryAsString(Mongo mongo) {
        return Fixture.getMemberNameByState(mongo, "secondary");
    }

    protected void enableMaxTimeFailPoint() {
        cleanupMongo.getDB("admin").command(new BasicDBObject("configureFailPoint", "maxTimeAlwaysTimeOut").append("mode", "alwaysOn"),
                                            0, ReadPreference.primary());
    }

    protected void disableMaxTimeFailPoint() {
        if (serverIsAtLeastVersion(2.5)) {
            cleanupMongo.getDB("admin").command(new BasicDBObject("configureFailPoint", "maxTimeAlwaysTimeOut").append("mode", "off"),
                                                0, ReadPreference.primary());
        }
    }


    @SuppressWarnings("unchecked")
    protected int getReplicaSetSize(Mongo mongo) {
        int size = 0;

        CommandResult replicaSetStatus = Fixture.runReplicaSetStatusCommand(mongo);

        for (final BasicDBObject member : (List<BasicDBObject>) replicaSetStatus.get("members")) {

            final String stateStr = member.getString("stateStr");

            if (stateStr.equals("PRIMARY") || stateStr.equals("SECONDARY"))
                size++;
        }

        return size;
    }


    protected static CommandResult runIsMaster(final Mongo pMongo) {
        // Check to see if this is a replica set... if not, get out of here.
        return pMongo.getDB("admin").command(new BasicDBObject("ismaster", 1));
    }
}
