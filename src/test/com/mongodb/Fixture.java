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

import java.net.UnknownHostException;
import java.util.List;

/**
 * Helper class for the acceptance tests.
 */
public final class Fixture {
    public static final String DEFAULT_URI = "mongodb://localhost:27017";
    public static final String MONGODB_URI_SYSTEM_PROPERTY_NAME = "org.mongodb.test.uri";

    private static MongoClient mongoClient;
    private static MongoClientURI mongoClientURI;
    private static DB defaultDatabase;

    private Fixture() {
    }

    public static synchronized MongoClient getMongoClient() {
        if (mongoClient == null) {
            MongoClientURI mongoURI = getMongoClientURI();
            try {
                mongoClient = new MongoClient(mongoURI);
            } catch (UnknownHostException e) {
                throw new IllegalArgumentException("Invalid Mongo URI: " + mongoURI.getURI(), e);
            }
            Runtime.getRuntime().addShutdownHook(new ShutdownHook());
        }
        return mongoClient;
    }

    public static synchronized DB getDefaultDatabase() {
        if (defaultDatabase == null) {
            defaultDatabase = getMongoClient().getDB("DriverTest-" + System.nanoTime());
        }
        return defaultDatabase;
    }

    /**
     *
     * @param version  must be a major version, e.g. 1.8, 2,0, 2.2
     * @return true if server is at least specified version
     */
    public static boolean serverIsAtLeastVersion(double version) {
        String serverVersion = (String) getMongoClient().getDB("admin").command("serverStatus").get("version");
        return Double.parseDouble(serverVersion.substring(0, 3)) >= version;
    }

    public static boolean isStandalone() {
        return !isReplicaSet() && !isSharded();
    }

    public static boolean isSharded() {
        CommandResult isMasterResult = runIsMaster();
        Object msg = isMasterResult.get("msg");
        return msg != null && msg.equals("isdbgrid");
    }

    public static boolean isReplicaSet() {
        return runIsMaster().get("setName") != null;
    }

    public static boolean isServerStartedWithJournalingDisabled() {
        return serverStartedWithBooleanOption("--nojournal", "nojournal");
    }

    private static boolean serverStartedWithBooleanOption(final String commandLineOption, final String configOption) {
        CommandResult res = getMongoClient().getDB("admin").command(new BasicDBObject("getCmdLineOpts", 1));
        res.throwOnError();
        if (res.containsField("parsed") && ((DBObject) res.get("parsed")).containsField(configOption)) {
            return (Boolean) ((DBObject) res.get("parsed")).get(configOption);
        } else {
            return ((List) res.get("argv")).contains(commandLineOption);
        }
    }

    private static CommandResult runIsMaster() {
        // Check to see if this is a replica set... if not, get out of here.
        return getMongoClient().getDB("admin").command(new BasicDBObject("ismaster", 1));
    }

    static class ShutdownHook extends Thread {
        @Override
        public void run() {
            synchronized (Fixture.class) {
                if (mongoClient != null) {
                    if (defaultDatabase != null) {
                        defaultDatabase.dropDatabase();
                    }
                    mongoClient.close();
                    mongoClient = null;
                }
            }
        }
    }

    public static synchronized MongoClientURI getMongoClientURI() {
        if (mongoClientURI == null) {
            String mongoURIProperty = System.getProperty(MONGODB_URI_SYSTEM_PROPERTY_NAME);
            String mongoURIString = mongoURIProperty == null || mongoURIProperty.length() == 0
                                    ? DEFAULT_URI : mongoURIProperty;
            mongoClientURI = new MongoClientURI(mongoURIString);
        }
        return mongoClientURI;
    }

    public static MongoClientOptions getOptions() {
        return getMongoClientURI().getOptions();
    }
}
