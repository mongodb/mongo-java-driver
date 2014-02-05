/*
 * Copyright (c) 2008 - 2014 MongoDB, Inc.
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

package org.mongodb;

import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.Cluster;
import org.mongodb.connection.ClusterType;
import org.mongodb.connection.PowerOfTwoBufferPool;
import org.mongodb.connection.SSLSettings;
import org.mongodb.connection.ServerAddress;
import org.mongodb.connection.ServerDescription;
import org.mongodb.connection.ServerVersion;
import org.mongodb.session.Session;

import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.Executor;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mongodb.connection.ClusterConnectionMode.MULTIPLE;
import static org.mongodb.connection.ClusterType.REPLICA_SET;
import static org.mongodb.connection.ClusterType.SHARDED;

/**
 * Helper class for the acceptance tests.  Used primarily by DatabaseTestCase and FunctionalSpecification.  This fixture allows Test
 * super-classes to share functionality whilst minimising duplication.
 */
public final class Fixture {
    public static final String DEFAULT_URI = "mongodb://localhost:27017";
    public static final String MONGODB_URI_SYSTEM_PROPERTY_NAME = "org.mongodb.test.uri";

    private static MongoClientURI mongoClientURI;
    private static MongoClientImpl mongoClient;
    private static final BufferProvider POWER_OF_TWO_BUFFER_POOL = new PowerOfTwoBufferPool();
    private static MongoDatabase defaultDatabase;

    private Fixture() {
    }

    public static synchronized MongoClient getMongoClient() {
        if (mongoClient == null) {
            MongoClientURI mongoURI = getMongoClientURI();
            try {
                mongoClient = (MongoClientImpl) MongoClients.create(mongoURI, mongoURI.getOptions());
            } catch (UnknownHostException e) {
                throw new IllegalArgumentException("Invalid Mongo URI: " + mongoURI.getURI(), e);
            }
            Runtime.getRuntime().addShutdownHook(new ShutdownHook());
        }
        return mongoClient;
    }


    public static synchronized MongoDatabase getDefaultDatabase() {
        if (defaultDatabase == null) {
            defaultDatabase = getMongoClient().getDatabase("DriverTest-" + System.nanoTime());
        }
        return defaultDatabase;
    }

    public static boolean clusterIsType(final ClusterType clusterType) {
        return getCluster().getDescription(10, SECONDS).getType() == clusterType;
    }

    public static boolean serverVersionAtLeast(final List<Integer> versionArray) {
        return getCluster().getDescription(10, SECONDS).getAny().get(0).getVersion().compareTo(new ServerVersion(versionArray)) >= 0;
    }

    static class ShutdownHook extends Thread {
        @Override
        public void run() {
            if (mongoClient != null) {
                if (defaultDatabase != null) {
                    defaultDatabase.tools().drop();
                }
                mongoClient.close();
                mongoClient = null;
            }
        }
    }

    public static synchronized MongoClientURI getMongoClientURI() {
        if (mongoClientURI == null) {
            String mongoURIProperty = System.getProperty(MONGODB_URI_SYSTEM_PROPERTY_NAME);
            String mongoURIString = mongoURIProperty == null || mongoURIProperty.isEmpty()
                                    ? DEFAULT_URI : mongoURIProperty;
            mongoClientURI = new MongoClientURI(mongoURIString);
        }
        return mongoClientURI;
    }

    /**
     * Will initialise a collection for testing.  This method gets the collection from the given database and drops it, so any collection
     * passed into this method will be empty after initialisation.
     *
     * @param database       the MongoDatabase that contains the required collection
     * @param collectionName the name of the collection to be initialised
     * @return the MongoCollection for collectionName
     */
    public static MongoCollection<Document> initialiseCollection(final MongoDatabase database, final String collectionName) {
        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.tools().drop();
        return collection;
    }

    public static Session getSession() {
        getMongoClient();
        return mongoClient.getSession();
    }

    public static Cluster getCluster() {
        getMongoClient();
        return mongoClient.getCluster();
    }

    public static Executor getExecutor() {
        getMongoClient();
        return mongoClient.getExecutor();
    }

    public static BufferProvider getBufferProvider() {
        return POWER_OF_TWO_BUFFER_POOL;
    }

    public static SSLSettings getSSLSettings() {
        return getOptions().getSslSettings();
    }

    public static MongoClientOptions getOptions() {
        return getMongoClientURI().getOptions();
    }

    public static ServerAddress getPrimary() throws InterruptedException {
        getMongoClient();
        List<ServerDescription> serverDescriptions = mongoClient.getCluster().getDescription(10, SECONDS).getPrimaries();
        while (serverDescriptions.isEmpty()) {
            Thread.sleep(100);
            serverDescriptions = mongoClient.getCluster().getDescription(10, SECONDS).getPrimaries();
        }
        return serverDescriptions.get(0).getAddress();
    }

    public static List<MongoCredential> getCredentialList() {
        return getMongoClientURI().getCredentialList();
    }

    public static boolean isDiscoverableReplicaSet() {
        getMongoClient();
        return mongoClient.getCluster().getDescription(10, SECONDS).getType() == REPLICA_SET
               && mongoClient.getCluster().getDescription(10, SECONDS).getConnectionMode() == MULTIPLE;
    }

    public static boolean isSharded() {
        getMongoClient();
        return mongoClient.getCluster().getDescription(10, SECONDS).getType() == SHARDED;
    }

    public static boolean isAuthenticated() {
        return !getMongoClientURI().getCredentialList().isEmpty();
    }

    public static void enableMaxTimeFailPoint() {
        org.junit.Assume.assumeFalse(isSharded());
        getMongoClient().getDatabase("admin").executeCommand(new Document("configureFailPoint", "maxTimeAlwaysTimeOut")
                                                             .append("mode", "alwaysOn"),
                                                             ReadPreference.primary());
    }

    public static void disableMaxTimeFailPoint() {
        org.junit.Assume.assumeFalse(isSharded());
        if (serverVersionAtLeast(asList(2, 5, 3)) && !isSharded()) {
            getMongoClient().getDatabase("admin").executeCommand(new Document("configureFailPoint", "maxTimeAlwaysTimeOut")
                                                                 .append("mode", "off"),
                                                                 ReadPreference.primary());
        }
    }
}
