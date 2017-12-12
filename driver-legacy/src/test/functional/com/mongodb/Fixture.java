/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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

import com.mongodb.connection.ServerDescription;

import java.util.List;

/**
 * Helper class for the acceptance tests.  Considering replacing with MongoClientTestBase.
 */
public final class Fixture {
    public static final String DEFAULT_URI = "mongodb://localhost:27017";
    public static final String MONGODB_URI_SYSTEM_PROPERTY_NAME = "org.mongodb.test.uri";
    private static final String DEFAULT_DATABASE_NAME = "JavaDriverTest";

    private static MongoClient mongoClient;
    private static MongoClientURI mongoClientURI;
    private static DB defaultDatabase;

    private Fixture() {
    }

    public static synchronized MongoClient getMongoClient() {
        if (mongoClient == null) {
            MongoClientURI mongoURI = getMongoClientURI();
            mongoClient = new MongoClient(mongoURI);
            Runtime.getRuntime().addShutdownHook(new ShutdownHook());
        }
        return mongoClient;
    }

    @SuppressWarnings("deprecation") // This is for access to the old API, so it will use deprecated methods
    public static synchronized DB getDefaultDatabase() {
        if (defaultDatabase == null) {
            defaultDatabase = getMongoClient().getDB(getDefaultDatabaseName());
        }
        return defaultDatabase;
    }

    public static String getDefaultDatabaseName() {
        return DEFAULT_DATABASE_NAME;
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

    public static synchronized String getMongoClientURIString() {
        String mongoURIProperty = System.getProperty(MONGODB_URI_SYSTEM_PROPERTY_NAME);
        return mongoURIProperty == null || mongoURIProperty.isEmpty()
               ? DEFAULT_URI : mongoURIProperty;
    }

    public static synchronized MongoClientURI getMongoClientURI() {
        if (mongoClientURI == null) {
            mongoClientURI = getMongoClientURI(MongoClientOptions.builder());
        }
        return mongoClientURI;
    }

    public static synchronized MongoClientURI getMongoClientURI(final MongoClientOptions.Builder builder) {
        MongoClientURI mongoClientURI = null;
        String mongoURIString = getMongoClientURIString();
        if (System.getProperty("java.version").startsWith("1.6.")) {
            builder.sslInvalidHostNameAllowed(true);
        }

        mongoClientURI = new MongoClientURI(mongoURIString, builder);
        return mongoClientURI;
    }

    public static MongoClientOptions getOptions() {
        return getMongoClientURI().getOptions();
    }

    @SuppressWarnings("deprecation")
    public static ServerAddress getPrimary() throws InterruptedException {
        getMongoClient();
        List<ServerDescription> serverDescriptions = mongoClient.getCluster().getDescription().getPrimaries();
        while (serverDescriptions.isEmpty()) {
            Thread.sleep(100);
            serverDescriptions = mongoClient.getCluster().getDescription().getPrimaries();
        }
        return serverDescriptions.get(0).getAddress();
    }
}
