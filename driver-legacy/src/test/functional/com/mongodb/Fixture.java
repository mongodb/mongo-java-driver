/*
 * Copyright 2008-present MongoDB, Inc.
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

import static com.mongodb.internal.connection.ClusterDescriptionHelper.getPrimaries;

/**
 * Helper class for the acceptance tests.
 */
public final class Fixture {
    private static final String DEFAULT_DATABASE_NAME = "JavaDriverTest";

    private static MongoClient mongoClient;
    private static MongoClientURI mongoClientURI;
    private static DB defaultDatabase;

    private Fixture() {
    }

    public static synchronized com.mongodb.MongoClient getMongoClient() {
        if (mongoClient == null) {
            MongoClientURI mongoURI = getMongoClientURI();
            mongoClient = new MongoClient(mongoURI);
            Runtime.getRuntime().addShutdownHook(new ShutdownHook());
        }
        return mongoClient;
    }

    public static synchronized com.mongodb.MongoClient getLegacyMongoClient() {
        if (mongoClient == null) {
            MongoClientURI mongoURI = getMongoClientURI();
            mongoClient = new MongoClient(mongoURI);
            Runtime.getRuntime().addShutdownHook(new ShutdownHook());
        }
        return mongoClient;
    }

    public static int getServerSessionPoolInUseCount() {
        return getMongoClient().getServerSessionPool().getInUseCount();
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
        return ClusterFixture.getConnectionString().getConnectionString();
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

    public static ServerAddress getPrimary() throws InterruptedException {
        getMongoClient();
        List<ServerDescription> serverDescriptions = getPrimaries(mongoClient.getCluster().getDescription());
        while (serverDescriptions.isEmpty()) {
            Thread.sleep(100);
            serverDescriptions = getPrimaries(mongoClient.getCluster().getDescription());
        }
        return serverDescriptions.get(0).getAddress();
    }
}
