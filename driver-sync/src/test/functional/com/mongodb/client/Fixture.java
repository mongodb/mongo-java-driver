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

package com.mongodb.client;

import com.mongodb.Block;
import com.mongodb.ClusterFixture;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.internal.MongoClientImpl;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerSettings;
import com.mongodb.connection.SslSettings;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.internal.connection.ClusterDescriptionHelper.getPrimaries;

/**
 * Helper class for the acceptance tests.
 */
public final class Fixture {
    private static final String DEFAULT_DATABASE_NAME = "JavaDriverTest";
    private static final long MIN_HEARTBEAT_FREQUENCY_MS = 50L;

    private static MongoClient mongoClient;
    private static MongoClientSettings mongoClientSettings;
    private static MongoDatabase defaultDatabase;

    private Fixture() {
    }

    public static synchronized MongoClient getMongoClient() {
        if (mongoClient == null) {
            mongoClient = MongoClients.create(getMongoClientSettings());
            Runtime.getRuntime().addShutdownHook(new ShutdownHook());
        }
        return mongoClient;
    }

    public static synchronized MongoDatabase getDefaultDatabase() {
        if (defaultDatabase == null) {
            defaultDatabase = getMongoClient().getDatabase(getDefaultDatabaseName());
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
                        defaultDatabase.drop();
                    }
                    mongoClient.close();
                    mongoClient = null;
                }
            }
        }
    }

    private static synchronized String getConnectionStringProperty() {
        return ClusterFixture.getConnectionString().getConnectionString();
    }

    public static synchronized MongoClientSettings getMongoClientSettings() {
        if (mongoClientSettings == null) {
            MongoClientSettings.Builder builder = MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString(getConnectionStringProperty()))
                    .applyToServerSettings(new Block<ServerSettings.Builder>() {
                        @Override
                        public void apply(final ServerSettings.Builder builder) {
                            builder.minHeartbeatFrequency(MIN_HEARTBEAT_FREQUENCY_MS, TimeUnit.MILLISECONDS);
                        }
                    });
            if (System.getProperty("java.version").startsWith("1.6.")) {
                builder.applyToSslSettings(new Block<SslSettings.Builder>() {
                    @Override
                    public void apply(final SslSettings.Builder builder) {
                        builder.invalidHostNameAllowed(true);
                    }
                });
            }
            mongoClientSettings = builder.build();
        }
        return mongoClientSettings;
    }

    public static MongoClientSettings.Builder getMongoClientSettingsBuilder() {
        return MongoClientSettings.builder(getMongoClientSettings());
    }

    public static ServerAddress getPrimary() throws InterruptedException {
        getMongoClient();
        List<ServerDescription> serverDescriptions = getPrimaries(((MongoClientImpl) mongoClient).getCluster().getDescription());
        while (serverDescriptions.isEmpty()) {
            Thread.sleep(100);
            serverDescriptions = getPrimaries(((MongoClientImpl) mongoClient).getCluster().getDescription());
        }
        return serverDescriptions.get(0).getAddress();
    }
}
