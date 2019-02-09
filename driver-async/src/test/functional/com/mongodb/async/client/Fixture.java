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

package com.mongodb.async.client;

import com.mongodb.Block;
import com.mongodb.ClusterFixture;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoTimeoutException;
import com.mongodb.async.FutureResultCallback;
import com.mongodb.connection.AsynchronousSocketChannelStreamFactoryFactory;
import com.mongodb.connection.SslSettings;
import com.mongodb.connection.StreamFactoryFactory;
import com.mongodb.connection.TlsChannelStreamFactoryFactory;
import org.bson.Document;

import static com.mongodb.ClusterFixture.getSslSettings;
import static com.mongodb.connection.ClusterType.SHARDED;
import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Helper class for asynchronous tests.
 */
public final class Fixture {
    private static final String DEFAULT_DATABASE_NAME = "JavaDriverTest";

    private static MongoClientImpl mongoClient;


    private Fixture() {
    }

    public static synchronized MongoClient getMongoClient() {
        if (mongoClient == null) {
            mongoClient = (MongoClientImpl) MongoClients.create(getMongoClientBuilderFromConnectionString().build());
            Runtime.getRuntime().addShutdownHook(new ShutdownHook());
        }
        return mongoClient;
    }

    public static MongoClientSettings getMongoClientSettings() {
        return getMongoClientBuilderFromConnectionString().build();
    }

    public static com.mongodb.MongoClientSettings.Builder getMongoClientBuilderFromConnectionString() {
        com.mongodb.MongoClientSettings.Builder builder = com.mongodb.MongoClientSettings.builder()
                .applyConnectionString(getConnectionString());
        if (System.getProperty("java.version").startsWith("1.6.")) {
            builder.applyToSslSettings(new Block<SslSettings.Builder>() {
                @Override
                public void apply(final SslSettings.Builder builder) {
                    builder.invalidHostNameAllowed(true);
                }
            });
        }
        builder.streamFactoryFactory(getStreamFactoryFactory());
        return builder;
    }

    public static StreamFactoryFactory getStreamFactoryFactory() {
        if (getSslSettings().isEnabled()) {
            return new TlsChannelStreamFactoryFactory();
        } else {
            return AsynchronousSocketChannelStreamFactoryFactory.builder().build();
        }
    }

    public static synchronized ConnectionString getConnectionString() {
        return ClusterFixture.getConnectionString();
    }

    public static String getDefaultDatabaseName() {
        return DEFAULT_DATABASE_NAME;
    }

    public static MongoDatabase getDefaultDatabase() {
        return getMongoClient().getDatabase(getDefaultDatabaseName());
    }

    public static MongoCollection<Document> initializeCollection(final MongoNamespace namespace) {
        MongoDatabase database = getMongoClient().getDatabase(namespace.getDatabaseName());
        try {
            FutureResultCallback<Document> futureResultCallback = new FutureResultCallback<Document>();
            database.runCommand(new Document("drop", namespace.getCollectionName()), futureResultCallback);
            futureResultCallback.get(60, SECONDS);
        } catch (MongoCommandException e) {
            if (!e.getErrorMessage().startsWith("ns not found")) {
                throw e;
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
        return database.getCollection(namespace.getCollectionName());
    }

    public static boolean isSharded() {
        getMongoClient();
        return mongoClient.getCluster().getDescription().getType() == SHARDED;
    }

    public static void dropDatabase(final String name) {
        if (name == null) {
            return;
        }
        try {
            FutureResultCallback<Document> futureResultCallback = new FutureResultCallback<Document>();
            getMongoClient().getDatabase(name)
                            .runCommand(new Document("dropDatabase", 1), futureResultCallback);
            futureResultCallback.get(60, SECONDS);
        } catch (MongoCommandException e) {
            if (!e.getErrorMessage().startsWith("ns not found")) {
                throw e;
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public static void drop(final MongoNamespace namespace) {
        try {
            FutureResultCallback<Document> futureResultCallback = new FutureResultCallback<Document>();
            getMongoClient().getDatabase(namespace.getDatabaseName())
                            .runCommand(new Document("drop", namespace.getCollectionName()), futureResultCallback);
            futureResultCallback.get();
        } catch (MongoCommandException e) {
            if (!e.getErrorMessage().contains("ns not found")) {
                throw e;
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public static synchronized void waitForLastServerSessionPoolRelease() {
        if (mongoClient != null) {
            long startTime = System.currentTimeMillis();
            int sessionInUseCount = getSessionInUseCount();
            while (sessionInUseCount > 0) {
                try {
                    if (System.currentTimeMillis() > startTime + ClusterFixture.TIMEOUT * 1000) {
                        throw new MongoTimeoutException("Timed out waiting for server session pool in use count to drop to 0.  Now at: "
                                + sessionInUseCount);
                    }
                    sleep(10);
                    sessionInUseCount = getSessionInUseCount();
                } catch (InterruptedException e) {
                    throw new MongoInterruptedException("Interrupted", e);
                }
            }
        }
    }

    private static int getSessionInUseCount() {
        return mongoClient.getServerSessionPool().getInUseCount();
    }

    static class ShutdownHook extends Thread {
        @Override
        public void run() {
            try {
                dropDatabase(getDefaultDatabaseName());
            } catch (Exception e) {
                // ignore
            }
            mongoClient.close();
            mongoClient = null;
        }
    }
}
