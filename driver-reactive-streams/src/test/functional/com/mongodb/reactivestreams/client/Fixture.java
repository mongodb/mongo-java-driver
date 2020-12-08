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

package com.mongodb.reactivestreams.client;

import com.mongodb.ClusterFixture;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoTimeoutException;
import com.mongodb.connection.AsynchronousSocketChannelStreamFactoryFactory;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerVersion;
import com.mongodb.connection.StreamFactoryFactory;
import com.mongodb.connection.TlsChannelStreamFactoryFactory;
import com.mongodb.reactivestreams.client.internal.MongoClientImpl;
import org.bson.Document;
import org.bson.conversions.Bson;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.List;

import static com.mongodb.ClusterFixture.TIMEOUT_DURATION;
import static com.mongodb.ClusterFixture.getSslSettings;
import static java.lang.Thread.sleep;

/**
 * Helper class for asynchronous tests.
 */
public final class Fixture {
    private static MongoClientImpl mongoClient;
    private static ServerVersion serverVersion;
    private static ClusterType clusterType;

    private Fixture() {
    }

    public static synchronized MongoClient getMongoClient() {
        if (mongoClient == null) {
            mongoClient = (MongoClientImpl) MongoClients.create(getMongoClientSettings());
            serverVersion = getServerVersion();
            clusterType = getClusterType();
            Runtime.getRuntime().addShutdownHook(new ShutdownHook());
        }
        return mongoClient;
    }

    public static MongoClientSettings getMongoClientSettings() {
        return getMongoClientSettingsBuilder().build();
    }

    public static MongoClientSettings.Builder getMongoClientSettingsBuilder() {
        return MongoClientSettings.builder().applyConnectionString(ClusterFixture.getConnectionString());
    }

    public static String getDefaultDatabaseName() {
        return ClusterFixture.getDefaultDatabaseName();
    }

    public static MongoDatabase getDefaultDatabase() {
        return getMongoClient().getDatabase(getDefaultDatabaseName());
    }

    public static MongoCollection<Document> initializeCollection(final MongoNamespace namespace) {
        MongoDatabase database = getMongoClient().getDatabase(namespace.getDatabaseName());
        try {
            Mono.from(database.runCommand(new Document("drop", namespace.getCollectionName()))).block(TIMEOUT_DURATION);
        } catch (MongoCommandException e) {
            if (!e.getErrorMessage().contains("ns not found")) {
                throw e;
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
        return database.getCollection(namespace.getCollectionName());
    }

    public static void dropDatabase(final String name) {
        if (name == null) {
            return;
        }
        try {
            Mono.from(getMongoClient().getDatabase(name).runCommand(new Document("dropDatabase", 1))).block(TIMEOUT_DURATION);
        } catch (MongoCommandException e) {
            if (!e.getErrorMessage().contains("ns not found")) {
                throw e;
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public static void drop(final MongoNamespace namespace) {
        try {
            Mono.from(getMongoClient().getDatabase(namespace.getDatabaseName())
                              .runCommand(new Document("drop", namespace.getCollectionName()))).block(TIMEOUT_DURATION);
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
                    if (System.currentTimeMillis() > startTime + TIMEOUT_DURATION.toMillis()) {
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

    public static boolean serverVersionAtLeast(final int majorVersion, final int minorVersion) {
        getMongoClient();
        return serverVersion.compareTo(new ServerVersion(Arrays.asList(majorVersion, minorVersion, 0))) >= 0;
    }

    public static boolean isReplicaSet() {
        getMongoClient();
        return clusterType == ClusterType.REPLICA_SET;
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

    public static MongoClientSettings.Builder getMongoClientBuilderFromConnectionString() {
        MongoClientSettings.Builder builder = MongoClientSettings.builder()
                .applyConnectionString(getConnectionString());
        builder.streamFactoryFactory(getStreamFactoryFactory());
        return builder;
    }

    @SuppressWarnings("unchecked")
    private static ServerVersion getServerVersion() {
        Document response = runAdminCommand(new Document("buildInfo", 1));
        List<Integer> versionArray = (List<Integer>) response.get("versionArray");
        return new ServerVersion(versionArray.subList(0, 3));
    }

    private static ClusterType getClusterType() {
        Document response = runAdminCommand(new Document("ismaster", 1));
        if (response.containsKey("setName")) {
            return ClusterType.REPLICA_SET;
        } else if ("isdbgrid".equals(response.getString("msg"))) {
            return ClusterType.SHARDED;
        } else {
            return ClusterType.STANDALONE;
        }
    }

    private static Document runAdminCommand(final Bson command) {
        return Mono.from(getMongoClient().getDatabase("admin")
                .runCommand(command)).block(TIMEOUT_DURATION);
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
