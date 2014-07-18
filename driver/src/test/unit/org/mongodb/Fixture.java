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

package org.mongodb;

import com.mongodb.MongoCredential;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.binding.AsyncClusterBinding;
import com.mongodb.binding.AsyncReadWriteBinding;
import com.mongodb.binding.AsyncSingleConnectionBinding;
import com.mongodb.binding.ClusterBinding;
import com.mongodb.binding.PinnedBinding;
import com.mongodb.binding.ReadWriteBinding;
import com.mongodb.connection.AsynchronousSocketChannelStreamFactory;
import com.mongodb.connection.Cluster;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.DefaultClusterFactory;
import com.mongodb.connection.SSLSettings;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerVersion;
import com.mongodb.connection.SocketStreamFactory;
import com.mongodb.connection.StreamFactory;
import com.mongodb.connection.netty.NettyStreamFactory;
import com.mongodb.management.JMXConnectionPoolListener;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE;
import static com.mongodb.connection.ClusterType.REPLICA_SET;
import static com.mongodb.connection.ClusterType.SHARDED;
import static java.lang.Thread.sleep;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Helper class for the acceptance tests.  Used primarily by DatabaseTestCase and FunctionalSpecification.  This fixture allows Test
 * super-classes to share functionality whilst minimising duplication.
 */
public final class Fixture {
    public static final String DEFAULT_URI = "mongodb://localhost:27017";
    public static final String MONGODB_URI_SYSTEM_PROPERTY_NAME = "org.mongodb.test.uri";

    private static MongoClientURI mongoClientURI;
    private static MongoClientImpl mongoClient;
    private static MongoDatabase defaultDatabase;
    private static Cluster asyncCluster;

    private Fixture() {
    }

    public static synchronized MongoClient getMongoClient() {
        if (mongoClient == null) {
            MongoClientURI mongoURI = getMongoClientURI();
            mongoClient = (MongoClientImpl) MongoClients.create(mongoURI, mongoURI.getOptions());
            Runtime.getRuntime().addShutdownHook(new ShutdownHook());
        }
        return mongoClient;
    }


    public static synchronized MongoDatabase getDefaultDatabase() {
        if (defaultDatabase == null) {
            defaultDatabase = getMongoClient().getDatabase("DriverTest-" + 1);
        }
        return defaultDatabase;
    }

    public static boolean clusterIsType(final ClusterType clusterType) {
        return getCluster().getDescription(10, SECONDS).getType() == clusterType;
    }

    public static boolean serverVersionAtLeast(final List<Integer> versionArray) {
        ClusterDescription clusterDescription = getCluster().getDescription(10, SECONDS);
        int retries = 0;
        while (clusterDescription.getAny().isEmpty() && retries <= 3) {
            try {
                Thread.sleep(1000);
                retries++;
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted", e);
            }
            clusterDescription = getCluster().getDescription(10, SECONDS);
        }
        if (clusterDescription.getAny().isEmpty()) {
            throw new RuntimeException("There are no servers available in " + clusterDescription);
        }
        return clusterDescription.getAny().get(0).getVersion().compareTo(new ServerVersion(versionArray)) >= 0;
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

    public static ReadWriteBinding getBinding() {
        getMongoClient();
        return new ClusterBinding(getCluster(), ReadPreference.primary(), 1, SECONDS);
    }

    public static PinnedBinding getPinnedBinding() {
        getMongoClient();
        return new PinnedBinding(getCluster(), 1, SECONDS);
    }

    public static AsyncSingleConnectionBinding getAsyncSingleConnectionBinding() {
        getMongoClient();
        return new AsyncSingleConnectionBinding(getAsyncCluster(), 1, SECONDS);
    }

    public static AsyncReadWriteBinding getAsyncBinding() {
        getMongoClient();
        return new AsyncClusterBinding(getAsyncCluster(), ReadPreference.primary(), 1, SECONDS);
    }

    public static Cluster getCluster() {
        getMongoClient();
        return mongoClient.getCluster();
    }

    public static StreamFactory getAsyncStreamFactory() {
        return getAsyncStreamFactory(getMongoClientURI().getOptions());
    }

    public static Cluster getAsyncCluster() {
        if (asyncCluster == null) {
                asyncCluster = createCluster(getMongoClientURI());
        }
        return asyncCluster;
    }

    public static Cluster createCluster(final MongoClientURI mongoURI) {
        if (mongoURI.getHosts().size() == 1) {
            return createCluster(ClusterSettings.builder()
                                                .mode(ClusterConnectionMode.SINGLE)
                                                .hosts(Arrays.asList(new ServerAddress(mongoURI.getHosts()
                                                                                               .get(0))))
                                                .requiredReplicaSetName(mongoURI.getOptions().getRequiredReplicaSetName())
                                                .build(),
                                 mongoURI.getCredentialList(), mongoURI.getOptions(), getAsyncStreamFactory(mongoURI.getOptions())
                                );
        } else {
            List<ServerAddress> seedList = new ArrayList<ServerAddress>();
            for (final String cur : mongoURI.getHosts()) {
                seedList.add(new ServerAddress(cur));
            }
            return createCluster(ClusterSettings.builder()
                                                .hosts(seedList)
                                                .requiredReplicaSetName(mongoURI.getOptions().getRequiredReplicaSetName())
                                                .build(),
                                 mongoURI.getCredentialList(), mongoURI.getOptions(), getAsyncStreamFactory(mongoURI.getOptions()));
        }
    }

    private static Cluster createCluster(final ClusterSettings clusterSettings, final List<MongoCredential> credentialList,
                                         final MongoClientOptions options, final StreamFactory streamFactory) {
        StreamFactory heartbeatStreamFactory = getHeartbeatStreamFactory(options);
        return new DefaultClusterFactory().create(clusterSettings, options.getServerSettings(),
                                                  options.getConnectionPoolSettings(), streamFactory,
                                                  heartbeatStreamFactory,
                                                  credentialList, null, new JMXConnectionPoolListener(), null);
    }

    private static StreamFactory getHeartbeatStreamFactory(final MongoClientOptions options) {
        return new SocketStreamFactory(options.getHeartbeatSocketSettings(), options.getSslSettings());
    }

    private static StreamFactory getAsyncStreamFactory(final MongoClientOptions options) {
        String streamType = System.getProperty("org.mongodb.async.type", "nio2");

        if (streamType.equals("netty") || options.getSslSettings().isEnabled()) {
            return new NettyStreamFactory(options.getSocketSettings(), options.getSslSettings());
        } else if (streamType.equals("nio2")) {
            return new AsynchronousSocketChannelStreamFactory(options.getSocketSettings(), options.getSslSettings());
        } else {
            throw new IllegalArgumentException("Unsupported stream type " + streamType);
        }
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
            sleep(100);
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
                                                             ReadPreference.primary()
                                                            );
    }

    public static void disableMaxTimeFailPoint() {
        org.junit.Assume.assumeFalse(isSharded());
        if (serverVersionAtLeast(asList(2, 5, 3)) && !isSharded()) {
            getMongoClient().getDatabase("admin").executeCommand(new Document("configureFailPoint", "maxTimeAlwaysTimeOut")
                                                                 .append("mode", "off"),
                                                                 ReadPreference.primary()
                                                                );
        }
    }
}
