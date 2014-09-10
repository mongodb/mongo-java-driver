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

import com.mongodb.binding.AsyncClusterBinding;
import com.mongodb.binding.AsyncReadWriteBinding;
import com.mongodb.binding.AsyncSingleConnectionBinding;
import com.mongodb.binding.ClusterBinding;
import com.mongodb.binding.PinnedBinding;
import com.mongodb.binding.ReadWriteBinding;
import com.mongodb.codecs.DocumentCodec;
import com.mongodb.connection.AsynchronousSocketChannelStreamFactory;
import com.mongodb.connection.Cluster;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.DefaultClusterFactory;
import com.mongodb.connection.SSLSettings;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerSettings;
import com.mongodb.connection.ServerVersion;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SocketStreamFactory;
import com.mongodb.connection.StreamFactory;
import com.mongodb.connection.netty.NettyStreamFactory;
import com.mongodb.management.JMXConnectionPoolListener;
import com.mongodb.operation.CommandWriteOperation;
import com.mongodb.operation.DropDatabaseOperation;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.codecs.BsonDocumentCodec;
import org.mongodb.Document;

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
public final class ClusterFixture {
    public static final String DEFAULT_URI = "mongodb://localhost:27017";
    public static final String MONGODB_URI_SYSTEM_PROPERTY_NAME = "org.mongodb.test.uri";
    private static final String DEFAULT_DATABASE_NAME = "JavaDriverTest";

    private static ConnectionString connectionString;
    private static Cluster cluster;
    private static Cluster asyncCluster;

    static {
        String mongoURIProperty = System.getProperty(MONGODB_URI_SYSTEM_PROPERTY_NAME);
        String mongoURIString = mongoURIProperty == null || mongoURIProperty.isEmpty()
                                ? DEFAULT_URI : mongoURIProperty;
        connectionString = new ConnectionString(mongoURIString);
    }

    private ClusterFixture() {
        Runtime.getRuntime().addShutdownHook(new ShutdownHook());
    }

    public static String getDefaultDatabaseName() {
        return DEFAULT_DATABASE_NAME;
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
            if (cluster != null) {
                new DropDatabaseOperation(getDefaultDatabaseName()).execute(getBinding());
                cluster.close();
            }
        }
    }

    public static synchronized ConnectionString getConnectionString() {
        return connectionString;
    }

    public static ReadWriteBinding getBinding() {
        return getBinding(getCluster());
    }

    public static ReadWriteBinding getBinding(final Cluster cluster) {
        return new ClusterBinding(cluster, ReadPreference.primary(), 1, SECONDS);
    }

    public static PinnedBinding getPinnedBinding() {
        return new PinnedBinding(getCluster(), 1, SECONDS);
    }

    public static AsyncSingleConnectionBinding getAsyncSingleConnectionBinding() {
        return new AsyncSingleConnectionBinding(getAsyncCluster(), 1, SECONDS);
    }

    public static AsyncReadWriteBinding getAsyncBinding() {
        return new AsyncClusterBinding(getAsyncCluster(), ReadPreference.primary(), 1, SECONDS);
    }

    public static Cluster getCluster() {
        if (cluster == null) {
            cluster = createCluster(new SocketStreamFactory(getSocketSettings(), getSSLSettings()));
        }
        return cluster;
    }

    public static Cluster getAsyncCluster() {
        if (asyncCluster == null) {
            asyncCluster = createCluster(getAsyncStreamFactory());
        }
        return asyncCluster;
    }

    public static Cluster createCluster(final StreamFactory streamFactory) {
        return new DefaultClusterFactory().create(ClusterSettings.builder().applyConnectionString(getConnectionString()).build(),
                                                  ServerSettings.builder().build(),
                                                  ConnectionPoolSettings.builder().applyConnectionString(getConnectionString()).build(),
                                                  streamFactory,
                                                  new SocketStreamFactory(SocketSettings.builder().build(), getSSLSettings()),
                                                  getConnectionString().getCredentialList(), null, new JMXConnectionPoolListener(), null);

    }

    public static StreamFactory getAsyncStreamFactory() {
        String streamType = System.getProperty("org.mongodb.async.type", "nio2");

        if (streamType.equals("netty") || getSSLSettings().isEnabled()) {
            return new NettyStreamFactory(getSocketSettings(), getSSLSettings());
        } else if (streamType.equals("nio2")) {
            return new AsynchronousSocketChannelStreamFactory(getSocketSettings(), getSSLSettings());
        } else {
            throw new IllegalArgumentException("Unsupported stream type " + streamType);
        }
    }

    private static SocketSettings getSocketSettings() {
        return SocketSettings.builder().applyConnectionString(getConnectionString()).build();
    }

    public static SSLSettings getSSLSettings() {
        return SSLSettings.builder().applyConnectionString(getConnectionString()).build();
    }

    public static ServerAddress getPrimary() throws InterruptedException {
        List<ServerDescription> serverDescriptions = getCluster().getDescription(10, SECONDS).getPrimaries();
        while (serverDescriptions.isEmpty()) {
            sleep(100);
            serverDescriptions = getCluster().getDescription(10, SECONDS).getPrimaries();
        }
        return serverDescriptions.get(0).getAddress();
    }

    public static List<MongoCredential> getCredentialList() {
        return getConnectionString().getCredentialList();
    }

    public static boolean isDiscoverableReplicaSet() {
        return getCluster().getDescription(10, SECONDS).getType() == REPLICA_SET
               && getCluster().getDescription(10, SECONDS).getConnectionMode() == MULTIPLE;
    }

    public static boolean isSharded() {
        return getCluster().getDescription(10, SECONDS).getType() == SHARDED;
    }

    public static boolean isAuthenticated() {
        return !getConnectionString().getCredentialList().isEmpty();
    }

    public static void enableMaxTimeFailPoint() {
        org.junit.Assume.assumeFalse(isSharded());
        new CommandWriteOperation<BsonDocument>("admin",
                                                new BsonDocumentWrapper<Document>(new Document("configureFailPoint", "maxTimeAlwaysTimeOut")
                                                                                  .append("mode", "alwaysOn"),
                                                                                  new DocumentCodec()),
                                                new BsonDocumentCodec())
        .execute(getBinding());
    }

    public static void disableMaxTimeFailPoint() {
        org.junit.Assume.assumeFalse(isSharded());
        if (serverVersionAtLeast(asList(2, 5, 3)) && !isSharded()) {
            new CommandWriteOperation<BsonDocument>("admin",
                                                    new BsonDocumentWrapper<Document>(new Document("configureFailPoint",
                                                                                                   "maxTimeAlwaysTimeOut")
                                                                                      .append("mode", "off"),
                                                                                      new DocumentCodec()),
                                                    new BsonDocumentCodec())
            .execute(getBinding());
        }
    }
}
