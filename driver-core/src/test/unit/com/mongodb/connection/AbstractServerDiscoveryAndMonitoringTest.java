/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.mongodb.connection;

import com.mongodb.ConnectionString;
import com.mongodb.ServerAddress;
import com.mongodb.event.ClusterListener;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import util.JsonPoweredTestHelper;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.connection.DescriptionHelper.createServerDescription;
import static com.mongodb.connection.DescriptionHelper.getVersion;
import static com.mongodb.connection.ServerConnectionState.CONNECTING;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class AbstractServerDiscoveryAndMonitoringTest {
    private final BsonDocument definition;
    private DefaultTestClusterableServerFactory factory;
    private BaseCluster cluster;

    public AbstractServerDiscoveryAndMonitoringTest(final BsonDocument definition) {
        this.definition = definition;
    }

    public static Collection<Object[]> data(final String root) throws URISyntaxException, IOException {
        List<Object[]> data = new ArrayList<Object[]>();
        for (File file : JsonPoweredTestHelper.getTestFiles(root)) {
            BsonDocument testDocument = JsonPoweredTestHelper.getTestDocument(file);
            data.add(new Object[]{testDocument.getString("description").getValue(), testDocument});
        }
        return data;
    }

    protected void applyResponse(final BsonArray response) {
        ServerAddress serverAddress = new ServerAddress(response.get(0).asString().getValue());
        BsonDocument isMasterResult = response.get(1).asDocument();
        ServerDescription serverDescription;

        if (isMasterResult.isEmpty()) {
            serverDescription = ServerDescription.builder().type(ServerType.UNKNOWN).state(CONNECTING).address(serverAddress).build();
        } else {
            serverDescription = createServerDescription(serverAddress, isMasterResult,
                    getVersion(new BsonDocument("versionArray",
                            new BsonArray(asList(new BsonInt32(2),
                                    new BsonInt32(6),
                                    new BsonInt32(0))))),
                    5000000);
        }
        factory.sendNotification(serverAddress, serverDescription);
    }

    protected ClusterType getClusterType(final String topologyType) {
        return getClusterType(topologyType, Collections.<ServerDescription>emptyList());
    }

    protected ClusterType getClusterType(final String topologyType, final Collection<ServerDescription> serverDescriptions) {
        if (topologyType.equalsIgnoreCase("Single")) {
            assertEquals(1, serverDescriptions.size());
            return serverDescriptions.iterator().next().getClusterType();
        } else if (topologyType.equalsIgnoreCase("Sharded")) {
            return ClusterType.SHARDED;
        } else if (topologyType.startsWith("ReplicaSet")) {
            return ClusterType.REPLICA_SET;
        } else if (topologyType.equalsIgnoreCase("Unknown")) {
            return ClusterType.UNKNOWN;
        } else {
            throw new IllegalArgumentException("Unsupported topology type: " + topologyType);
        }
    }

    protected ServerType getServerType(final String serverTypeString) {
        ServerType serverType;
        if (serverTypeString.equals("RSPrimary")) {
            serverType = ServerType.REPLICA_SET_PRIMARY;
        } else if (serverTypeString.equals("RSSecondary")) {
            serverType = ServerType.REPLICA_SET_SECONDARY;
        } else if (serverTypeString.equals("RSArbiter")) {
            serverType = ServerType.REPLICA_SET_ARBITER;
        } else if (serverTypeString.equals("RSGhost")) {
            serverType = ServerType.REPLICA_SET_GHOST;
        } else if (serverTypeString.equals("RSOther")) {
            serverType = ServerType.REPLICA_SET_OTHER;
        } else if (serverTypeString.equals("Mongos")) {
            serverType = ServerType.SHARD_ROUTER;
        } else if (serverTypeString.equals("Standalone")) {
            serverType = ServerType.STANDALONE;
        } else if (serverTypeString.equals("PossiblePrimary")) {
            serverType = ServerType.UNKNOWN;
        } else if (serverTypeString.equals("Unknown")) {
            serverType = ServerType.UNKNOWN;
        } else {
            throw new UnsupportedOperationException("No handler for server type " + serverTypeString);
        }
        return serverType;
    }

    protected void init(final ServerListenerFactory serverListenerFactory, final ClusterListener clusterListener) {
        ConnectionString connectionString = new ConnectionString(definition.getString("uri").getValue());

        ClusterConnectionMode mode = getMode(connectionString);
        ClusterSettings settings = ClusterSettings.builder()
                                           .serverSelectionTimeout(1, TimeUnit.SECONDS)
                                           .hosts(getHosts(connectionString))
                                           .mode(mode)
                                           .requiredReplicaSetName(connectionString.getRequiredReplicaSetName())
                                           .build();

        ClusterId clusterId = new ClusterId();

        factory = new DefaultTestClusterableServerFactory(clusterId, mode, serverListenerFactory);

        ClusterSettings clusterSettings = settings.getClusterListeners().contains(clusterListener) ? settings
                : ClusterSettings.builder(settings).addClusterListener(clusterListener).build();

        if (settings.getMode() == ClusterConnectionMode.SINGLE) {
            cluster = new SingleServerCluster(clusterId, clusterSettings, factory);
        } else {
            cluster = new MultiServerCluster(clusterId, clusterSettings, factory);
        }
    }

    private List<ServerAddress> getHosts(final ConnectionString connectionString) {
        List<ServerAddress> serverAddresses = new ArrayList<ServerAddress>();
        for (String host : connectionString.getHosts()) {
            serverAddresses.add(new ServerAddress(host));
        }
        return serverAddresses;
    }

    private ClusterConnectionMode getMode(final ConnectionString connectionString) {
        if (connectionString.getHosts().size() > 1 || connectionString.getRequiredReplicaSetName() != null) {
            return ClusterConnectionMode.MULTIPLE;
        } else {
            return ClusterConnectionMode.SINGLE;
        }
    }

    protected BsonDocument getDefinition() {
        return definition;
    }

    protected DefaultTestClusterableServerFactory getFactory() {
        return factory;
    }

    protected BaseCluster getCluster() {
        return cluster;
    }
}
