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

package com.mongodb.internal.connection;

import com.mongodb.ConnectionString;
import com.mongodb.MongoSocketReadException;
import com.mongodb.MongoSocketReadTimeoutException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;
import com.mongodb.event.ClusterListener;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.connection.SdamServerDescriptionManager.SdamIssue;
import com.mongodb.internal.time.Timeout;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import util.JsonPoweredTestHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT;
import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS;
import static com.mongodb.connection.ServerConnectionState.CONNECTING;
import static com.mongodb.internal.connection.DescriptionHelper.createServerDescription;
import static com.mongodb.internal.connection.ProtocolHelper.getCommandFailureException;
import static org.junit.Assert.assertEquals;

public class AbstractServerDiscoveryAndMonitoringTest {
    private final BsonDocument definition;
    private DefaultTestClusterableServerFactory factory;
    private Cluster cluster;

    public AbstractServerDiscoveryAndMonitoringTest(final BsonDocument definition) {
        this.definition = definition;
    }

    public static Collection<Object[]> data(final String resourcePath) {
        List<Object[]> data = new ArrayList<>();
        for (BsonDocument testDocument : JsonPoweredTestHelper.getTestDocuments(resourcePath)) {
            data.add(new Object[]{testDocument.getString("fileName").getValue()
                    + ": " + testDocument.getString("description").getValue(), testDocument});
        }
        return data;
    }

    protected void applyResponse(final BsonArray response) {
        ServerAddress serverAddress = new ServerAddress(response.get(0).asString().getValue());
        BsonDocument helloResult = response.get(1).asDocument();
        ServerDescription serverDescription;

        if (helloResult.isEmpty()) {
            serverDescription = ServerDescription.builder().type(ServerType.UNKNOWN).state(CONNECTING).address(serverAddress).build();
        } else {
            serverDescription = createServerDescription(serverAddress, helloResult, 5000000, 0);
        }
        factory.sendNotification(serverAddress, serverDescription);
    }

    protected void applyApplicationError(final BsonDocument applicationError) {
        Timeout serverSelectionTimeout = OPERATION_CONTEXT.getTimeoutContext().computeServerSelectionTimeout();
        ServerAddress serverAddress = new ServerAddress(applicationError.getString("address").getValue());
        TimeoutContext timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS);
        int errorGeneration = applicationError.getNumber("generation",
                new BsonInt32(((DefaultServer) getCluster().getServersSnapshot(serverSelectionTimeout, timeoutContext).getServer(serverAddress))
                        .getConnectionPool().getGeneration())).intValue();
        int maxWireVersion = applicationError.getNumber("maxWireVersion").intValue();
        String when = applicationError.getString("when").getValue();
        String type = applicationError.getString("type").getValue();

        DefaultServer server = (DefaultServer) cluster.getServersSnapshot(serverSelectionTimeout, timeoutContext).getServer(serverAddress);
        RuntimeException exception;

        switch (type) {
            case "command":
                exception = getCommandFailureException(applicationError.getDocument("response"), serverAddress,
                        OPERATION_CONTEXT.getTimeoutContext());
                break;
            case "network":
                exception = new MongoSocketReadException("Read error", serverAddress, new IOException());
                break;
            case "timeout":
                exception = new MongoSocketReadTimeoutException("Read timeout error", serverAddress, new IOException());
                break;
            default:
                throw new UnsupportedOperationException("Unsupported application error type: " + type);
        }

        switch (when) {
            case "beforeHandshakeCompletes":
                server.sdamServerDescriptionManager().handleExceptionBeforeHandshake(
                        SdamIssue.specific(exception, new SdamIssue.Context(server.serverId(), errorGeneration, maxWireVersion)));
                break;
            case "afterHandshakeCompletes":
                server.sdamServerDescriptionManager().handleExceptionAfterHandshake(
                        SdamIssue.specific(exception, new SdamIssue.Context(server.serverId(), errorGeneration, maxWireVersion)));
                break;
            default:
                throw new UnsupportedOperationException("Unsupported `when` value: " + when);
        }
    }

    protected ClusterType getClusterType(final String topologyType) {
        return getClusterType(topologyType, Collections.emptyList());
    }

    protected ClusterType getClusterType(final String topologyType, final Collection<ServerDescription> serverDescriptions) {
        if (topologyType.equalsIgnoreCase("Single")) {
            assertEquals(1, serverDescriptions.size());
            return serverDescriptions.iterator().next().getClusterType();
        } else if (topologyType.equalsIgnoreCase("Sharded")) {
            return ClusterType.SHARDED;
        } else if (topologyType.equalsIgnoreCase("LoadBalanced")) {
            return ClusterType.LOAD_BALANCED;
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
        } else if (serverTypeString.equals("LoadBalancer")) {
            serverType = ServerType.LOAD_BALANCER;
        } else if (serverTypeString.equals("Unknown")) {
            serverType = ServerType.UNKNOWN;
        } else {
            throw new UnsupportedOperationException("No handler for server type " + serverTypeString);
        }
        return serverType;
    }

    protected void init(final ServerListenerFactory serverListenerFactory, final ClusterListener clusterListener) {
        ConnectionString connectionString = new ConnectionString(definition.getString("uri").getValue());
        ClusterSettings settings = ClusterSettings.builder()
                                           .applyConnectionString(connectionString)
                                           .serverSelectionTimeout(1, TimeUnit.SECONDS)
                                           .build();

        ClusterId clusterId = new ClusterId();

        factory = new DefaultTestClusterableServerFactory(settings.getMode(), serverListenerFactory);

        ClusterSettings clusterSettings = settings.getClusterListeners().contains(clusterListener) ? settings
                : ClusterSettings.builder(settings).addClusterListener(clusterListener).build();

        if (settings.getMode() == ClusterConnectionMode.SINGLE) {
            cluster = new SingleServerCluster(clusterId, clusterSettings, factory);
        } else if (settings.getMode() == ClusterConnectionMode.MULTIPLE) {
            cluster = new MultiServerCluster(clusterId, clusterSettings, factory);
        } else {
            cluster = new LoadBalancedCluster(clusterId, clusterSettings, factory, null);
        }
    }

    protected BsonDocument getDefinition() {
        return definition;
    }

    protected boolean isSingleServerClusterExpected() {
        ConnectionString connectionString = new ConnectionString(definition.getString("uri").getValue());
        Boolean directConnection = connectionString.isDirectConnection();
        return (directConnection != null && directConnection)
                || (directConnection == null && connectionString.getHosts().size() == 1
                && connectionString.getRequiredReplicaSetName() == null);
    }

    protected Cluster getCluster() {
        return cluster;
    }
}
