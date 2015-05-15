/*
 * Copyright 2014-2015 MongoDB, Inc.
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

package com.mongodb.connection;

import com.mongodb.ConnectionString;
import com.mongodb.JsonPoweredTestHelper;
import com.mongodb.ServerAddress;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.connection.ClusterType.REPLICA_SET;
import static com.mongodb.connection.ClusterType.SHARDED;
import static com.mongodb.connection.ClusterType.UNKNOWN;
import static com.mongodb.connection.DescriptionHelper.createServerDescription;
import static com.mongodb.connection.DescriptionHelper.getVersion;
import static com.mongodb.connection.ServerConnectionState.CONNECTING;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

// See https://github.com/mongodb/specifications/tree/master/source/server-discovery-and-monitoring/tests
@RunWith(Parameterized.class)
public class ServerDiscoveryAndMonitoringTest {
    private final TestClusterableServerFactory factory = new TestClusterableServerFactory();
    private final BsonDocument definition;
    private final BaseCluster cluster;

    public ServerDiscoveryAndMonitoringTest(final String description, final BsonDocument definition) {
        this.definition = definition;
        cluster = getCluster(definition.getString("uri").getValue());
    }

    @Test
    public void shouldPassAllOutcomes() {
        for (BsonValue phase : definition.getArray("phases")) {
            for (BsonValue response : phase.asDocument().getArray("responses")) {
                applyResponse(response.asArray());
            }
            BsonDocument outcome = phase.asDocument().getDocument("outcome");
            assertTopologyType(outcome.getString("topologyType").getValue());
            assertServers(outcome.getDocument("servers"));
        }
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        List<Object[]> data = new ArrayList<Object[]>();
        for (File file : JsonPoweredTestHelper.getTestFiles("/server-discovery-and-monitoring")) {
            BsonDocument testDocument = JsonPoweredTestHelper.getTestDocument(file);
            data.add(new Object[]{testDocument.getString("description").getValue(), testDocument});
        }
        return data;
    }

    private void assertServers(final BsonDocument servers) {
        if (servers.size() != cluster.getCurrentDescription().getAll().size()) {
            fail("Cluster description contains servers that are not part of the expected outcome");
        }

        for (String serverName : servers.keySet()) {
            assertServer(serverName, servers.getDocument(serverName));
        }
    }

    private void assertServer(final String serverName, final BsonDocument expectedServerDescriptionDocument) {
        ServerDescription serverDescription = getServerDescription(serverName);

        assertNotNull(serverDescription);
        assertEquals(getServerType(expectedServerDescriptionDocument.getString("type").getValue()), serverDescription.getType());

        if (expectedServerDescriptionDocument.isString("setName")) {
            assertNotNull(serverDescription.getSetName());
            assertEquals(serverDescription.getSetName(), expectedServerDescriptionDocument.getString("setName").getValue());
        }
    }

    private ServerDescription getServerDescription(final String serverName) {
        ServerDescription serverDescription  = null;
        for (ServerDescription cur: cluster.getCurrentDescription().getAll()) {
            if (cur.getAddress().equals(new ServerAddress(serverName))) {
                serverDescription = cur;
                break;
            }
        }
        return serverDescription;
    }

    private ServerType getServerType(final String serverTypeString) {
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

    private void assertTopologyType(final String topologyType) {
        if (topologyType.equals("Single")) {
            assertEquals(SingleServerCluster.class, cluster.getClass());
        } else if (topologyType.equals("ReplicaSetWithPrimary")) {
            assertEquals(MultiServerCluster.class, cluster.getClass());
            assertEquals(REPLICA_SET, cluster.getCurrentDescription().getType());
            assertEquals(1, cluster.getCurrentDescription().getPrimaries().size());
        } else if (topologyType.equals("ReplicaSetNoPrimary")) {
            assertEquals(MultiServerCluster.class, cluster.getClass());
            assertEquals(REPLICA_SET, cluster.getCurrentDescription().getType());
            assertEquals(0, cluster.getCurrentDescription().getPrimaries().size());
        } else if (topologyType.equals("Sharded")) {
            assertEquals(MultiServerCluster.class, cluster.getClass());
            assertEquals(SHARDED, cluster.getCurrentDescription().getType());
        } else if (topologyType.equals("Unknown")) {
            assertEquals(UNKNOWN, cluster.getCurrentDescription().getType());
        } else {
            throw new UnsupportedOperationException("No handler for topology type " + topologyType);
        }
    }

    private void applyResponse(final BsonArray response) {
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

    BaseCluster getCluster(final String uri) {
        ConnectionString connectionString = new ConnectionString(uri);

        ClusterSettings settings = ClusterSettings.builder()
                                                  .serverSelectionTimeout(1, TimeUnit.SECONDS)
                                                  .hosts(getHosts(connectionString))
                                                  .mode(getMode(connectionString))
                                                  .requiredReplicaSetName(connectionString.getRequiredReplicaSetName())
                                                  .build();

        if (settings.getMode() == ClusterConnectionMode.SINGLE) {
            return new SingleServerCluster(new ClusterId(), settings, factory, new NoOpClusterListener());
        } else {
            return new MultiServerCluster(new ClusterId(), settings, factory, new NoOpClusterListener());
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
}
