/*
 * Copyright 2014-2016 MongoDB, Inc.
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

import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.mongodb.ClusterType.ReplicaSet;
import static com.mongodb.ClusterType.Sharded;
import static com.mongodb.ClusterType.Unknown;
import static com.mongodb.ServerConnectionState.Connecting;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

// See https://github.com/mongodb/specifications/tree/master/source/server-discovery-and-monitoring/tests
@RunWith(Parameterized.class)
public class ServerDiscoveryAndMonitoringTest {
    private final TestClusterableServerFactory factory = new TestClusterableServerFactory();
    private final DBObject definition;
    private final BaseCluster cluster;

    public ServerDiscoveryAndMonitoringTest(final String description, final DBObject definition) throws UnknownHostException {
        this.definition = definition;
        cluster = getCluster((String) definition.get("uri"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldPassAllOutcomes() throws UnknownHostException {
        for (DBObject phase : (List<DBObject>) definition.get("phases")) {
            for (List response : (List<List>) phase.get("responses")) {
                applyResponse(response);
            }
            DBObject outcome = (DBObject) phase.get("outcome");
            assertTopologyType((String) outcome.get("topologyType"));
            assertServers((DBObject) outcome.get("servers"));
        }
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        List<Object[]> data = new ArrayList<Object[]>();
        for (File file : JsonPoweredTestHelper.getTestFiles("/server-discovery-and-monitoring")) {
            DBObject testDocument = JsonPoweredTestHelper.getTestDocument(file);
            data.add(new Object[]{testDocument.get("description"), testDocument});
        }
        return data;
    }

    private void assertServers(final DBObject servers) throws UnknownHostException {
        if (servers.keySet().size() != cluster.getCurrentDescription().getAll().size()) {
            fail("Cluster description contains servers that are not part of the expected outcome");
        }

        for (String serverName : servers.keySet()) {
            assertServer(serverName, (DBObject) servers.get(serverName));
        }
    }

    private void assertServer(final String serverName, final DBObject expectedServerDescriptionDocument) throws UnknownHostException {
        ServerDescription serverDescription = getServerDescription(serverName);

        assertNotNull(serverDescription);
        assertEquals(getServerType((String) expectedServerDescriptionDocument.get("type")), serverDescription.getType());

        if (expectedServerDescriptionDocument.get("electionId") instanceof ObjectId) {
            assertNotNull(serverDescription.getElectionId());
            assertEquals(expectedServerDescriptionDocument.get("electionId"), serverDescription.getElectionId());
        } else {
            assertNull(serverDescription.getElectionId());
        }

        if (expectedServerDescriptionDocument.get("setVersion") instanceof Number) {
            assertNotNull(serverDescription.getSetVersion());
            assertEquals(expectedServerDescriptionDocument.get("setVersion"), serverDescription.getSetVersion());
        } else {
            assertNull(serverDescription.getSetVersion());
        }

        if (expectedServerDescriptionDocument.get("setName") instanceof String) {
            assertNotNull(serverDescription.getSetName());
            assertEquals(expectedServerDescriptionDocument.get("setName"), serverDescription.getSetName());
        } else {
            assertNull(serverDescription.getSetName());
        }
    }

    private ServerDescription getServerDescription(final String serverName) throws UnknownHostException {
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
            serverType = ServerType.ReplicaSetPrimary;
        } else if (serverTypeString.equals("RSSecondary")) {
            serverType = ServerType.ReplicaSetSecondary;
        } else if (serverTypeString.equals("RSArbiter")) {
            serverType = ServerType.ReplicaSetArbiter;
        } else if (serverTypeString.equals("RSGhost")) {
            serverType = ServerType.ReplicaSetGhost;
        } else if (serverTypeString.equals("RSOther")) {
            serverType = ServerType.ReplicaSetOther;
        } else if (serverTypeString.equals("Mongos")) {
            serverType = ServerType.ShardRouter;
        } else if (serverTypeString.equals("Standalone")) {
            serverType = ServerType.StandAlone;
        } else if (serverTypeString.equals("PossiblePrimary")) {
            serverType = ServerType.Unknown;
        } else if (serverTypeString.equals("Unknown")) {
            serverType = ServerType.Unknown;
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
            assertEquals(ReplicaSet, cluster.getCurrentDescription().getType());
            assertEquals(1, cluster.getCurrentDescription().getPrimaries().size());
        } else if (topologyType.equals("ReplicaSetNoPrimary")) {
            assertEquals(MultiServerCluster.class, cluster.getClass());
            assertEquals(ReplicaSet, cluster.getCurrentDescription().getType());
            assertEquals(0, cluster.getCurrentDescription().getPrimaries().size());
        } else if (topologyType.equals("Sharded")) {
            assertEquals(MultiServerCluster.class, cluster.getClass());
            assertEquals(Sharded, cluster.getCurrentDescription().getType());
        } else if (topologyType.equals("Unknown")) {
            assertEquals(Unknown, cluster.getCurrentDescription().getType());
        } else {
            throw new UnsupportedOperationException("No handler for topology type " + topologyType);
        }
    }

    private void applyResponse(final List response) throws UnknownHostException {
        ServerAddress serverAddress = new ServerAddress((String) response.get(0));
        CommandResult result = new CommandResult(serverAddress);
        result.putAll((DBObject) response.get(1));
        ServerDescription serverDescription;

        if (((DBObject) response.get(1)).keySet().isEmpty()) {
            serverDescription = ServerDescription.builder().type(ServerType.Unknown).state(Connecting).address(serverAddress).build();
        } else {
            serverDescription = ServerMonitor.createDescription(result, new ServerVersion(2, 6), 5000000);
        }
        factory.getServer(serverAddress).sendNotification(serverDescription);
    }

    BaseCluster getCluster(final String uri) throws UnknownHostException {
        MongoClientURI connectionString = new MongoClientURI(uri);

        ClusterSettings settings = ClusterSettings.builder()
                .hosts(getHosts(connectionString))
                .mode(getMode(connectionString))
                .requiredReplicaSetName(connectionString.getOptions().getRequiredReplicaSetName())
                .build();

        if (settings.getMode() == ClusterConnectionMode.Single) {
            return new SingleServerCluster("1", settings, factory, new NoOpClusterListener());
        } else {
            return new MultiServerCluster("1", settings, factory, new NoOpClusterListener());
        }
    }

    private List<ServerAddress> getHosts(final MongoClientURI connectionString) throws UnknownHostException {
        List<ServerAddress> serverAddresses = new ArrayList<ServerAddress>();
        for (String host : connectionString.getHosts()) {
            serverAddresses.add(new ServerAddress(host));
        }
        return serverAddresses;
    }

    private ClusterConnectionMode getMode(final MongoClientURI connectionString) {
        if (connectionString.getHosts().size() > 1 || connectionString.getOptions().getRequiredReplicaSetName() != null) {
            return ClusterConnectionMode.Multiple;
        } else {
            return ClusterConnectionMode.Single;
        }
    }
}
