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

import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.time.Timeout;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonValue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT;
import static com.mongodb.ClusterFixture.getClusterDescription;
import static com.mongodb.internal.connection.ClusterDescriptionHelper.getPrimaries;
import static com.mongodb.internal.event.EventListenerHelper.NO_OP_CLUSTER_LISTENER;
import static com.mongodb.internal.event.EventListenerHelper.NO_OP_SERVER_LISTENER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

// See https://github.com/mongodb/specifications/tree/master/source/server-discovery-and-monitoring/tests
@RunWith(Parameterized.class)
public class ServerDiscoveryAndMonitoringTest extends AbstractServerDiscoveryAndMonitoringTest {

    public ServerDiscoveryAndMonitoringTest(final String description, final BsonDocument definition) {
        super(definition);
        init(serverAddress -> NO_OP_SERVER_LISTENER, NO_OP_CLUSTER_LISTENER);
    }

    @Test
    public void shouldPassAllOutcomes() {
        for (BsonValue phase : getDefinition().getArray("phases")) {
            if (phase.asDocument().containsKey("responses")) {
                for (BsonValue response : phase.asDocument().getArray("responses")) {
                    applyResponse(response.asArray());
                }
            }
            if (phase.asDocument().containsKey("applicationErrors")) {
                for (BsonValue response : phase.asDocument().getArray("applicationErrors")) {
                    applyApplicationError(response.asDocument());
                }
            }
            BsonDocument outcome = phase.asDocument().getDocument("outcome");
            assertTopology(outcome);
            assertServers(outcome.getDocument("servers"));
        }
    }

    private void assertTopology(final BsonDocument outcome) {
        String topologyType = outcome.getString("topologyType").getValue();
        assertTopologyType(topologyType);
        assertLogicalSessionTimeout(outcome.get("logicalSessionTimeoutMinutes", BsonNull.VALUE));
        assertDriverCompatibility(outcome.get("compatible"));
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return data("/server-discovery-and-monitoring");
    }

    private void assertServers(final BsonDocument servers) {
        if (servers.size() != getCluster().getCurrentDescription().getServerDescriptions().size()) {
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

        if (expectedServerDescriptionDocument.isObjectId("electionId")) {
            assertNotNull(serverDescription.getElectionId());
            assertEquals(expectedServerDescriptionDocument.getObjectId("electionId").getValue(), serverDescription.getElectionId());
        } else {
            assertNull(serverDescription.getElectionId());
        }

        if (expectedServerDescriptionDocument.isNumber("setVersion")) {
            assertNotNull(serverDescription.getSetVersion());
            assertEquals(expectedServerDescriptionDocument.getNumber("setVersion").intValue(),
                    serverDescription.getSetVersion().intValue());
        } else {
            assertNull(serverDescription.getSetVersion());
        }

        if (expectedServerDescriptionDocument.isString("setName")) {
            assertNotNull(serverDescription.getSetName());
            assertEquals(expectedServerDescriptionDocument.getString("setName").getValue(), serverDescription.getSetName());
        } else {
            assertNull(serverDescription.getSetName());
        }

        if (expectedServerDescriptionDocument.isDocument("pool")) {
            int expectedGeneration = expectedServerDescriptionDocument.getDocument("pool").getNumber("generation").intValue();
            Timeout serverSelectionTimeout = OPERATION_CONTEXT.getTimeoutContext().computeServerSelectionTimeout();
            DefaultServer server = (DefaultServer) getCluster()
                    .getServersSnapshot(serverSelectionTimeout, OPERATION_CONTEXT.getTimeoutContext())
                    .getServer(new ServerAddress(serverName));
            assertEquals(expectedGeneration, server.getConnectionPool().getGeneration());
        }
    }

    private ServerDescription getServerDescription(final String serverName) {
        ServerDescription serverDescription  = null;
        for (ServerDescription cur : getCluster().getCurrentDescription().getServerDescriptions()) {
            if (cur.getAddress().equals(new ServerAddress(serverName))) {
                serverDescription = cur;
                break;
            }
        }
        return serverDescription;
    }

    private void assertTopologyType(final String topologyType) {
        switch (topologyType) {
            case "Single":
                assertTrue(getCluster().getClass() == SingleServerCluster.class
                        || (getCluster().getClass() == MultiServerCluster.class
                            && getClusterDescription(getCluster()).getType() == ClusterType.STANDALONE));
                assertEquals(getClusterType(topologyType, getCluster().getCurrentDescription().getServerDescriptions()),
                        getCluster().getCurrentDescription().getType());
                break;
            case "ReplicaSetWithPrimary":
                assertEquals(MultiServerCluster.class, getCluster().getClass());
                assertEquals(getClusterType(topologyType), getCluster().getCurrentDescription().getType());
                assertEquals(1, getPrimaries(getCluster().getCurrentDescription()).size());
                break;
            case "ReplicaSetNoPrimary":
                assertEquals(MultiServerCluster.class, getCluster().getClass());
                assertEquals(getClusterType(topologyType), getCluster().getCurrentDescription().getType());
                assertEquals(0, getPrimaries(getCluster().getCurrentDescription()).size());
                break;
            case "Sharded":
                assertEquals(MultiServerCluster.class, getCluster().getClass());
                assertEquals(getClusterType(topologyType), getCluster().getCurrentDescription().getType());
                break;
            case "LoadBalanced":
                assertEquals(LoadBalancedCluster.class, getCluster().getClass());
                assertEquals(getClusterType(topologyType), getCluster().getCurrentDescription().getType());
                break;
            case "Unknown":
                assertEquals(getClusterType(topologyType), getCluster().getCurrentDescription().getType());
                break;
            default:
                throw new UnsupportedOperationException("No handler for topology type " + topologyType);
        }
    }

    private void assertLogicalSessionTimeout(final BsonValue logicalSessionTimeoutMinutes) {
        if (logicalSessionTimeoutMinutes.isNull()) {
            assertNull(getCluster().getCurrentDescription().getLogicalSessionTimeoutMinutes());
        } else if (logicalSessionTimeoutMinutes.isNumber()) {
            assertEquals((Integer) logicalSessionTimeoutMinutes.asNumber().intValue(),
                    getCluster().getCurrentDescription().getLogicalSessionTimeoutMinutes());
        }
    }

    private void assertDriverCompatibility(final BsonValue compatible) {
        if (compatible != null) {
            assertEquals(compatible.asBoolean().getValue(), getCluster().getCurrentDescription().isCompatibleWithDriver());
        }
    }
}
