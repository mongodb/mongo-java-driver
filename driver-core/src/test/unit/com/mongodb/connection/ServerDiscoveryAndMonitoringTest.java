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

package com.mongodb.connection;

import com.mongodb.ServerAddress;
import com.mongodb.event.ServerListener;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;

import static com.mongodb.connection.ClusterType.STANDALONE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

// See https://github.com/mongodb/specifications/tree/master/source/server-discovery-and-monitoring/tests
@RunWith(Parameterized.class)
public class ServerDiscoveryAndMonitoringTest extends AbstractServerDiscoveryAndMonitoringTest {

    public ServerDiscoveryAndMonitoringTest(final String description, final BsonDocument definition) {
        super(definition);
        init(new ServerListenerFactory() {
            @Override
            public ServerListener create(final ServerAddress serverAddress) {
                return new NoOpServerListener();
            }
        }, new NoOpClusterListener());
    }

    @Test
    public void shouldPassAllOutcomes() {
        for (BsonValue phase : getDefinition().getArray("phases")) {
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
        return data("/server-discovery-and-monitoring");
    }

    private void assertServers(final BsonDocument servers) {
        if (servers.size() != getCluster().getCurrentDescription().getAll().size()) {
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
    }

    private ServerDescription getServerDescription(final String serverName) {
        ServerDescription serverDescription  = null;
        for (ServerDescription cur : getCluster().getCurrentDescription().getAll()) {
            if (cur.getAddress().equals(new ServerAddress(serverName))) {
                serverDescription = cur;
                break;
            }
        }
        return serverDescription;
    }

    private void assertTopologyType(final String topologyType) {
        if (topologyType.equals("Single")) {
            assertEquals(SingleServerCluster.class, getCluster().getClass());
            assertEquals(getClusterType(topologyType), STANDALONE);
        } else if (topologyType.equals("ReplicaSetWithPrimary")) {
            assertEquals(MultiServerCluster.class, getCluster().getClass());
            assertEquals(getClusterType(topologyType), getCluster().getCurrentDescription().getType());
            assertEquals(1, getCluster().getCurrentDescription().getPrimaries().size());
        } else if (topologyType.equals("ReplicaSetNoPrimary")) {
            assertEquals(MultiServerCluster.class, getCluster().getClass());
            assertEquals(getClusterType(topologyType), getCluster().getCurrentDescription().getType());
            assertEquals(0, getCluster().getCurrentDescription().getPrimaries().size());
        } else if (topologyType.equals("Sharded")) {
            assertEquals(MultiServerCluster.class, getCluster().getClass());
            assertEquals(getClusterType(topologyType), getCluster().getCurrentDescription().getType());
        } else if (topologyType.equals("Unknown")) {
            assertEquals(getClusterType(topologyType), getCluster().getCurrentDescription().getType());
        } else {
            throw new UnsupportedOperationException("No handler for topology type " + topologyType);
        }
    }
}
