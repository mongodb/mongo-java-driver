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

package com.mongodb.connection;

import com.mongodb.ConnectionString;
import com.mongodb.ServerAddress;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.json.JsonReader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.mongodb.connection.ClusterType.REPLICA_SET;
import static com.mongodb.connection.ClusterType.SHARDED;
import static com.mongodb.connection.ClusterType.UNKNOWN;
import static com.mongodb.connection.DescriptionHelper.createServerDescription;
import static com.mongodb.connection.ServerConnectionState.CONNECTING;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.runners.Parameterized.Parameters;

// See https://github.com/mongodb/specifications/tree/master/source/server-discovery-and-monitoring/tests
@RunWith(Parameterized.class)
public class JsonPoweredClusterTest {

    private final TestClusterableServerFactory factory = new TestClusterableServerFactory();
    private final BsonDocument definition;
    private final Cluster cluster;

    public JsonPoweredClusterTest(final File file) throws IOException {
        definition = getTestDocument(getFileAsString(file));
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

    private void assertServers(final BsonDocument servers) {
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
        for (ServerDescription cur: cluster.getDescription().getAll()) {
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
            assertEquals(REPLICA_SET, cluster.getDescription().getType());
            assertEquals(1, cluster.getDescription().getPrimaries().size());
        } else if (topologyType.equals("ReplicaSetNoPrimary")) {
            assertEquals(MultiServerCluster.class, cluster.getClass());
            assertEquals(REPLICA_SET, cluster.getDescription().getType());
            assertEquals(0, cluster.getDescription().getPrimaries().size());
        } else if (topologyType.equals("Sharded")) {
            assertEquals(MultiServerCluster.class, cluster.getClass());
            assertEquals(SHARDED, cluster.getDescription().getType());
        } else if (topologyType.equals("Unknown")) {
            assertEquals(UNKNOWN, cluster.getDescription().getType());
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
                                                        new BsonDocument("versionArray",
                                                                         new BsonArray(asList(new BsonInt32(2),
                                                                                              new BsonInt32(6),
                                                                                              new BsonInt32(0)))),
                                                        5000000);
        }
        factory.sendNotification(serverAddress, serverDescription);
    }

    @Parameters // (name = "{1}")  for when we update to JUnit >= 4.11
    public static Collection<Object[]> data() throws URISyntaxException {
        List<File> files = getTestFiles();
        List<Object[]> data = new ArrayList<Object[]>();
        for (File file : files) {
            data.add(new Object[]{file});
        }
        return data;
    }

    BsonDocument getTestDocument(final String contents) throws IOException {
        return new BsonDocumentCodec().decode(new JsonReader(contents), DecoderContext.builder().build());
    }

    private String getFileAsString(final File file) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = null;
        StringBuilder stringBuilder = new StringBuilder();
        String ls = System.getProperty("line.separator");

        while ((line = reader.readLine()) != null) {
            stringBuilder.append(line);
            stringBuilder.append(ls);
        }

        return stringBuilder.toString();
    }

    Cluster getCluster(final String uri) {
        ConnectionString connectionString = new ConnectionString(uri);

        ClusterSettings settings = ClusterSettings.builder()
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

    private static List<File> getTestFiles() throws URISyntaxException {
        List<File> files = new ArrayList<File>();
        addFilesFromDirectory(new File(JsonPoweredClusterTest.class.getResource("/server-discovery-and-monitoring").toURI()), files);
        return files;
    }

    private static void addFilesFromDirectory(final File directory, final List<File> files) {
        for (String fileName : directory.list()) {
            File file = new File(directory, fileName);
            if (file.isDirectory()) {
                addFilesFromDirectory(file, files);
            } else if (file.getName().endsWith(".json")) {
                files.add(file);
            }
        }
    }
}
