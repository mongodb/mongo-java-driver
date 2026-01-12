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

package com.mongodb.connection;

import com.mongodb.ClusterFixture;
import com.mongodb.MongoConfigurationException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.Tag;
import com.mongodb.TagSet;
import com.mongodb.event.ServerDescriptionChangedEvent;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.connection.BaseCluster;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.connection.Server;
import com.mongodb.internal.connection.ServerTuple;
import com.mongodb.internal.connection.TestClusterableServerFactory;
import com.mongodb.internal.mockito.MongoMockito;
import com.mongodb.internal.selector.ReadPreferenceServerSelector;
import com.mongodb.internal.selector.WritableServerSelector;
import com.mongodb.internal.time.Timeout;
import com.mongodb.lang.NonNull;
import com.mongodb.lang.Nullable;
import com.mongodb.selector.ServerSelector;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.json.JsonWriterSettings;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import util.JsonPoweredTestHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS;
import static com.mongodb.connection.ServerDescription.MIN_DRIVER_WIRE_VERSION;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

// See https://github.com/mongodb/specifications/tree/master/source/server-selection/tests
@RunWith(Parameterized.class)
public class ServerSelectionSelectionTest {
    private final String description;
    private final BsonDocument definition;
    private final ClusterDescription clusterDescription;
    private final long heartbeatFrequencyMS;
    private final boolean error;
    private final List<ServerAddress> deprioritizedServerAddresses;

    private static final long SERVER_SELECTION_TIMEOUT_MS = 200;
    private static final Set<String> TOPOLOGY_DESCRIPTION_FIELDS = new HashSet<>(Arrays.asList("type", "servers"));
    private static final Set<String> SERVER_DESCRIPTION_FIELDS = new HashSet<>(Arrays.asList(
            "address", "type", "tags", "avg_rtt_ms", "lastWrite", "lastUpdateTime", "maxWireVersion"));
    private static final Set<String> READ_PREFERENCE_FIELDS = new HashSet<>(
            Arrays.asList("mode", "tag_sets", "maxStalenessSeconds"));

    public ServerSelectionSelectionTest(final String description, final BsonDocument definition) {
        this.description = description;
        this.definition = definition;
        this.heartbeatFrequencyMS = definition.getNumber("heartbeatFrequencyMS", new BsonInt64(10000)).longValue();
        this.error = definition.getBoolean("error", BsonBoolean.FALSE).getValue();
        this.clusterDescription = buildClusterDescription(definition.getDocument("topology_description"),
                ServerSettings.builder().heartbeatFrequency(heartbeatFrequencyMS, TimeUnit.MILLISECONDS).build());
        this.deprioritizedServerAddresses = extractDeprioritizedServerAddresses(definition);
    }

    @Test
    public void shouldPassAllOutcomes() {
        // skip this test because the driver prohibits maxStaleness or tagSets with mode of primary at a much lower level
        assumeTrue(!description.endsWith("/max-staleness/tests/ReplicaSetWithPrimary/MaxStalenessWithModePrimary.json"));
        ServerTuple serverTuple;
        BaseCluster cluster = null;
        try {
            ServerSelector serverSelector = getServerSelector();
            OperationContext operationContext = createOperationContext();
            Cluster.ServersSnapshot serversSnapshot = createServersSnapshot(clusterDescription);
            cluster = createTestCluster(clusterDescription, serversSnapshot);
            serverTuple = cluster.selectServer(serverSelector, operationContext);
            if (error) {
                fail("Should have thrown exception");
            }
        } catch (MongoConfigurationException e) {
            if (!error) {
                fail("Should not have thrown exception: " + e);
            }
            return;
        } catch (MongoTimeoutException mongoTimeoutException) {
            List<ServerDescription> inLatencyWindowServers = buildServerDescriptions(definition.getArray("in_latency_window"));
            assertTrue("Expected emtpy but was " + inLatencyWindowServers.size() + " " + definition.toJson(
                    JsonWriterSettings.builder()
                            .indent(true).build()), inLatencyWindowServers.isEmpty());
            return;
        } finally {
            if (cluster != null) {
                cluster.close();
            }
        }
        List<ServerDescription> inLatencyWindowServers = buildServerDescriptions(definition.getArray("in_latency_window"));
        assertNotNull(serverTuple);
        assertTrue(inLatencyWindowServers.stream().anyMatch(s -> s.getAddress().equals(serverTuple.getServerDescription().getAddress())));
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        List<Object[]> data = new ArrayList<>();
        //source/server-selection/tests/server_selection/ReplicaSetNoPrimary/read/DeprioritizedPrimary.json
        for (BsonDocument testDocument : JsonPoweredTestHelper.getSpecTestDocuments("server-selection/tests/server_selection")) {
            String resourcePath = testDocument.getString("resourcePath").getValue();
            data.add(new Object[]{resourcePath, testDocument});
        }
        for (BsonDocument testDocument : JsonPoweredTestHelper.getSpecTestDocuments("max-staleness/tests")) {
            data.add(new Object[]{testDocument.getString("resourcePath").getValue(), testDocument});
        }
        return data;
    }

    public static ClusterDescription buildClusterDescription(final BsonDocument topologyDescription,
                                                             @Nullable final ServerSettings serverSettings) {
        validateTestDescriptionFields(topologyDescription.keySet(), TOPOLOGY_DESCRIPTION_FIELDS);
        ClusterType clusterType = getClusterType(topologyDescription.getString("type").getValue());
        ClusterConnectionMode connectionMode = getClusterConnectionMode(clusterType);
        List<ServerDescription> servers = buildServerDescriptions(topologyDescription.getArray("servers"));
        return new ClusterDescription(connectionMode, clusterType, servers, ClusterSettings.builder().build(),
                serverSettings == null ? ServerSettings.builder().build() : serverSettings);
    }

    @NonNull
    private static ClusterConnectionMode getClusterConnectionMode(final ClusterType clusterType) {
        if (clusterType == ClusterType.LOAD_BALANCED) {
            return ClusterConnectionMode.LOAD_BALANCED;
        }
        return ClusterConnectionMode.MULTIPLE;
    }

    private static ClusterType getClusterType(final String type) {
        if (type.equals("Single")) {
            return ClusterType.STANDALONE;
        } else if (type.startsWith("ReplicaSet")) {
            return ClusterType.REPLICA_SET;
        } else if (type.equals("Sharded")) {
            return ClusterType.SHARDED;
        } else if (type.equals("LoadBalanced")) {
            return ClusterType.LOAD_BALANCED;
        } else if (type.equals("Unknown")) {
            return ClusterType.UNKNOWN;
        }

        throw new UnsupportedOperationException("Unknown topology type: " + type);
    }

    private static List<ServerDescription> buildServerDescriptions(final BsonArray serverDescriptions) {
        List<ServerDescription> descriptions = new ArrayList<>();
        for (BsonValue document : serverDescriptions) {
            descriptions.add(buildServerDescription(document.asDocument()));
        }
        return descriptions;
    }

    private static ServerDescription buildServerDescription(final BsonDocument serverDescription) {
        validateTestDescriptionFields(serverDescription.keySet(), SERVER_DESCRIPTION_FIELDS);
        ServerDescription.Builder builder = ServerDescription.builder();
        builder.address(new ServerAddress(serverDescription.getString("address").getValue()));
        ServerType serverType = getServerType(serverDescription.getString("type").getValue());
        builder.ok(serverType != ServerType.UNKNOWN);
        builder.type(serverType);
        if (serverDescription.containsKey("tags")) {
            builder.tagSet(buildTagSet(serverDescription.getDocument("tags")));
        }
        if (serverDescription.containsKey("avg_rtt_ms")) {
            builder.roundTripTime(serverDescription.getNumber("avg_rtt_ms").asInt32().getValue(), TimeUnit.MILLISECONDS);
        }
        builder.state(ServerConnectionState.CONNECTED);
        if (serverDescription.containsKey("lastWrite")) {
            builder.lastWriteDate(new Date(serverDescription.getDocument("lastWrite").getNumber("lastWriteDate").longValue()));
        }
        if (serverDescription.containsKey("lastUpdateTime")) {
            builder.lastUpdateTimeNanos(serverDescription.getNumber("lastUpdateTime").longValue() * 1000000);  // convert to nanos
        } else {
            builder.lastUpdateTimeNanos(42L);
        }
        if (serverDescription.containsKey("maxWireVersion")) {
            builder.maxWireVersion(serverDescription.getNumber("maxWireVersion").intValue());
        } else {
            builder.maxWireVersion(MIN_DRIVER_WIRE_VERSION);
        }
        return builder.build();
    }

    private static ServerType getServerType(final String serverTypeString) {
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
        } else if (serverTypeString.equals("LoadBalancer")) {
            serverType = ServerType.LOAD_BALANCER;
        } else if (serverTypeString.equals("PossiblePrimary")) {
            serverType = ServerType.UNKNOWN;
        } else if (serverTypeString.equals("Unknown")) {
            serverType = ServerType.UNKNOWN;
        } else {
            throw new UnsupportedOperationException("No handler for server type " + serverTypeString);
        }
        return serverType;
    }

    private List<TagSet> buildTagSets(final BsonArray tags) {
        List<TagSet> tagSets = new ArrayList<>();
        for (BsonValue tag : tags) {
            tagSets.add(buildTagSet(tag.asDocument()));
        }
        return tagSets;
    }


    private static TagSet buildTagSet(final BsonDocument tags) {
        List<Tag> tagsSetTags = new ArrayList<>();
        for (String key : tags.keySet()) {
            tagsSetTags.add(new Tag(key, tags.getString(key).getValue()));
        }
        return new TagSet(tagsSetTags);
    }

    private ServerSelector getServerSelector() {
        if (definition.getString("operation", new BsonString("read")).getValue().equals("write")) {
            return new WritableServerSelector();
        } else {
            BsonDocument readPreferenceDefinition = definition.getDocument("read_preference");
            validateTestDescriptionFields(readPreferenceDefinition.keySet(), READ_PREFERENCE_FIELDS);
            ReadPreference readPreference;
            if (readPreferenceDefinition.getString("mode").getValue().equals("Primary")) {
                readPreference = ReadPreference.valueOf("Primary");
            } else if (readPreferenceDefinition.containsKey("maxStalenessSeconds")) {
                readPreference = ReadPreference.valueOf(readPreferenceDefinition.getString("mode", new BsonString("Primary")).getValue(),
                        buildTagSets(readPreferenceDefinition.getArray("tag_sets", new BsonArray())),
                        Math.round(readPreferenceDefinition.getNumber("maxStalenessSeconds").doubleValue() * 1000), TimeUnit.MILLISECONDS);
            } else {
                readPreference = ReadPreference.valueOf(readPreferenceDefinition.getString("mode", new BsonString("Primary")).getValue(),
                        buildTagSets(readPreferenceDefinition.getArray("tag_sets", new BsonArray())));
            }
            return new ReadPreferenceServerSelector(readPreference);
        }
    }

    private static List<ServerAddress> extractDeprioritizedServerAddresses(final BsonDocument definition) {
        if (!definition.containsKey("deprioritized_servers")) {
            return Collections.emptyList();
        }
        return definition.getArray("deprioritized_servers")
                .stream()
                .map(BsonValue::asDocument)
                .map(ServerSelectionSelectionTest::buildServerDescription)
                .map(ServerDescription::getAddress)
                .collect(Collectors.toList());
    }

    private OperationContext createOperationContext() {
        OperationContext operationContext =
                OperationContext.simpleOperationContext(new TimeoutContext(TIMEOUT_SETTINGS.withServerSelectionTimeoutMS(SERVER_SELECTION_TIMEOUT_MS)));
        OperationContext.ServerDeprioritization serverDeprioritization = operationContext.getServerDeprioritization();
        for (ServerAddress address : deprioritizedServerAddresses) {
            serverDeprioritization.updateCandidate(address);
            serverDeprioritization.onAttemptFailure(new MongoConfigurationException("test"));
        }
        return operationContext;
    }

    private static Cluster.ServersSnapshot createServersSnapshot(
            final ClusterDescription clusterDescription) {
        Map<ServerAddress, Server> serverMap = new HashMap<>();
        for (ServerDescription desc : clusterDescription.getServerDescriptions()) {
            serverMap.put(desc.getAddress(), MongoMockito.mock(Server.class, server -> {
                // Operation count selector should select any server since they all have 0 operation count.
                when(server.operationCount()).thenReturn(0);
            }));
        }
        return serverMap::get;
    }

    private BaseCluster createTestCluster(final ClusterDescription clusterDescription, final Cluster.ServersSnapshot serversSnapshot) {
        BaseCluster baseCluster = new BaseCluster(
                new ClusterId(),
                clusterDescription.getClusterSettings(),
                new TestClusterableServerFactory(),
                ClusterFixture.CLIENT_METADATA) {
            @Override
            protected void connect() {
            }

            @Override
            public ServersSnapshot getServersSnapshot(final Timeout serverSelectionTimeout, final TimeoutContext timeoutContext) {
                return serversSnapshot;
            }

            @Override
            public void onChange(final ServerDescriptionChangedEvent event) {
            }
        };
        baseCluster.updateDescription(clusterDescription);
        return baseCluster;
    }

    private static void validateTestDescriptionFields(final Set<String> actualFields, final Set<String> knownFields) {
        Set<String> unknownFields = new HashSet<>(actualFields);
        unknownFields.removeAll(knownFields);
        if (!unknownFields.isEmpty()) {
            throw new UnsupportedOperationException("Unknown fields: " + unknownFields);
        }
    }
}
