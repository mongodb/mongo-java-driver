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

import com.mongodb.ClusterFixture;
import com.mongodb.MongoConfigurationException;
import com.mongodb.MongoException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.Tag;
import com.mongodb.TagSet;
import com.mongodb.assertions.Assertions;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerSettings;
import com.mongodb.connection.ServerType;
import com.mongodb.event.ServerDescriptionChangedEvent;
import com.mongodb.internal.IgnorableRequestContext;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.mockito.MongoMockito;
import com.mongodb.internal.observability.micrometer.TracingManager;
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.mockito.Mockito.when;

/**
 * See <a href="https://github.com/mongodb/specifications/tree/master/source/server-selection/tests/server_selection">Server Selection Tests</a>.
 */
@RunWith(Parameterized.class)
public class ServerSelectionSelectionTest {
    private final String description;
    private final BsonDocument definition;
    private final ClusterDescription clusterDescription;
    private final boolean error;

    private static final Set<String> TOPOLOGY_DESCRIPTION_FIELDS = new HashSet<>(Arrays.asList("type", "servers"));
    private static final Set<String> SERVER_DESCRIPTION_FIELDS = new HashSet<>(Arrays.asList(
            "address", "type", "tags", "avg_rtt_ms", "lastWrite", "lastUpdateTime", "maxWireVersion"));
    private static final Set<String> READ_PREFERENCE_FIELDS = new HashSet<>(
            Arrays.asList("mode", "tag_sets", "maxStalenessSeconds"));

    public ServerSelectionSelectionTest(final String description, final BsonDocument definition) {
        this.description = description;
        this.definition = definition;

        long heartbeatFrequencyMS = definition.getNumber("heartbeatFrequencyMS", new BsonInt64(10000)).longValue();
        this.error = definition.getBoolean("error", BsonBoolean.FALSE).getValue();
        this.clusterDescription = buildClusterDescription(definition.getDocument("topology_description"),
                ServerSettings.builder().heartbeatFrequency(heartbeatFrequencyMS, TimeUnit.MILLISECONDS).build());
    }

    @Test
    public void shouldPassAllOutcomes() {
        // skip this test because the driver prohibits maxStaleness or tagSets with mode of primary at a much lower level
        assumeFalse(description.endsWith("/max-staleness/tests/ReplicaSetWithPrimary/MaxStalenessWithModePrimary.json"));
        ServerTuple serverTuple;
        ServerSelector serverSelector = getServerSelector();
        OperationContext operationContext = createOperationContext();
        Cluster.ServersSnapshot serversSnapshot = createServersSnapshot(clusterDescription);
        List<ServerDescription> inLatencyWindowServers = buildServerDescriptions(definition.getArray("in_latency_window", new BsonArray()));

        try (BaseCluster cluster = new TestCluster(clusterDescription, serversSnapshot)) {
            serverTuple = cluster.selectServer(serverSelector, operationContext);
            if (error) {
                fail(format("Should have thrown exception"));
            }
        } catch (MongoConfigurationException e) {
            if (!error) {
                fail(format("Should not have thrown exception: %s", e));
            }
            return;
        } catch (MongoTimeoutException mongoTimeoutException) {
            assertTrue(format("Expected empty but was %s", inLatencyWindowServers.size()),
                    inLatencyWindowServers.isEmpty());
            return;
        }
        assertNotNull(format("Server tuple should not be null"), serverTuple);
        assertTrue(format("Selected server should be in latency window. Selected server: %s", serverTuple.getServerDescription()),
                inLatencyWindowServers.stream().anyMatch(s -> s.equals(serverTuple.getServerDescription())));
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        List<Object[]> data = new ArrayList<>();
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
                new OperationContext(
                        IgnorableRequestContext.INSTANCE,
                        NoOpSessionContext.INSTANCE,
                        new TimeoutContext(TIMEOUT_SETTINGS.withServerSelectionTimeoutMS(0)),
                        TracingManager.NO_OP,
                        null,
                        null,
                        new OperationContext.ServerDeprioritization(true));
        OperationContext.ServerDeprioritization serverDeprioritization = operationContext.getServerDeprioritization();
        for (ServerAddress address : extractDeprioritizedServerAddresses(definition)) {
            serverDeprioritization.updateCandidate(address, clusterDescription.getType());
            // The spec defines deprioritized_servers as a pre-populated list to feed into the selection mechanism - not as "simulate the
            // failure that caused deprioritization." Thus, SystemOverloadedError used unconditionally regardless of the cluster type.
            MongoException error = new MongoException("test");
            error.addLabel(MongoException.SYSTEM_OVERLOADED_ERROR_LABEL);
            serverDeprioritization.onAttemptFailure(error);
        }
        return operationContext;
    }

    private static Cluster.ServersSnapshot createServersSnapshot(
            final ClusterDescription clusterDescription) {
        Map<ServerAddress, Server> serverMap = new HashMap<>();
        for (ServerDescription desc : clusterDescription.getServerDescriptions()) {
            serverMap.put(desc.getAddress(), MongoMockito.mock(Server.class, server -> {
                // `MinimumOperationCountServerSelector` should select any server since they all have 0 operation count.
                when(server.operationCount()).thenReturn(0);
            }));
        }
        return serverMap::get;
    }

    private static void validateTestDescriptionFields(final Set<String> actualFields, final Set<String> knownFields) {
        Set<String> unknownFields = new HashSet<>(actualFields);
        unknownFields.removeAll(knownFields);
        if (!unknownFields.isEmpty()) {
            throw new UnsupportedOperationException("Unknown fields: " + unknownFields);
        }
    }

    private static class TestCluster extends BaseCluster {
        private final ServersSnapshot serversSnapshot;

        TestCluster(final ClusterDescription clusterDescription, final ServersSnapshot serversSnapshot) {
            super(new ClusterId(), clusterDescription.getClusterSettings(), new TestClusterableServerFactory(),
                    ClusterFixture.CLIENT_METADATA);
            this.serversSnapshot = serversSnapshot;
            updateDescription(clusterDescription);
        }

        @Override
        protected void connect() {
            // NOOP: this method may be invoked in test cases where no server is expected to be selected.
        }

        @Override
        public ServersSnapshot getServersSnapshot(final Timeout serverSelectionTimeout, final TimeoutContext timeoutContext) {
            return serversSnapshot;
        }

        @Override
        public void onChange(final ServerDescriptionChangedEvent event) {
            Assertions.fail();
        }
    }

    private String format(final String messageFormat, final Object... args) {
        String message = String.format(messageFormat, args);
        return message + "\nTest Definition:\n" + definition.toJson(JsonWriterSettings.builder().indent(true).build());
    }
}
