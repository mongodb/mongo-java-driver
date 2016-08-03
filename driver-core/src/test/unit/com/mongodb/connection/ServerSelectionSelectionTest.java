/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.connection;

import com.mongodb.MongoConfigurationException;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.Tag;
import com.mongodb.TagSet;
import com.mongodb.selector.CompositeServerSelector;
import com.mongodb.selector.LatencyMinimizingServerSelector;
import com.mongodb.selector.ReadPreferenceServerSelector;
import com.mongodb.selector.ServerSelector;
import com.mongodb.selector.WritableServerSelector;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import util.JsonPoweredTestHelper;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

// See https://github.com/mongodb/specifications/tree/master/source/server-selection/tests
@RunWith(Parameterized.class)
public class ServerSelectionSelectionTest {
    private final String description;
    private final BsonDocument definition;
    private final ClusterDescription clusterDescription;
    private final long heartbeatFrequencyMS;
    private final boolean error;

    public ServerSelectionSelectionTest(final String description, final BsonDocument definition) {
        this.description = description;
        this.definition = definition;
        this.heartbeatFrequencyMS = definition.getNumber("heartbeatFrequencyMS" , new BsonInt64(10000)).longValue();
        this.error = definition.getBoolean("error" , BsonBoolean.FALSE).getValue();
        this.clusterDescription = buildClusterDescription(definition.getDocument("topology_description"));
    }

    @Test
    public void shouldPassAllOutcomes() {
        // skip these tests because the driver prohibits maxStaleness or tagSets with mode of primary at a much lower level
        assumeTrue(!description.equals("max-staleness/server_selection/ReplicaSetWithPrimary/MaxStalenessWithModePrimary.json"));
        assumeTrue(!description.equals("max-staleness/server_selection/ReplicaSetWithPrimary/PrimaryPreferred_incompatible.json"));

        ServerSelector serverSelector = null;
        List<ServerDescription> suitableServers = buildServerDescriptions(definition.getArray("suitable_servers" , new BsonArray()));
        List<ServerDescription> selectedServers = null;
        try {
            serverSelector = getServerSelector();
            selectedServers = serverSelector.select(clusterDescription);
            if (error) {
                fail("Should have thrown exception");
            }
        } catch (MongoConfigurationException e) {
            if (!error) {
                fail("Should not have thrown exception: " + e);
            }
            return;
        }
        assertServers(selectedServers, suitableServers);

        ServerSelector latencyBasedServerSelector = new CompositeServerSelector(asList(serverSelector,
                new LatencyMinimizingServerSelector(15, TimeUnit.MILLISECONDS)));
        List<ServerDescription> inLatencyWindowServers = buildServerDescriptions(definition.getArray("in_latency_window"));
        List<ServerDescription> latencyBasedSelectedServers = latencyBasedServerSelector.select(clusterDescription);
        assertServers(latencyBasedSelectedServers, inLatencyWindowServers);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        List<Object[]> data = new ArrayList<Object[]>();
        for (File file : JsonPoweredTestHelper.getTestFiles("/server-selection/server_selection")) {
            data.add(new Object[]{getDescription("server-selection/server_selection", file), JsonPoweredTestHelper.getTestDocument(file)});
        }
        for (File file : JsonPoweredTestHelper.getTestFiles("/max-staleness/server_selection")) {
            data.add(new Object[]{getDescription("max-staleness/server_selection", file), JsonPoweredTestHelper.getTestDocument(file)});
        }
        return data;
    }

    private static String getDescription(final String root, final File file) {
        return root + "/" + file.getParentFile().getName() + "/" + file.getName();
    }

    private ClusterDescription buildClusterDescription(final BsonDocument topologyDescription) {
        ClusterType clusterType = getClusterType(topologyDescription.getString("type").getValue());
        ClusterConnectionMode connectionMode = ClusterConnectionMode.MULTIPLE;
        List<ServerDescription> servers = buildServerDescriptions(topologyDescription.getArray("servers"));
        return new ClusterDescription(connectionMode, clusterType, servers, null,
                                             ServerSettings.builder()
                                                     .heartbeatFrequency(heartbeatFrequencyMS, TimeUnit.MILLISECONDS)
                                                     .build());
    }

    private ClusterType getClusterType(final String type) {
        if (type.equals("Single")) {
            return ClusterType.STANDALONE;
        } else if (type.startsWith("ReplicaSet")) {
            return ClusterType.REPLICA_SET;
        } else if (type.equals("Sharded")) {
            return ClusterType.SHARDED;
        } else if (type.equals("Unknown")) {
            return ClusterType.UNKNOWN;
        }

        throw new UnsupportedOperationException("Unknown topology type: " + type);
    }

    private List<ServerDescription> buildServerDescriptions(final BsonArray serverDescriptions) {
        List<ServerDescription> descriptions = new ArrayList<ServerDescription>();
        for (BsonValue document : serverDescriptions) {
            descriptions.add(buildServerDescription(document.asDocument()));
        }
        return descriptions;
    }

    private ServerDescription buildServerDescription(final BsonDocument serverDescription) {
        ServerDescription.Builder builder = ServerDescription.builder();
        builder.address(new ServerAddress(serverDescription.getString("address").getValue()));
        builder.type(getServerType(serverDescription.getString("type").getValue()));
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
        }
        builder.ok(true);
        return builder.build();
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

    private List<TagSet> buildTagSets(final BsonArray tags) {
        List<TagSet> tagSets = new ArrayList<TagSet>();
        for (BsonValue tag : tags) {
            tagSets.add(buildTagSet(tag.asDocument()));
        }
        return tagSets;
    }


    private TagSet buildTagSet(final BsonDocument tags) {
        List<Tag> tagsSetTags = new ArrayList<Tag>();
        for (String key : tags.keySet()) {
            tagsSetTags.add(new Tag(key, tags.getString(key).getValue()));
        }
        return new TagSet(tagsSetTags);
    }

    private ServerSelector getServerSelector() {
        if (definition.getString("operation" , new BsonString("read")).getValue().equals("write")) {
            return new WritableServerSelector();
        } else {
            BsonDocument readPreferenceDefinition = definition.getDocument("read_preference");
            ReadPreference readPreference;
            if (readPreferenceDefinition.getString("mode").getValue().equals("Primary")) {
                readPreference = ReadPreference.valueOf("Primary");
            } else {
                readPreference = ReadPreference.valueOf(readPreferenceDefinition.getString("mode", new BsonString("Primary")).getValue(),
                        buildTagSets(readPreferenceDefinition.getArray("tag_sets" , new BsonArray())),
                        readPreferenceDefinition.getNumber("maxStalenessMS" , new BsonInt64(0)).longValue(), TimeUnit.MILLISECONDS);
            }
            return new ReadPreferenceServerSelector(readPreference);
        }
    }

    private void assertServers(final List<ServerDescription> actual, final List<ServerDescription> expected) {
        assertEquals(expected.size(), actual.size());
        assertTrue(actual.containsAll(expected));
    }
}
