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

package com.mongodb.internal.selector;

import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerDescription;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.mongodb.connection.ServerConnectionState.CONNECTED;
import static com.mongodb.connection.ServerConnectionState.CONNECTING;
import static com.mongodb.connection.ServerDescription.builder;
import static com.mongodb.connection.ServerType.REPLICA_SET_PRIMARY;
import static com.mongodb.connection.ServerType.REPLICA_SET_SECONDARY;
import static com.mongodb.internal.operation.ServerVersionHelper.FIVE_DOT_ZERO_WIRE_VERSION;
import static com.mongodb.internal.operation.ServerVersionHelper.FOUR_DOT_FOUR_WIRE_VERSION;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ReadPreferenceWithFallbackServerSelectorTest {

    @Test
    public void shouldSelectCorrectServersWhenAtLeastOneServerIsOlderThanMinimum() {
        ReadPreferenceWithFallbackServerSelector selector =
                new ReadPreferenceWithFallbackServerSelector(
                        ReadPreference.secondary(), FIVE_DOT_ZERO_WIRE_VERSION, ReadPreference.primary());

        ClusterDescription clusterDescription = new ClusterDescription(
                ClusterConnectionMode.MULTIPLE,
                ClusterType.REPLICA_SET,
                asList(
                        builder().ok(true).state(CONNECTED).type(REPLICA_SET_PRIMARY).address(new ServerAddress("localhost:27017"))
                                .maxWireVersion(FOUR_DOT_FOUR_WIRE_VERSION).build(),
                        builder().ok(true).state(CONNECTED).type(REPLICA_SET_SECONDARY).address(new ServerAddress("localhost:27018"))
                                .maxWireVersion(FIVE_DOT_ZERO_WIRE_VERSION).build()));
        assertEquals(clusterDescription.getServerDescriptions().stream()
                        .filter(serverDescription -> serverDescription.getType() == REPLICA_SET_PRIMARY).collect(toList()),
                selector.select(clusterDescription));
        assertEquals(ReadPreference.primary(), selector.getAppliedReadPreference());
    }

    @Test
    public void shouldSelectCorrectServersWhenAllServersAreAtLeastMinimum() {
        ReadPreferenceWithFallbackServerSelector selector =
                new ReadPreferenceWithFallbackServerSelector(
                        ReadPreference.secondary(), FIVE_DOT_ZERO_WIRE_VERSION, ReadPreference.primary());

        ClusterDescription clusterDescription = new ClusterDescription(
                ClusterConnectionMode.MULTIPLE,
                ClusterType.REPLICA_SET,
                asList(
                        builder().ok(true).state(CONNECTED).type(REPLICA_SET_PRIMARY).address(new ServerAddress("localhost:27017"))
                                .maxWireVersion(FIVE_DOT_ZERO_WIRE_VERSION).build(),
                        builder().ok(true).state(CONNECTED).type(REPLICA_SET_SECONDARY).address(new ServerAddress("localhost:27018"))
                                .maxWireVersion(FIVE_DOT_ZERO_WIRE_VERSION).build()));
        assertEquals(clusterDescription.getServerDescriptions().stream()
                        .filter(serverDescription -> serverDescription.getType() == REPLICA_SET_SECONDARY).collect(toList()),
                selector.select(clusterDescription));
        assertEquals(ReadPreference.secondary(), selector.getAppliedReadPreference());
    }

    @Test
    public void shouldSelectCorrectServersWhenNoServersHaveBeenDiscovered() {
        ReadPreferenceWithFallbackServerSelector selector =
                new ReadPreferenceWithFallbackServerSelector(
                        ReadPreference.secondary(), FIVE_DOT_ZERO_WIRE_VERSION, ReadPreference.primary());

        ClusterDescription clusterDescription = new ClusterDescription(
                ClusterConnectionMode.MULTIPLE,
                ClusterType.REPLICA_SET,
                asList(builder().ok(false).state(CONNECTING).address(new ServerAddress("localhost:27017")).build(),
                        builder().ok(false).state(CONNECTING).address(new ServerAddress("localhost:27018")).build()));
        assertEquals(emptyList(), selector.select(clusterDescription));
        assertEquals(ReadPreference.secondary(), selector.getAppliedReadPreference());

        // when there is one connecting server, and a primary and secondary with maxWireVersion >= minWireVersion, apply read preference
        clusterDescription = new ClusterDescription(
                ClusterConnectionMode.MULTIPLE,
                ClusterType.REPLICA_SET,
                asList(
                        builder().ok(false).state(CONNECTING).address(new ServerAddress("localhost:27017")).build(),
                        builder().ok(true).state(CONNECTED).type(REPLICA_SET_PRIMARY).address(new ServerAddress("localhost:27018"))
                                .maxWireVersion(FIVE_DOT_ZERO_WIRE_VERSION)
                                .build(),
                        builder().ok(true).state(CONNECTED).type(REPLICA_SET_SECONDARY).address(new ServerAddress("localhost:27019"))
                                .maxWireVersion(FIVE_DOT_ZERO_WIRE_VERSION)
                                .build()));
        List<ServerDescription> serverDescriptionList = selector.select(clusterDescription);
        assertEquals(clusterDescription.getServerDescriptions().stream()
                        .filter(serverDescription -> serverDescription.getType() == REPLICA_SET_SECONDARY).collect(toList()),
                serverDescriptionList);
        assertEquals(ReadPreference.secondary(), selector.getAppliedReadPreference());
    }
}
