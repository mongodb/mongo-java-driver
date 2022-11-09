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

import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.Tag;
import com.mongodb.TagSet;
import com.mongodb.internal.connection.ClusterDescriptionHelper;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE;
import static com.mongodb.connection.ClusterConnectionMode.SINGLE;
import static com.mongodb.connection.ClusterType.REPLICA_SET;
import static com.mongodb.connection.ClusterType.SHARDED;
import static com.mongodb.connection.ClusterType.STANDALONE;
import static com.mongodb.connection.ClusterType.UNKNOWN;
import static com.mongodb.connection.ServerConnectionState.CONNECTED;
import static com.mongodb.connection.ServerConnectionState.CONNECTING;
import static com.mongodb.connection.ServerDescription.MAX_DRIVER_WIRE_VERSION;
import static com.mongodb.connection.ServerDescription.MIN_DRIVER_WIRE_VERSION;
import static com.mongodb.connection.ServerDescription.builder;
import static com.mongodb.connection.ServerType.REPLICA_SET_OTHER;
import static com.mongodb.connection.ServerType.REPLICA_SET_PRIMARY;
import static com.mongodb.connection.ServerType.REPLICA_SET_SECONDARY;
import static com.mongodb.internal.connection.ClusterDescriptionHelper.getAll;
import static com.mongodb.internal.connection.ClusterDescriptionHelper.getAny;
import static com.mongodb.internal.connection.ClusterDescriptionHelper.getAnyPrimaryOrSecondary;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ClusterDescriptionTest {

    private ServerDescription primary, secondary, otherSecondary, uninitiatedMember, notOkMember;
    private ClusterDescription cluster;

    @Before
    public void setUp() {
        TagSet tags1 = new TagSet(asList(new Tag("foo", "1"),
                                         new Tag("bar", "2"),
                                         new Tag("baz", "1")));
        TagSet tags2 = new TagSet(asList(new Tag("foo", "1"),
                                         new Tag("bar", "2"),
                                         new Tag("baz", "2")));
        TagSet tags3 = new TagSet(asList(new Tag("foo", "1"),
                                         new Tag("bar", "3"),
                                         new Tag("baz", "3")));

        primary = builder()
                  .state(CONNECTED).address(new ServerAddress("localhost", 27017)).ok(true)
                  .type(REPLICA_SET_PRIMARY).tagSet(tags1)
                  .build();

        secondary = builder()
                    .state(CONNECTED).address(new ServerAddress("localhost", 27018)).ok(true)
                    .type(REPLICA_SET_SECONDARY).tagSet(tags2)
                    .build();

        otherSecondary = builder()
                         .state(CONNECTED).address(new ServerAddress("otherhost", 27019)).ok(true)
                         .type(REPLICA_SET_SECONDARY).tagSet(tags3)
                         .build();
        uninitiatedMember = builder()
                            .state(CONNECTED).address(new ServerAddress("localhost", 27020)).ok(true)
                            .type(REPLICA_SET_OTHER)
                            .build();

        notOkMember = builder().state(CONNECTED).address(new ServerAddress("localhost", 27021)).ok(false)
                               .build();

        List<ServerDescription> nodeList = asList(primary, secondary, otherSecondary, uninitiatedMember, notOkMember);

        cluster = new ClusterDescription(MULTIPLE, REPLICA_SET, nodeList);
    }

    @Test
    public void testMode() {
        ClusterDescription description = new ClusterDescription(MULTIPLE, UNKNOWN, Collections.emptyList());
        assertEquals(MULTIPLE, description.getConnectionMode());
    }

    @Test
    public void testSettings() {
        ClusterDescription description = new ClusterDescription(MULTIPLE, UNKNOWN, Collections.emptyList());
        assertNull(description.getClusterSettings());
        assertNull(description.getServerSettings());

        ClusterDescription descriptionWithSettings = new ClusterDescription(MULTIPLE, UNKNOWN, Collections.emptyList(),
                                                                                   ClusterSettings.builder()
                                                                                           .hosts(asList(new ServerAddress()))
                                                                                           .build(),
                                                                                   ServerSettings.builder().build());
        assertNotNull(descriptionWithSettings.getClusterSettings());
        assertNotNull(descriptionWithSettings.getServerSettings());
    }

    @Test
    public void testAll() {
        ClusterDescription description = new ClusterDescription(MULTIPLE, UNKNOWN, Collections.emptyList());
        assertTrue(getAll(description).isEmpty());
        assertEquals(new HashSet<>(asList(primary, secondary, otherSecondary, uninitiatedMember, notOkMember)),
                     getAll(cluster));
    }

    @Test
    public void testAny() {
        List<ServerDescription> any = getAny(cluster);
        assertEquals(4, any.size());
        assertTrue(any.contains(primary));
        assertTrue(any.contains(secondary));
        assertTrue(any.contains(uninitiatedMember));
        assertTrue(any.contains(otherSecondary));
    }

    @Test
    public void testPrimaryOrSecondary() {
        assertEquals(asList(primary, secondary, otherSecondary), getAnyPrimaryOrSecondary(cluster));
        assertEquals(asList(primary, secondary), getAnyPrimaryOrSecondary(cluster, new TagSet(asList(new Tag("foo", "1"),
                new Tag("bar", "2")))));
    }

    @Test
    public void testHasReadableServer() {
        assertTrue(cluster.hasReadableServer(ReadPreference.primary()));
        assertFalse(new ClusterDescription(MULTIPLE, REPLICA_SET, asList(secondary, otherSecondary))
                            .hasReadableServer(ReadPreference.primary()));
        assertTrue(new ClusterDescription(MULTIPLE, REPLICA_SET, asList(secondary, otherSecondary))
                            .hasReadableServer(ReadPreference.secondary()));

    }

    @Test
    public void testHasWritableServer() {
        assertTrue(cluster.hasWritableServer());
        assertFalse(new ClusterDescription(MULTIPLE, REPLICA_SET, asList(secondary, otherSecondary))
                            .hasWritableServer());
    }

    @Test
    public void testGetByServerAddress() {
        assertEquals(primary, ClusterDescriptionHelper.getByServerAddress(cluster, primary.getAddress()));
        assertNull(ClusterDescriptionHelper.getByServerAddress(cluster, notOkMember.getAddress()));
    }

    @Test
    public void testSortingOfAll() {
        ClusterDescription description =
        new ClusterDescription(MULTIPLE, UNKNOWN, asList(
                                                        builder()
                                                        .state(CONNECTING)
                                                        .address(new ServerAddress("loc:27019"))
                                                        .build(),
                                                        builder()
                                                        .state(CONNECTING)
                                                        .address(new ServerAddress("loc:27018"))
                                                        .build(),
                                                        builder()
                                                        .state(CONNECTING)
                                                        .address(new ServerAddress("loc:27017"))
                                                        .build())
        );
        Iterator<ServerDescription> iter = getAll(description).iterator();
        assertEquals(new ServerAddress("loc:27017"), iter.next().getAddress());
        assertEquals(new ServerAddress("loc:27018"), iter.next().getAddress());
        assertEquals(new ServerAddress("loc:27019"), iter.next().getAddress());
    }

    @Test
    public void clusterDescriptionWithAnIncompatiblyNewServerShouldBeIncompatible() {
        ClusterDescription description =
        new ClusterDescription(MULTIPLE, UNKNOWN, asList(
                                                        builder()
                                                        .state(CONNECTING)
                                                        .address(new ServerAddress("loc:27019"))
                                                        .build(),
                                                        builder()
                                                        .state(CONNECTED)
                                                        .ok(true)
                                                        .address(new ServerAddress("loc:27018"))
                                                        .minWireVersion(MAX_DRIVER_WIRE_VERSION + 1)
                                                        .maxWireVersion(MAX_DRIVER_WIRE_VERSION + 1)
                                                        .build(),
                                                        builder()
                                                        .state(CONNECTING)
                                                        .address(new ServerAddress("loc:27017"))
                                                        .build())
        );
        assertFalse(description.isCompatibleWithDriver());
        assertEquals(new ServerAddress("loc:27018"), description.findServerIncompatiblyNewerThanDriver().getAddress());
        assertNull(description.findServerIncompatiblyOlderThanDriver());
    }

    @Test
    public void clusterDescriptionWithAnIncompatiblyOlderServerShouldBeIncompatible() {
        ClusterDescription description =
                new ClusterDescription(MULTIPLE, UNKNOWN, asList(
                        builder()
                                .state(CONNECTING)
                                .address(new ServerAddress("loc:27019"))
                                .build(),
                        builder()
                                .state(CONNECTED)
                                .ok(true)
                                .address(new ServerAddress("loc:27018"))
                                .minWireVersion(0)
                                .maxWireVersion(MIN_DRIVER_WIRE_VERSION - 1)
                                .build(),
                        builder()
                                .state(CONNECTING)
                                .address(new ServerAddress("loc:27017"))
                                .build())
                );
        assertFalse(description.isCompatibleWithDriver());
        assertEquals(new ServerAddress("loc:27018"), description.findServerIncompatiblyOlderThanDriver().getAddress());
        assertNull(description.findServerIncompatiblyNewerThanDriver());
    }

    @Test
    public void clusterDescriptionWithCompatibleServerShouldBeCompatible() {
        ClusterDescription description =
        new ClusterDescription(MULTIPLE, UNKNOWN, asList(
                                                        builder()
                                                        .state(CONNECTING)
                                                        .address(new ServerAddress("loc:27019"))
                                                        .build(),
                                                        builder()
                                                        .state(CONNECTING)
                                                        .address(new ServerAddress("loc:27018"))
                                                        .build(),
                                                        builder()
                                                        .state(CONNECTING)
                                                        .address(new ServerAddress("loc:27017"))
                                                        .build())
        );
        assertTrue(description.isCompatibleWithDriver());
        assertNull(description.findServerIncompatiblyNewerThanDriver());
        assertNull(description.findServerIncompatiblyOlderThanDriver());
    }

    @Test
    public void testLogicalSessionTimeoutMinutes() {
        ClusterDescription description = new ClusterDescription(MULTIPLE, REPLICA_SET, asList(
                builder().state(CONNECTING)
                        .address(new ServerAddress("loc:27017")).build()
        ));
        assertNull(description.getLogicalSessionTimeoutMinutes());

        description = new ClusterDescription(MULTIPLE, REPLICA_SET, asList(
                builder().state(CONNECTED)
                        .address(new ServerAddress("loc:27017"))
                        .build()
        ));
        assertNull(description.getLogicalSessionTimeoutMinutes());

        description = new ClusterDescription(MULTIPLE, REPLICA_SET, asList(
                builder().state(CONNECTED)
                        .ok(true)
                        .address(new ServerAddress("loc:27017"))
                        .build()
        ));
        assertNull(description.getLogicalSessionTimeoutMinutes());

        description = new ClusterDescription(SINGLE, STANDALONE, asList(
                builder().state(CONNECTED)
                        .ok(true)
                        .address(new ServerAddress("loc:27017"))
                        .type(ServerType.STANDALONE)
                        .logicalSessionTimeoutMinutes(5)
                        .build()
        ));
        assertEquals(Integer.valueOf(5), description.getLogicalSessionTimeoutMinutes());

        description = new ClusterDescription(SINGLE, SHARDED, asList(
                builder().state(CONNECTED)
                        .ok(true)
                        .address(new ServerAddress("loc:27017"))
                        .type(ServerType.SHARD_ROUTER)
                        .logicalSessionTimeoutMinutes(5)
                        .build()
        ));
        assertEquals(Integer.valueOf(5), description.getLogicalSessionTimeoutMinutes());

        description = new ClusterDescription(MULTIPLE, SHARDED, asList(
                builder().state(CONNECTED)
                        .ok(true)
                        .address(new ServerAddress("loc:27017"))
                        .type(ServerType.SHARD_ROUTER)
                        .logicalSessionTimeoutMinutes(5)
                        .build(),
                builder().state(CONNECTING)
                        .address(new ServerAddress("loc:27018"))
                        .build()
        ));
        assertEquals(Integer.valueOf(5), description.getLogicalSessionTimeoutMinutes());

        description = new ClusterDescription(MULTIPLE, SHARDED, asList(
                builder().state(CONNECTED)
                        .ok(true)
                        .address(new ServerAddress("loc:27017"))
                        .type(ServerType.SHARD_ROUTER)
                        .logicalSessionTimeoutMinutes(5)
                        .build(),
                builder().state(CONNECTED)
                        .ok(true)
                        .address(new ServerAddress("loc:27018"))
                        .type(ServerType.SHARD_ROUTER)
                        .logicalSessionTimeoutMinutes(3)
                        .build()
        ));
        assertEquals(Integer.valueOf(3), description.getLogicalSessionTimeoutMinutes());

        description = new ClusterDescription(MULTIPLE, REPLICA_SET, asList(
                builder().state(CONNECTED)
                        .ok(true)
                        .address(new ServerAddress("loc:27017"))
                        .type(REPLICA_SET_PRIMARY)
                        .logicalSessionTimeoutMinutes(5)
                        .build(),
                builder().state(CONNECTING)
                        .address(new ServerAddress("loc:27018"))
                        .build()
        ));
        assertEquals(Integer.valueOf(5), description.getLogicalSessionTimeoutMinutes());

        description = new ClusterDescription(MULTIPLE, REPLICA_SET, asList(
                builder().state(CONNECTED)
                        .ok(true)
                        .address(new ServerAddress("loc:27017"))
                        .type(REPLICA_SET_PRIMARY)
                        .logicalSessionTimeoutMinutes(5)
                        .build(),
                builder().state(CONNECTED)
                        .ok(true)
                        .address(new ServerAddress("loc:27018"))
                        .type(REPLICA_SET_SECONDARY)
                        .logicalSessionTimeoutMinutes(3)
                        .build(),
                builder().state(CONNECTING)
                        .address(new ServerAddress("loc:27019"))
                        .build()
        ));
        assertEquals(Integer.valueOf(3), description.getLogicalSessionTimeoutMinutes());

        description = new ClusterDescription(MULTIPLE, REPLICA_SET, asList(
                builder().state(CONNECTED)
                        .ok(true)
                        .address(new ServerAddress("loc:27017"))
                        .type(REPLICA_SET_PRIMARY)
                        .logicalSessionTimeoutMinutes(3)
                        .build(),
                builder().state(CONNECTED)
                        .ok(true)
                        .address(new ServerAddress("loc:27018"))
                        .type(REPLICA_SET_SECONDARY)
                        .logicalSessionTimeoutMinutes(5)
                        .build(),
                builder().state(CONNECTING)
                        .address(new ServerAddress("loc:27019"))
                        .build()
        ));
        assertEquals(Integer.valueOf(3), description.getLogicalSessionTimeoutMinutes());
    }

    @Test
    public void testObjectOverrides() {
        ClusterDescription description =
        new ClusterDescription(MULTIPLE, UNKNOWN, asList(
                                                        builder()
                                                        .state(CONNECTING)
                                                        .address(new ServerAddress("loc:27019"))
                                                        .lastUpdateTimeNanos(42L)
                                                        .build(),
                                                        builder()
                                                        .state(CONNECTING)
                                                        .address(new ServerAddress("loc:27018"))
                                                        .lastUpdateTimeNanos(42L)
                                                        .build(),
                                                        builder()
                                                        .state(CONNECTING)
                                                        .address(new ServerAddress("loc:27017"))
                                                        .lastUpdateTimeNanos(42L)
                                                        .build())
        );
        ClusterDescription descriptionTwo =
        new ClusterDescription(MULTIPLE, UNKNOWN, asList(
                                                        builder()
                                                        .state(CONNECTING)
                                                        .address(new ServerAddress("loc:27019"))
                                                        .lastUpdateTimeNanos(42L)
                                                        .build(),
                                                        builder()
                                                        .state(CONNECTING)
                                                        .address(new ServerAddress("loc:27018"))
                                                        .lastUpdateTimeNanos(42L)
                                                        .build(),
                                                        builder()
                                                        .state(CONNECTING)
                                                        .address(new ServerAddress("loc:27017"))
                                                        .lastUpdateTimeNanos(42L)
                                                        .build())
        );
        assertEquals(description, descriptionTwo);
        assertEquals(description.hashCode(), descriptionTwo.hashCode());
        assertTrue(description.toString().startsWith("ClusterDescription"));
    }
}
