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

import com.mongodb.ServerAddress;
import com.mongodb.Tag;
import com.mongodb.TagSet;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashSet;

import static com.mongodb.connection.ServerConnectionState.CONNECTED;
import static com.mongodb.connection.ServerConnectionState.CONNECTING;
import static com.mongodb.connection.ServerDescription.MAX_DRIVER_WIRE_VERSION;
import static com.mongodb.connection.ServerDescription.MIN_DRIVER_WIRE_VERSION;
import static com.mongodb.connection.ServerType.REPLICA_SET_PRIMARY;
import static com.mongodb.connection.ServerType.UNKNOWN;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ServerDescriptionTest {

    @Test(expected = IllegalArgumentException.class)
    public void testMissingStatus() throws UnknownHostException {
        ServerDescription.builder().address(new ServerAddress()).type(REPLICA_SET_PRIMARY).build();

    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingAddress() throws UnknownHostException {
        ServerDescription.builder().state(CONNECTED).type(REPLICA_SET_PRIMARY).build();

    }

    @Test
    public void testDefaults() throws UnknownHostException {
        ServerDescription serverDescription = ServerDescription.builder().address(new ServerAddress())
                                                               .state(CONNECTED)
                                                               .build();

        assertEquals(new ServerAddress(), serverDescription.getAddress());
        assertFalse(serverDescription.isOk());
        assertEquals(CONNECTED, serverDescription.getState());
        assertEquals(UNKNOWN, serverDescription.getType());

        assertFalse(serverDescription.isReplicaSetMember());
        assertFalse(serverDescription.isShardRouter());
        assertFalse(serverDescription.isStandAlone());

        assertFalse(serverDescription.isPrimary());
        assertFalse(serverDescription.isSecondary());

        assertEquals(0F, serverDescription.getRoundTripTimeNanos(), 0L);

        assertEquals(0x1000000, serverDescription.getMaxDocumentSize());

        assertNull(serverDescription.getPrimary());
        assertEquals(Collections.<String>emptySet(), serverDescription.getHosts());
        assertEquals(new TagSet(), serverDescription.getTagSet());
        assertEquals(Collections.<String>emptySet(), serverDescription.getHosts());
        assertEquals(Collections.<String>emptySet(), serverDescription.getPassives());
        assertNull(serverDescription.getSetName());
        assertEquals(new ServerVersion(), serverDescription.getVersion());
        assertEquals(0, serverDescription.getMinWireVersion());
        assertEquals(0, serverDescription.getMaxWireVersion());
    }

    @Test
    public void testObjectOverrides() throws UnknownHostException {
        ServerDescription.Builder builder = ServerDescription.builder()
                                                             .address(new ServerAddress())
                                                             .type(ServerType.SHARD_ROUTER)
                                                             .tagSet(new TagSet(asList(new Tag("dc", "ny"))))
                                                                     .setName("test")
                                                                     .maxDocumentSize(100)
                                                                     .roundTripTime(50000, java.util.concurrent.TimeUnit.NANOSECONDS)
                                                                     .primary("localhost:27017")
                                                                     .hosts(new HashSet<String>(asList("localhost:27017",
                                                                                                       "localhost:27018")))
                                                                     .passives(new HashSet<String>(asList("localhost:27019")))
                                                                     .ok(true)
                                                                     .state(CONNECTED)
                                                                     .version(new ServerVersion(asList(2, 4, 1)))
                                                                     .minWireVersion(1)
                                                                     .maxWireVersion(2);
        assertEquals(builder.build(), builder.build());
        assertEquals(builder.build().hashCode(), builder.build().hashCode());
        assertTrue(builder.build().toString().startsWith("ServerDescription"));
    }

    @Test
    public void testIsPrimaryAndIsSecondary() throws UnknownHostException {
        ServerDescription serverDescription = ServerDescription.builder()
                                                               .address(new ServerAddress())
                                                               .type(ServerType.SHARD_ROUTER)
                                                               .ok(false)
                                                               .state(CONNECTED)
                                                               .build();
        assertFalse(serverDescription.isPrimary());
        assertFalse(serverDescription.isSecondary());

        serverDescription = ServerDescription.builder()
                                             .address(new ServerAddress())
                                             .type(ServerType.SHARD_ROUTER)
                                             .ok(true)
                                             .state(CONNECTED)
                                             .build();
        assertTrue(serverDescription.isPrimary());
        assertTrue(serverDescription.isSecondary());

        serverDescription = ServerDescription.builder()
                                             .address(new ServerAddress())
                                             .type(ServerType.STANDALONE)
                                             .ok(true)
                                             .state(CONNECTED)
                                             .build();
        assertTrue(serverDescription.isPrimary());
        assertTrue(serverDescription.isSecondary());

        serverDescription = ServerDescription.builder()
                                             .address(new ServerAddress())
                                             .type(REPLICA_SET_PRIMARY)
                                             .ok(true)
                                             .state(CONNECTED)
                                             .build();
        assertTrue(serverDescription.isPrimary());
        assertFalse(serverDescription.isSecondary());

        serverDescription = ServerDescription.builder()
                                             .address(new ServerAddress())
                                             .type(ServerType.REPLICA_SET_SECONDARY)
                                             .ok(true)
                                             .state(CONNECTED)
                                             .build();
        assertFalse(serverDescription.isPrimary());
        assertTrue(serverDescription.isSecondary());
    }

    @Test
    public void testHasTags() throws UnknownHostException {
        ServerDescription serverDescription = ServerDescription.builder()
                                                               .address(new ServerAddress())
                                                               .type(ServerType.SHARD_ROUTER)
                                                               .ok(false)
                                                               .state(CONNECTED)
                                                               .build();
        assertFalse(serverDescription.hasTags(new TagSet(asList(new Tag("dc", "ny")))));

        serverDescription = ServerDescription.builder()
                                             .address(new ServerAddress())
                                             .type(ServerType.SHARD_ROUTER)
                                             .ok(true)
                                             .state(CONNECTED)
                                             .build();
        assertTrue(serverDescription.hasTags(new TagSet(asList(new Tag("dc", "ny")))));

        serverDescription = ServerDescription.builder()
                                             .address(new ServerAddress())
                                             .type(ServerType.STANDALONE)
                                             .ok(true)
                                             .state(CONNECTED)
                                             .build();
        assertTrue(serverDescription.hasTags(new TagSet(asList(new Tag("dc", "ny")))));

        serverDescription = ServerDescription.builder()
                                             .address(new ServerAddress())
                                             .type(REPLICA_SET_PRIMARY)
                                             .ok(true)
                                             .state(CONNECTED)
                                             .build();
        assertTrue(serverDescription.hasTags(new TagSet()));

        serverDescription = ServerDescription.builder()
                                             .address(new ServerAddress())
                                             .type(REPLICA_SET_PRIMARY)
                                             .ok(true)
                                             .tagSet(new TagSet(asList(new Tag("dc", "ca"))))
                                             .state(CONNECTED)
                                             .build();
        assertFalse(serverDescription.hasTags(new TagSet(asList(new Tag("dc", "ny")))));

        serverDescription = ServerDescription.builder()
                                             .address(new ServerAddress())
                                             .type(REPLICA_SET_PRIMARY)
                                             .ok(true)
                                             .tagSet(new TagSet(asList(new Tag("rack", "1"))))
                                             .state(CONNECTED)
                                             .build();
        assertFalse(serverDescription.hasTags(new TagSet(asList(new Tag("rack", "2")))));

        serverDescription = ServerDescription.builder()
                                             .address(new ServerAddress())
                                             .type(REPLICA_SET_PRIMARY)
                                             .ok(true)
                                             .tagSet(new TagSet(asList(new Tag("rack", "1"))))
                                             .state(CONNECTED)
                                             .build();
        assertTrue(serverDescription.hasTags(new TagSet(asList(new Tag("rack", "1")))));
    }

    @Test
    public void notOkServerShouldBeCompatible() throws UnknownHostException {
        assertTrue(ServerDescription.builder()
                                    .address(new ServerAddress())
                                    .state(CONNECTING)
                                    .ok(false)
                                    .build()
                                    .isCompatibleWithDriver());
    }

    @Test
    public void serverWithMinWireVersionEqualToDriverMaxWireVersionShouldBeCompatible() throws UnknownHostException {
        assertTrue(ServerDescription.builder()
                                    .address(new ServerAddress())
                                    .state(CONNECTING)
                                    .ok(true)
                                    .minWireVersion(MAX_DRIVER_WIRE_VERSION)
                                    .maxWireVersion(MAX_DRIVER_WIRE_VERSION + 1)
                                    .build()
                                    .isCompatibleWithDriver());

    }

    @Test
    public void serverWithMaxWireVersionEqualToDriverMinWireVersionShouldBeCompatible() throws UnknownHostException {
        assertTrue(ServerDescription.builder()
                                    .address(new ServerAddress())
                                    .state(CONNECTING)
                                    .ok(true)
                                    .minWireVersion(MIN_DRIVER_WIRE_VERSION - 1)
                                    .maxWireVersion(MIN_DRIVER_WIRE_VERSION)
                                    .build()
                                    .isCompatibleWithDriver());

    }

    @Test
    public void serverWithMinWireVersionGreaterThanDriverMaxWireVersionShouldBeIncompatible() throws UnknownHostException {
        assertFalse(ServerDescription.builder()
                                     .address(new ServerAddress())
                                     .state(CONNECTING)
                                     .ok(true)
                                     .minWireVersion(MAX_DRIVER_WIRE_VERSION + 1)
                                     .maxWireVersion(MAX_DRIVER_WIRE_VERSION + 1)
                                     .build()
                                     .isCompatibleWithDriver());

    }

    @Test
    public void serverWithMaxWireVersionLessThanDriverMinWireVersionShouldBeIncompatible() throws UnknownHostException {
        assertFalse(ServerDescription.builder()
                                     .address(new ServerAddress())
                                     .state(CONNECTING)
                                     .ok(true)
                                     .minWireVersion(MIN_DRIVER_WIRE_VERSION - 1)
                                     .maxWireVersion(MIN_DRIVER_WIRE_VERSION - 1)
                                     .build()
                                     .isCompatibleWithDriver());

    }
}
