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

package com.mongodb;


import org.junit.Test;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static com.mongodb.ServerConnectionState.Connected;
import static com.mongodb.ServerConnectionState.Connecting;
import static com.mongodb.ServerDescription.MAX_DRIVER_WIRE_VERSION;
import static com.mongodb.ServerDescription.MIN_DRIVER_WIRE_VERSION;
import static com.mongodb.ServerType.ReplicaSetPrimary;
import static com.mongodb.ServerType.Unknown;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ServerDescriptionTest {

    @Test(expected =  IllegalArgumentException.class)
    public void testMissingStatus() throws UnknownHostException {
        ServerDescription.builder().address(new ServerAddress()).type(ReplicaSetPrimary).build();

    }

    @Test(expected =  IllegalArgumentException.class)
    public void testMissingAddress() throws UnknownHostException {
        ServerDescription.builder().state(Connected).type(ReplicaSetPrimary).build();

    }

    @Test
    public void testDefaults() throws UnknownHostException {
        ServerDescription serverDescription = ServerDescription.builder().address(new ServerAddress())
                                                               .state(Connected)
                                                               .build();

        assertEquals(new ServerAddress(), serverDescription.getAddress());
        assertFalse(serverDescription.isOk());
        assertEquals(Connected, serverDescription.getState());
        assertEquals(Unknown, serverDescription.getType());

        assertFalse(serverDescription.isReplicaSetMember());
        assertFalse(serverDescription.isShardRouter());
        assertFalse(serverDescription.isStandAlone());

        assertFalse(serverDescription.isPrimary());
        assertFalse(serverDescription.isSecondary());

        assertEquals(0F, serverDescription.getAveragePingTimeNanos(), 0L);

        assertEquals(0x1000000, serverDescription.getMaxDocumentSize());
        assertEquals(0x2000000, serverDescription.getMaxMessageSize());
        assertEquals(512, serverDescription.getMaxWriteBatchSize());

        assertNull(serverDescription.getPrimary());
        assertEquals(Collections.<String>emptySet(), serverDescription.getHosts());
        assertEquals(Tags.freeze(new Tags()), serverDescription.getTags());
        assertEquals(Collections.<String>emptySet(), serverDescription.getHosts());
        assertEquals(Collections.<String>emptySet(), serverDescription.getPassives());
        assertNull(serverDescription.getSetName());
        assertNull(serverDescription.getSetVersion());
        assertEquals(new ServerVersion(), serverDescription.getVersion());
        assertEquals(0, serverDescription.getMinWireVersion());
        assertEquals(0, serverDescription.getMaxWireVersion());
    }

    @Test
    public void testObjectOverrides() throws UnknownHostException {
        ServerDescription.Builder builder = ServerDescription.builder()
                                                             .address(new ServerAddress())
                                                             .type(ServerType.ShardRouter)
                                                             .tags(new Tags("dc", "ny"))
                                                             .setName("test")
                                                             .setVersion(11)
                                                             .maxDocumentSize(100)
                                                             .maxMessageSize(200)
                                                             .maxWriteBatchSize(1024)
                                                             .averagePingTime(50000, java.util.concurrent.TimeUnit.NANOSECONDS)
                                                             .primary("localhost:27017")
                                                             .hosts(new HashSet<String>(Arrays.asList("localhost:27017",
                                                                                                      "localhost:27018")))
                                                             .passives(new HashSet<String>(Arrays.asList("localhost:27019")))
                                                             .ok(true)
                                                             .state(Connected)
                                                             .version(new ServerVersion(Arrays.asList(2, 4, 1)))
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
                                                               .type(ServerType.ShardRouter)
                                                               .ok(false)
                                                               .state(Connected)
                                                               .build();
        assertFalse(serverDescription.isPrimary());
        assertFalse(serverDescription.isSecondary());

        serverDescription = ServerDescription.builder()
                                             .address(new ServerAddress())
                                             .type(ServerType.ShardRouter)
                                             .ok(true)
                                             .state(Connected)
                                             .build();
        assertTrue(serverDescription.isPrimary());
        assertTrue(serverDescription.isSecondary());

        serverDescription = ServerDescription.builder()
                                             .address(new ServerAddress())
                                             .type(ServerType.StandAlone)
                                             .ok(true)
                                             .state(Connected)
                                             .build();
        assertTrue(serverDescription.isPrimary());
        assertTrue(serverDescription.isSecondary());

        serverDescription = ServerDescription.builder()
                                             .address(new ServerAddress())
                                             .type(ReplicaSetPrimary)
                                             .ok(true)
                                             .state(Connected)
                                             .build();
        assertTrue(serverDescription.isPrimary());
        assertFalse(serverDescription.isSecondary());

        serverDescription = ServerDescription.builder()
                                             .address(new ServerAddress())
                                             .type(ServerType.ReplicaSetSecondary)
                                             .ok(true)
                                             .state(Connected)
                                             .build();
        assertFalse(serverDescription.isPrimary());
        assertTrue(serverDescription.isSecondary());
    }

    @Test
    public void testHasTags() throws UnknownHostException {
        ServerDescription serverDescription = ServerDescription.builder()
                                                               .address(new ServerAddress())
                                                               .type(ServerType.ShardRouter)
                                                               .ok(false)
                                                               .state(Connected)
                                                               .build();
        assertFalse(serverDescription.hasTags(new Tags("dc", "ny")));

        serverDescription = ServerDescription.builder()
                                             .address(new ServerAddress())
                                             .type(ServerType.ShardRouter)
                                             .ok(true)
                                             .state(Connected)
                                             .build();
        assertTrue(serverDescription.hasTags(new Tags("dc", "ny")));

        serverDescription = ServerDescription.builder()
                                             .address(new ServerAddress())
                                             .type(ServerType.StandAlone)
                                             .ok(true)
                                             .state(Connected)
                                             .build();
        assertTrue(serverDescription.hasTags(new Tags("dc", "ny")));

        serverDescription = ServerDescription.builder()
                                             .address(new ServerAddress())
                                             .type(ReplicaSetPrimary)
                                             .ok(true)
                                             .state(Connected)
                                             .build();
        assertTrue(serverDescription.hasTags(new Tags()));

        serverDescription = ServerDescription.builder()
                                             .address(new ServerAddress())
                                             .type(ReplicaSetPrimary)
                                             .ok(true)
                                             .tags(new Tags("rack", "1"))
                                             .state(Connected)
                                             .build();
        assertFalse(serverDescription.hasTags(new Tags("dc", "ny")));

        serverDescription = ServerDescription.builder()
                                             .address(new ServerAddress())
                                             .type(ReplicaSetPrimary)
                                             .ok(true)
                                             .tags(new Tags("rack", "1"))
                                             .state(Connected)
                                             .build();
        assertFalse(serverDescription.hasTags(new Tags("rack", "2")));

        serverDescription = ServerDescription.builder()
                                             .address(new ServerAddress())
                                             .type(ReplicaSetPrimary)
                                             .ok(true)
                                             .tags(new Tags("rack", "1"))
                                             .state(Connected)
                                             .build();
        assertTrue(serverDescription.hasTags(new Tags("rack", "1")));
    }

    @Test
    public void notOkServerShouldBeCompatible() throws UnknownHostException {
        assertTrue(ServerDescription.builder()
                                    .address(new ServerAddress())
                                    .state(Connecting)
                                    .ok(false)
                                    .build()
                                    .isCompatibleWithDriver());
    }

    @Test
    public void serverWithMinWireVersionEqualToDriverMaxWireVersionShouldBeCompatible() throws UnknownHostException {
        assertTrue(ServerDescription.builder()
                                    .address(new ServerAddress())
                                    .state(Connecting)
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
                                    .state(Connecting)
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
                                     .state(Connecting)
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
                                     .state(Connecting)
                                     .ok(true)
                                     .minWireVersion(MIN_DRIVER_WIRE_VERSION - 1)
                                     .maxWireVersion(MIN_DRIVER_WIRE_VERSION - 1)
                                     .build()
                                     .isCompatibleWithDriver());

    }

}