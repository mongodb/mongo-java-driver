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


import org.bson.types.ObjectId;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ServerConnectionState.Connected;
import static com.mongodb.ServerConnectionState.Connecting;
import static com.mongodb.ServerDescription.MAX_DRIVER_WIRE_VERSION;
import static com.mongodb.ServerDescription.MIN_DRIVER_WIRE_VERSION;
import static com.mongodb.ServerDescription.builder;
import static com.mongodb.ServerType.ReplicaSetPrimary;
import static com.mongodb.ServerType.Unknown;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ServerDescriptionTest {

    @Test(expected = IllegalArgumentException.class)
    public void testMissingStatus() throws UnknownHostException {
        builder().address(new ServerAddress()).type(ReplicaSetPrimary).build();

    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingAddress() throws UnknownHostException {
        builder().state(Connected).type(ReplicaSetPrimary).build();

    }

    @Test
    public void testDefaults() throws UnknownHostException {
        ServerDescription serverDescription = builder().address(new ServerAddress())
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

        assertEquals(0F, serverDescription.getAverageLatencyNanos(), 0L);

        assertEquals(0x1000000, serverDescription.getMaxDocumentSize());
        assertEquals(0x2000000, serverDescription.getMaxMessageSize());
        assertEquals(512, serverDescription.getMaxWriteBatchSize());

        assertNull(serverDescription.getPrimary());
        assertEquals(Collections.<String>emptySet(), serverDescription.getHosts());
        assertEquals(new TagSet(), serverDescription.getTagSet());
        assertNull(serverDescription.getCanonicalAddress());
        assertEquals(Collections.<String>emptySet(), serverDescription.getHosts());
        assertEquals(Collections.<String>emptySet(), serverDescription.getPassives());
        assertNull(serverDescription.getSetName());
        assertEquals(new ServerVersion(), serverDescription.getVersion());
        assertEquals(0, serverDescription.getMinWireVersion());
        assertEquals(0, serverDescription.getMaxWireVersion());
        assertNull(serverDescription.getElectionId());
        assertNull(serverDescription.getException());
    }

    @Test
    public void testBuilder() throws UnknownHostException {
        IllegalArgumentException exception = new IllegalArgumentException();
        ServerDescription serverDescription = builder()
                                              .address(new ServerAddress("localhost:27018"))
                                              .type(ServerType.ReplicaSetPrimary)
                                              .tagSet(new TagSet(new Tag("dc", "ny")))
                                              .setName("test")
                                              .maxDocumentSize(100)
                                              .maxMessageSize(200)
                                              .maxWriteBatchSize(1024)
                                              .averageLatency(50000, java.util.concurrent.TimeUnit.NANOSECONDS)
                                              .primary("localhost:27017")
                                              .canonicalAddress("localhost:27018")
                                              .hosts(new HashSet<String>(asList("localhost:27017",
                                                                                "localhost:27018",
                                                                                "localhost:27019",
                                                                                "localhost:27020")))
                                              .arbiters(new HashSet<String>(asList("localhost:27019")))
                                              .passives(new HashSet<String>(asList("localhost:27020")))
                                              .ok(true)
                                              .state(Connected)
                                              .version(new ServerVersion(asList(2, 4, 1)))
                                              .minWireVersion(1)
                                              .maxWireVersion(2)
                                              .electionId(new ObjectId("123412341234123412341234"))
                                              .exception(exception)
                                              .build();


        assertEquals(new ServerAddress("localhost:27018"), serverDescription.getAddress());
        assertTrue(serverDescription.isOk());
        assertEquals(Connected, serverDescription.getState());
        assertEquals(ReplicaSetPrimary, serverDescription.getType());

        assertTrue(serverDescription.isReplicaSetMember());
        assertFalse(serverDescription.isShardRouter());
        assertFalse(serverDescription.isStandAlone());

        assertTrue(serverDescription.isPrimary());
        assertFalse(serverDescription.isSecondary());

        assertEquals(50000, serverDescription.getAverageLatencyNanos(), 0L);

        assertEquals(100, serverDescription.getMaxDocumentSize());
        assertEquals(200, serverDescription.getMaxMessageSize());
        assertEquals(1024, serverDescription.getMaxWriteBatchSize());

        assertEquals("localhost:27017", serverDescription.getPrimary());
        assertEquals("localhost:27018", serverDescription.getCanonicalAddress());
        assertEquals(new HashSet<String>(asList("localhost:27017", "localhost:27018", "localhost:27019", "localhost:27020")),
                     serverDescription.getHosts());
        assertEquals(new TagSet(new Tag("dc", "ny")), serverDescription.getTagSet());
        assertEquals(new HashSet<String>(asList("localhost:27019")), serverDescription.getArbiters());
        assertEquals(new HashSet<String>(asList("localhost:27020")), serverDescription.getPassives());
        assertEquals("test", serverDescription.getSetName());
        assertEquals(new ServerVersion(asList(2, 4, 1)), serverDescription.getVersion());
        assertEquals(1, serverDescription.getMinWireVersion());
        assertEquals(2, serverDescription.getMaxWireVersion());
        assertEquals(new ObjectId("123412341234123412341234"), serverDescription.getElectionId());
        assertEquals(exception, serverDescription.getException());
    }

    @Test
    public void testObjectOverrides() throws UnknownHostException {
        ServerDescription.Builder builder = builder()
                                            .address(new ServerAddress())
                                            .type(ServerType.ShardRouter)
                                            .tagSet(new TagSet(new Tag("dc", "ny")))
                                            .setName("test")
                                            .maxDocumentSize(100)
                                            .maxMessageSize(200)
                                            .maxWriteBatchSize(1024)
                                            .averageLatency(50000, java.util.concurrent.TimeUnit.NANOSECONDS)
                                            .primary("localhost:27017")
                                            .canonicalAddress("localhost:27017")
                                            .hosts(new HashSet<String>(asList("localhost:27017",
                                                                              "localhost:27018")))
                                            .passives(new HashSet<String>(asList("localhost:27019")))
                                            .ok(true)
                                            .state(Connected)
                                            .version(new ServerVersion(asList(2, 4, 1)))
                                            .minWireVersion(1)
                                            .maxWireVersion(2)
                                            .electionId(new ObjectId())
                                            .exception(new IllegalArgumentException("This is illegal"));
        assertEquals(builder.build(), builder.build());
        assertEquals(builder.build().hashCode(), builder.build().hashCode());
        assertTrue(builder.build().toString().startsWith("ServerDescription"));
    }

    @Test
    public void testObjectOverridesWithUnequalException() throws UnknownHostException {
        ServerDescription.Builder builder1 = builder()
                                             .state(Connecting)
                                             .address(new ServerAddress())
                                             .exception(new IllegalArgumentException("This is illegal"));
        ServerDescription.Builder builder2 = builder()
                                             .state(Connecting)
                                             .address(new ServerAddress())
                                             .exception(new IllegalArgumentException("This is also illegal"));

        ServerDescription.Builder builder3 = builder()
                                             .state(Connecting)
                                             .address(new ServerAddress())
                                             .exception(new IllegalStateException("This is illegal"));
        ServerDescription.Builder builder4 = builder()
                                             .state(Connecting)
                                             .address(new ServerAddress());


        assertNotEquals(builder1.build(), builder2.build());
        assertNotEquals(builder1.build().hashCode(), builder2.build().hashCode());
        assertNotEquals(builder1.build(), builder3.build());
        assertNotEquals(builder1.build().hashCode(), builder3.build().hashCode());
        assertNotEquals(builder1.build(), builder4.build());
        assertNotEquals(builder1.build().hashCode(), builder4.build().hashCode());
        assertNotEquals(builder4.build(), builder3.build());
        assertNotEquals(builder4.build().hashCode(), builder3.build().hashCode());
    }

    @Test
    public void testShortDescription() throws UnknownHostException {
        assertEquals("{address=127.0.0.1:27017, type=Unknown, TagSet{[Tag{name='dc', value='ny'}, Tag{name='rack', value='1'}]}, " +
                     "averageLatency=5000.0 ms, state=Connected, exception={java.lang.IllegalArgumentException: This is illegal}, " +
                     "caused by {java.lang.NullPointerException: This is null}}",
                     builder().state(Connected)
                              .address(new ServerAddress())
                              .averageLatency(5000, TimeUnit.MILLISECONDS)
                              .tagSet(new TagSet(asList(new Tag("dc", "ny"), new Tag("rack", "1"))))
                              .exception(new IllegalArgumentException("This is illegal", new NullPointerException("This is null")))
                              .build()
                              .getShortDescription());
    }

    @Test
    public void testIsPrimaryAndIsSecondary() throws UnknownHostException {
        ServerDescription serverDescription = builder()
                                              .address(new ServerAddress())
                                              .type(ServerType.ShardRouter)
                                              .ok(false)
                                              .state(Connected)
                                              .build();
        assertFalse(serverDescription.isPrimary());
        assertFalse(serverDescription.isSecondary());

        serverDescription = builder()
                            .address(new ServerAddress())
                            .type(ServerType.ShardRouter)
                            .ok(true)
                            .state(Connected)
                            .build();
        assertTrue(serverDescription.isPrimary());
        assertTrue(serverDescription.isSecondary());

        serverDescription = builder()
                            .address(new ServerAddress())
                            .type(ServerType.StandAlone)
                            .ok(true)
                            .state(Connected)
                            .build();
        assertTrue(serverDescription.isPrimary());
        assertTrue(serverDescription.isSecondary());

        serverDescription = builder()
                            .address(new ServerAddress())
                            .type(ReplicaSetPrimary)
                            .ok(true)
                            .state(Connected)
                            .build();
        assertTrue(serverDescription.isPrimary());
        assertFalse(serverDescription.isSecondary());

        serverDescription = builder()
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
        ServerDescription serverDescription = builder()
                                              .address(new ServerAddress())
                                              .type(ServerType.ShardRouter)
                                              .ok(false)
                                              .state(Connected)
                                              .build();
        assertFalse(serverDescription.hasTags(new TagSet(new Tag("dc", "ny"))));

        serverDescription = builder()
                            .address(new ServerAddress())
                            .type(ServerType.ShardRouter)
                            .ok(true)
                            .state(Connected)
                            .build();
        assertTrue(serverDescription.hasTags(new TagSet(new Tag("dc", "ny"))));

        serverDescription = builder()
                            .address(new ServerAddress())
                            .type(ServerType.StandAlone)
                            .ok(true)
                            .state(Connected)
                            .build();
        assertTrue(serverDescription.hasTags(new TagSet(new Tag("dc", "ny"))));

        serverDescription = builder()
                            .address(new ServerAddress())
                            .type(ReplicaSetPrimary)
                            .ok(true)
                            .state(Connected)
                            .build();
        assertTrue(serverDescription.hasTags(new TagSet()));

        serverDescription = builder()
                            .address(new ServerAddress())
                            .type(ReplicaSetPrimary)
                            .ok(true)
                            .tagSet(new TagSet(new Tag("rack", "1")))
                            .state(Connected)
                            .build();
        assertFalse(serverDescription.hasTags(new TagSet(new Tag("dc", "ny"))));

        serverDescription = builder()
                            .address(new ServerAddress())
                            .type(ReplicaSetPrimary)
                            .ok(true)
                            .tagSet(new TagSet(new Tag("rack", "1")))
                            .state(Connected)
                            .build();
        assertFalse(serverDescription.hasTags(new TagSet(new Tag("rack", "2"))));

        serverDescription = builder()
                            .address(new ServerAddress())
                            .type(ReplicaSetPrimary)
                            .ok(true)
                            .tagSet(new TagSet(new Tag("rack", "1")))
                            .state(Connected)
                            .build();
        assertTrue(serverDescription.hasTags(new TagSet(new Tag("rack", "1"))));
    }

    @Test
    public void notOkServerShouldBeCompatible() throws UnknownHostException {
        assertTrue(builder()
                   .address(new ServerAddress())
                   .state(Connecting)
                   .ok(false)
                   .build()
                   .isCompatibleWithDriver());
    }

    @Test
    public void serverWithMinWireVersionEqualToDriverMaxWireVersionShouldBeCompatible() throws UnknownHostException {
        assertTrue(builder()
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
        assertTrue(builder()
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
        assertFalse(builder()
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
        assertFalse(builder()
                    .address(new ServerAddress())
                    .state(Connecting)
                    .ok(true)
                    .minWireVersion(MIN_DRIVER_WIRE_VERSION - 1)
                    .maxWireVersion(MIN_DRIVER_WIRE_VERSION - 1)
                    .build()
                    .isCompatibleWithDriver());

    }

}
