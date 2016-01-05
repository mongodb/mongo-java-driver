/*
 * Copyright 2008-2016 MongoDB, Inc.
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
import org.bson.types.ObjectId;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import static com.mongodb.connection.ServerConnectionState.CONNECTED;
import static com.mongodb.connection.ServerConnectionState.CONNECTING;
import static com.mongodb.connection.ServerDescription.MAX_DRIVER_WIRE_VERSION;
import static com.mongodb.connection.ServerDescription.MIN_DRIVER_WIRE_VERSION;
import static com.mongodb.connection.ServerDescription.builder;
import static com.mongodb.connection.ServerType.REPLICA_SET_PRIMARY;
import static com.mongodb.connection.ServerType.UNKNOWN;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ServerDescriptionTest {

    @Test(expected = IllegalArgumentException.class)
    public void testMissingStatus() throws UnknownHostException {
        builder().address(new ServerAddress()).type(REPLICA_SET_PRIMARY).build();

    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingAddress() throws UnknownHostException {
        builder().state(CONNECTED).type(REPLICA_SET_PRIMARY).build();

    }

    @Test
    public void testDefaults() throws UnknownHostException {
        ServerDescription serverDescription = builder().address(new ServerAddress())
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
        assertNull(serverDescription.getCanonicalAddress());
        assertEquals(Collections.<String>emptySet(), serverDescription.getHosts());
        assertEquals(Collections.<String>emptySet(), serverDescription.getPassives());
        assertNull(serverDescription.getSetName());
        assertEquals(new ServerVersion(), serverDescription.getVersion());
        assertEquals(0, serverDescription.getMinWireVersion());
        assertEquals(0, serverDescription.getMaxWireVersion());
        assertNull(serverDescription.getElectionId());
        assertNull(serverDescription.getSetVersion());
        assertNull(serverDescription.getException());
    }

    @Test
    public void testBuilder() throws UnknownHostException {
        IllegalArgumentException exception = new IllegalArgumentException();
        ServerDescription serverDescription = builder()
                                              .address(new ServerAddress("localhost:27018"))
                                              .type(ServerType.REPLICA_SET_PRIMARY)
                                              .tagSet(new TagSet(new Tag("dc", "ny")))
                                              .setName("test")
                                              .maxDocumentSize(100)
                                              .roundTripTime(50000, java.util.concurrent.TimeUnit.NANOSECONDS)
                                              .primary("localhost:27017")
                                              .canonicalAddress("localhost:27018")
                                              .hosts(new HashSet<String>(asList("localhost:27017",
                                                                                "localhost:27018",
                                                                                "localhost:27019",
                                                                                "localhost:27020")))
                                              .arbiters(new HashSet<String>(asList("localhost:27019")))
                                              .passives(new HashSet<String>(asList("localhost:27020")))
                                              .ok(true)
                                              .state(CONNECTED)
                                              .version(new ServerVersion(asList(2, 4, 1)))
                                              .minWireVersion(1)
                                              .maxWireVersion(2)
                                              .electionId(new ObjectId("123412341234123412341234"))
                                              .setVersion(new Integer(2))
                                              .exception(exception)
                                              .build();


        assertEquals(new ServerAddress("localhost:27018"), serverDescription.getAddress());
        assertTrue(serverDescription.isOk());
        assertEquals(CONNECTED, serverDescription.getState());
        assertEquals(REPLICA_SET_PRIMARY, serverDescription.getType());

        assertTrue(serverDescription.isReplicaSetMember());
        assertFalse(serverDescription.isShardRouter());
        assertFalse(serverDescription.isStandAlone());

        assertTrue(serverDescription.isPrimary());
        assertFalse(serverDescription.isSecondary());

        assertEquals(50000, serverDescription.getRoundTripTimeNanos(), 0L);

        assertEquals(100, serverDescription.getMaxDocumentSize());

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
        assertEquals(new Integer(2), serverDescription.getSetVersion());
        assertEquals(exception, serverDescription.getException());
    }

    @Test
    public void testObjectOverrides() throws UnknownHostException {
        ServerDescription.Builder builder = builder()
                                                             .address(new ServerAddress())
                                                             .type(ServerType.SHARD_ROUTER)
                                                             .tagSet(new TagSet(asList(new Tag("dc", "ny"))))
                                                                     .setName("test")
                                                                     .maxDocumentSize(100)
                                                                     .roundTripTime(50000, java.util.concurrent.TimeUnit.NANOSECONDS)
                                                                     .primary("localhost:27017")
                                                                     .canonicalAddress("localhost:27017")
                                                                     .hosts(new HashSet<String>(asList("localhost:27017",
                                                                                                       "localhost:27018")))
                                                                     .passives(new HashSet<String>(asList("localhost:27019")))
                                                                     .ok(true)
                                                                     .state(CONNECTED)
                                                                     .version(new ServerVersion(asList(2, 4, 1)))
                                                                     .minWireVersion(1)
                                                             .maxWireVersion(2)
                                                             .electionId(new ObjectId())
                                                             .setVersion(new Integer(2));
        assertEquals(builder.build(), builder.build());
        assertEquals(builder.build().hashCode(), builder.build().hashCode());
        assertTrue(builder.build().toString().startsWith("ServerDescription"));
    }

    @Test
    public void testObjectOverridesWithUnequalException() throws UnknownHostException {
        ServerDescription.Builder builder1 = builder()
                                             .state(CONNECTING)
                                             .address(new ServerAddress())
                                             .exception(new IllegalArgumentException("This is illegal"));
        ServerDescription.Builder builder2 = builder()
                                             .state(CONNECTING)
                                             .address(new ServerAddress())
                                             .exception(new IllegalArgumentException("This is also illegal"));

        ServerDescription.Builder builder3 = builder()
                                             .state(CONNECTING)
                                             .address(new ServerAddress())
                                             .exception(new IllegalStateException("This is illegal"));
        ServerDescription.Builder builder4 = builder()
                                             .state(CONNECTING)
                                             .address(new ServerAddress());


        assertThat(builder1.build(), not(builder2.build()));
        assertThat(builder1.build().hashCode(), not(builder2.build().hashCode()));
        assertThat(builder1.build(), not(builder3.build()));
        assertThat(builder1.build().hashCode(), not(builder3.build().hashCode()));
        assertThat(builder1.build(), not(builder4.build()));
        assertThat(builder1.build().hashCode(), not(builder4.build().hashCode()));
        assertThat(builder4.build(), not(builder3.build()));
        assertThat(builder4.build().hashCode(), not(builder3.build().hashCode()));
    }

    @Test
    public void testShortDescription() throws UnknownHostException {
        assertEquals("{address=127.0.0.1:27017, type=UNKNOWN, TagSet{[Tag{name='dc', value='ny'}, Tag{name='rack', value='1'}]}, "
                     + "roundTripTime=5000.0 ms, state=CONNECTED, exception={java.lang.IllegalArgumentException: This is illegal}, "
                     + "caused by {java.lang.NullPointerException: This is null}}",
                     builder().state(CONNECTED)
                              .address(new ServerAddress())
                              .roundTripTime(5000, TimeUnit.MILLISECONDS)
                              .tagSet(new TagSet(asList(new Tag("dc", "ny"), new Tag("rack", "1"))))
                              .exception(new IllegalArgumentException("This is illegal", new NullPointerException("This is null")))
                              .build()
                              .getShortDescription());
    }

    @Test
    public void testIsPrimaryAndIsSecondary() throws UnknownHostException {
        ServerDescription serverDescription = builder()
                                                               .address(new ServerAddress())
                                                               .type(ServerType.SHARD_ROUTER)
                                                               .ok(false)
                                                               .state(CONNECTED)
                                                               .build();
        assertFalse(serverDescription.isPrimary());
        assertFalse(serverDescription.isSecondary());

        serverDescription = builder()
                                             .address(new ServerAddress())
                                             .type(ServerType.SHARD_ROUTER)
                                             .ok(true)
                                             .state(CONNECTED)
                                             .build();
        assertTrue(serverDescription.isPrimary());
        assertTrue(serverDescription.isSecondary());

        serverDescription = builder()
                                             .address(new ServerAddress())
                                             .type(ServerType.STANDALONE)
                                             .ok(true)
                                             .state(CONNECTED)
                                             .build();
        assertTrue(serverDescription.isPrimary());
        assertTrue(serverDescription.isSecondary());

        serverDescription = builder()
                                             .address(new ServerAddress())
                                             .type(REPLICA_SET_PRIMARY)
                                             .ok(true)
                                             .state(CONNECTED)
                                             .build();
        assertTrue(serverDescription.isPrimary());
        assertFalse(serverDescription.isSecondary());

        serverDescription = builder()
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
        ServerDescription serverDescription = builder()
                                                               .address(new ServerAddress())
                                                               .type(ServerType.SHARD_ROUTER)
                                                               .ok(false)
                                                               .state(CONNECTED)
                                                               .build();
        assertFalse(serverDescription.hasTags(new TagSet(asList(new Tag("dc", "ny")))));

        serverDescription = builder()
                                             .address(new ServerAddress())
                                             .type(ServerType.SHARD_ROUTER)
                                             .ok(true)
                                             .state(CONNECTED)
                                             .build();
        assertTrue(serverDescription.hasTags(new TagSet(asList(new Tag("dc", "ny")))));

        serverDescription = builder()
                                             .address(new ServerAddress())
                                             .type(ServerType.STANDALONE)
                                             .ok(true)
                                             .state(CONNECTED)
                                             .build();
        assertTrue(serverDescription.hasTags(new TagSet(asList(new Tag("dc", "ny")))));

        serverDescription = builder()
                                             .address(new ServerAddress())
                                             .type(REPLICA_SET_PRIMARY)
                                             .ok(true)
                                             .state(CONNECTED)
                                             .build();
        assertTrue(serverDescription.hasTags(new TagSet()));

        serverDescription = builder()
                                             .address(new ServerAddress())
                                             .type(REPLICA_SET_PRIMARY)
                                             .ok(true)
                                             .tagSet(new TagSet(asList(new Tag("dc", "ca"))))
                                             .state(CONNECTED)
                                             .build();
        assertFalse(serverDescription.hasTags(new TagSet(asList(new Tag("dc", "ny")))));

        serverDescription = builder()
                                             .address(new ServerAddress())
                                             .type(REPLICA_SET_PRIMARY)
                                             .ok(true)
                                             .tagSet(new TagSet(asList(new Tag("rack", "1"))))
                                             .state(CONNECTED)
                                             .build();
        assertFalse(serverDescription.hasTags(new TagSet(asList(new Tag("rack", "2")))));

        serverDescription = builder()
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
        assertTrue(builder()
                                    .address(new ServerAddress())
                                    .state(CONNECTING)
                                    .ok(false)
                                    .build()
                                    .isCompatibleWithDriver());
    }

    @Test
    public void serverWithMinWireVersionEqualToDriverMaxWireVersionShouldBeCompatible() throws UnknownHostException {
        assertTrue(builder()
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
        assertTrue(builder()
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
        assertFalse(builder()
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
        assertFalse(builder()
                                     .address(new ServerAddress())
                                     .state(CONNECTING)
                                     .ok(true)
                                     .minWireVersion(MIN_DRIVER_WIRE_VERSION - 1)
                                     .maxWireVersion(MIN_DRIVER_WIRE_VERSION - 1)
                                     .build()
                                     .isCompatibleWithDriver());

    }
}
