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

import com.mongodb.MongoClientException;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.Tag;
import com.mongodb.TagSet;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;
import com.mongodb.connection.TopologyVersion;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;
import java.util.HashSet;

import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE;
import static com.mongodb.connection.ClusterConnectionMode.SINGLE;
import static com.mongodb.connection.ClusterType.REPLICA_SET;
import static com.mongodb.connection.ClusterType.STANDALONE;
import static com.mongodb.connection.ServerConnectionState.CONNECTED;
import static com.mongodb.connection.ServerConnectionState.CONNECTING;
import static com.mongodb.connection.ServerDescription.builder;
import static com.mongodb.internal.connection.EventHelper.equivalentEvent;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EventHelperTest {

    @Test
    public void testServerDescriptionEventEquivalence() {
        ServerDescription serverDescription = createBuilder().build();
        assertTrue(equivalentEvent(serverDescription, serverDescription));
        assertTrue(equivalentEvent((ServerDescription) null, null));
        assertFalse(equivalentEvent(serverDescription, null));
        assertFalse(equivalentEvent(null, serverDescription));

        assertTrue(equivalentEvent(createBuilder().build(), createBuilder().build()));

        assertFalse(equivalentEvent(serverDescription, createBuilder().ok(false).build()));
        assertFalse(equivalentEvent(serverDescription, createBuilder().state(CONNECTING).build()));
        assertFalse(equivalentEvent(serverDescription, createBuilder().type(ServerType.STANDALONE).build()));
        assertFalse(equivalentEvent(serverDescription, createBuilder().minWireVersion(2).build()));
        assertFalse(equivalentEvent(serverDescription, createBuilder().maxWireVersion(3).build()));
        assertFalse(equivalentEvent(serverDescription, createBuilder().canonicalAddress("host:27017").build()));
        assertFalse(equivalentEvent(serverDescription, createBuilder()
                .hosts(new HashSet<String>(asList("localhost:27017", "localhost:27018", "localhost:27019"))).build()));
        assertFalse(equivalentEvent(serverDescription, createBuilder()
                .passives(new HashSet<String>(singletonList("localhost:27018"))).build()));
        assertFalse(equivalentEvent(serverDescription, createBuilder()
                .arbiters(new HashSet<String>(singletonList("localhost:27018"))).build()));
        assertFalse(equivalentEvent(serverDescription, createBuilder().tagSet(new TagSet()).build()));
        assertFalse(equivalentEvent(serverDescription, createBuilder().setName("test2").build()));
        assertFalse(equivalentEvent(serverDescription, createBuilder().setVersion(3).build()));
        assertFalse(equivalentEvent(serverDescription, createBuilder().electionId(new ObjectId()).build()));
        assertFalse(equivalentEvent(serverDescription, createBuilder().primary("localhost:27018").build()));
        assertFalse(equivalentEvent(serverDescription, createBuilder().logicalSessionTimeoutMinutes(26).build()));
        assertFalse(equivalentEvent(serverDescription, createBuilder().topologyVersion(
                new TopologyVersion(new ObjectId("5e47699e32e4571020a96f07"), 43)).build()));

        assertTrue(equivalentEvent(createBuilder().exception(new MongoException("msg1")).build(),
                createBuilder().exception(new MongoException("msg1")).build()));
        assertFalse(equivalentEvent(serverDescription, createBuilder().exception(new MongoException("msg1")).build()));
        assertFalse(equivalentEvent(createBuilder().exception(new MongoException("msg1")).build(),
                createBuilder().exception(new MongoException("msg2")).build()));
        assertFalse(equivalentEvent(createBuilder().exception(new MongoException("msg1")).build(),
                createBuilder().exception(new IOException("msg1")).build()));

        assertTrue(equivalentEvent(serverDescription, createBuilder().lastWriteDate(new Date(100)).build()));
    }

    @Test
    public void testClusterDescriptionEquivalence() {
        assertTrue(equivalentEvent(new ClusterDescription(SINGLE, STANDALONE, singletonList(createBuilder().build())),
                new ClusterDescription(SINGLE, STANDALONE, singletonList(createBuilder().build()))));
        assertTrue(equivalentEvent(new ClusterDescription(MULTIPLE, REPLICA_SET,
                        asList(createBuilder("localhost:27017").build(),
                                createBuilder("localhost:27018").build())),
                new ClusterDescription(MULTIPLE, REPLICA_SET,
                        asList(createBuilder("localhost:27017").build(),
                                createBuilder("localhost:27018").build()))));
        assertTrue(equivalentEvent(new ClusterDescription(MULTIPLE, REPLICA_SET,
                        asList(createBuilder("localhost:27017").build(),
                                createBuilder("localhost:27018").build())),
                new ClusterDescription(MULTIPLE, REPLICA_SET,
                        asList(createBuilder("localhost:27018").build(),
                                createBuilder("localhost:27017").build()))));

        assertFalse(equivalentEvent(new ClusterDescription(SINGLE, STANDALONE, singletonList(createBuilder().build())),
                new ClusterDescription(SINGLE, STANDALONE, singletonList(createBuilder().maxWireVersion(4).build()))));
        assertFalse(equivalentEvent(new ClusterDescription(MULTIPLE, REPLICA_SET,
                        asList(createBuilder("localhost:27017").build(),
                                createBuilder("localhost:27018").build())),
                new ClusterDescription(MULTIPLE, REPLICA_SET,
                        singletonList(createBuilder("localhost:27017").build()))));
        assertFalse(equivalentEvent(new ClusterDescription(MULTIPLE, REPLICA_SET,
                        asList(createBuilder("localhost:27017").build(),
                                createBuilder("localhost:27018").build())),
                new ClusterDescription(MULTIPLE, REPLICA_SET,
                        asList(createBuilder("localhost:27017").build(),
                                createBuilder("localhost:27018").maxWireVersion(4).build()))));
        assertFalse(equivalentEvent(new ClusterDescription(MULTIPLE, REPLICA_SET,
                        asList(createBuilder("localhost:27017").build(),
                                createBuilder("localhost:27018").build())),
                new ClusterDescription(MULTIPLE, REPLICA_SET,
                        asList(createBuilder("localhost:27018").build(),
                                createBuilder("localhost:27017").maxWireVersion(4).build()))));

        assertTrue(equivalentEvent(new ClusterDescription(SINGLE, STANDALONE, new MongoException("msg1"), emptyList(), null, null),
                new ClusterDescription(SINGLE, STANDALONE, new MongoException("msg1"), emptyList(), null, null)));
        assertFalse(equivalentEvent(new ClusterDescription(SINGLE, STANDALONE, new MongoException("msg1"), emptyList(), null, null),
                new ClusterDescription(SINGLE, STANDALONE, null, emptyList(), null, null)));
        assertFalse(equivalentEvent(new ClusterDescription(SINGLE, STANDALONE, new MongoException("msg1"), emptyList(), null, null),
                new ClusterDescription(SINGLE, STANDALONE, new MongoException("msg2"), emptyList(), null, null)));
        assertFalse(equivalentEvent(new ClusterDescription(SINGLE, STANDALONE, new MongoException("msg1"), emptyList(), null, null),
                new ClusterDescription(SINGLE, STANDALONE, new MongoClientException("msg1"), emptyList(), null, null)));

    }

    private ServerDescription.Builder createBuilder() {
        return createBuilder("localhost:27017");
    }

    private ServerDescription.Builder createBuilder(final String address) {
        return builder().address(new ServerAddress(address))
                .ok(true)
                .state(CONNECTED)
                .type(ServerType.REPLICA_SET_PRIMARY)
                .minWireVersion(1)
                .maxWireVersion(2)
                .canonicalAddress(address)
                .hosts(new HashSet<String>(asList("localhost:27017", "localhost:27018")))
                .passives(new HashSet<String>(singletonList("localhost:27019")))
                .arbiters(new HashSet<String>(singletonList("localhost:27020")))
                .tagSet(new TagSet(singletonList(new Tag("dc", "ny"))))
                .setName("test")
                .setVersion(2)
                .electionId(new ObjectId("abcdabcdabcdabcdabcdabcd"))
                .primary("localhost:27017")
                .logicalSessionTimeoutMinutes(25)
                .topologyVersion(new TopologyVersion(new ObjectId("5e47699e32e4571020a96f07"), 42))
                .lastWriteDate(new Date(99));
    }
}
