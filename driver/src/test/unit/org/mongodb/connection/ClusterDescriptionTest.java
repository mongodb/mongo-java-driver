/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.connection;

import org.junit.Test;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mongodb.connection.ClusterConnectionMode.MULTIPLE;
import static org.mongodb.connection.ClusterType.REPLICA_SET;
import static org.mongodb.connection.ClusterType.UNKNOWN;
import static org.mongodb.connection.ServerConnectionState.CONNECTED;
import static org.mongodb.connection.ServerConnectionState.CONNECTING;
import static org.mongodb.connection.ServerDescription.MAX_DRIVER_WIRE_VERSION;
import static org.mongodb.connection.ServerType.REPLICA_SET_PRIMARY;

public class ClusterDescriptionTest {
    @Test
    public void testMode() {
        ClusterDescription description = new ClusterDescription(MULTIPLE, UNKNOWN, Collections.<ServerDescription>emptyList());
        assertEquals(MULTIPLE, description.getConnectionMode());
    }

    @Test
    public void testEmptySet() {
        ClusterDescription description = new ClusterDescription(MULTIPLE, UNKNOWN, Collections.<ServerDescription>emptyList());
        assertTrue(description.getAll().isEmpty());
    }

    @Test
    public void testIsConnecting() throws UnknownHostException {
        ClusterDescription description = new ClusterDescription(MULTIPLE, REPLICA_SET, asList(ServerDescription.builder()
                                                                                                               .state(CONNECTING)
                                                                                                               .address(new ServerAddress())
                                                                                                               .type(REPLICA_SET_PRIMARY)
                                                                                                               .build()));
        assertTrue(description.isConnecting());

        description = new ClusterDescription(MULTIPLE, REPLICA_SET, asList(ServerDescription.builder()
                                                                                            .state(CONNECTED)
                                                                                            .address(new ServerAddress())
                                                                                            .type(REPLICA_SET_PRIMARY)
                                                                                            .build()));
        assertFalse(description.isConnecting());
    }

    @Test
    public void testSortingOfAll() {
        ClusterDescription description = new ClusterDescription(MULTIPLE,
                                                                UNKNOWN,
                                                                asList(ServerDescription.builder()
                                                                                        .state(CONNECTING)
                                                                                        .address(new ServerAddress("loc:27019"))
                                                                                        .build(),
                                                                       ServerDescription.builder()
                                                                                        .state(CONNECTING)
                                                                                        .address(new ServerAddress("loc:27018"))
                                                                                        .build(),
                                                                       ServerDescription.builder()
                                                                                        .state(CONNECTING)
                                                                                        .address(new ServerAddress("loc:27017"))
                                                                                        .build())
        );
        Iterator<ServerDescription> iter = description.getAll().iterator();
        assertEquals(new ServerAddress("loc:27017"), iter.next().getAddress());
        assertEquals(new ServerAddress("loc:27018"), iter.next().getAddress());
        assertEquals(new ServerAddress("loc:27019"), iter.next().getAddress());
    }

    @Test
    public void clusterDescriptionWithAnIncompatibleServerShouldBeIncompatible() throws UnknownHostException {
        ClusterDescription description =
        new ClusterDescription(MULTIPLE, UNKNOWN, Arrays.asList(
                                                               ServerDescription.builder()
                                                                                .state(CONNECTING)
                                                                                .address(new ServerAddress("loc:27019"))
                                                                                .build(),
                                                               ServerDescription.builder()
                                                                                .state(CONNECTED)
                                                                                .ok(true)
                                                                                .address(new ServerAddress("loc:27018"))
                                                                                .minWireVersion(MAX_DRIVER_WIRE_VERSION + 1)
                                                                                .maxWireVersion(MAX_DRIVER_WIRE_VERSION + 1)
                                                                                .build(),
                                                               ServerDescription.builder()
                                                                                .state(CONNECTING)
                                                                                .address(new ServerAddress("loc:27017"))
                                                                                .build())
        );
        assertFalse(description.isCompatibleWithDriver());
    }

    @Test
    public void clusterDescriptionWithCompatibleServerShouldBeCompatible() throws UnknownHostException {
        ClusterDescription description =
        new ClusterDescription(MULTIPLE, UNKNOWN, Arrays.asList(
                                                               ServerDescription.builder()
                                                                                .state(CONNECTING)
                                                                                .address(new ServerAddress("loc:27019"))
                                                                                .build(),
                                                               ServerDescription.builder()
                                                                                .state(CONNECTING)
                                                                                .address(new ServerAddress("loc:27018"))
                                                                                .build(),
                                                               ServerDescription.builder()
                                                                                .state(CONNECTING)
                                                                                .address(new ServerAddress("loc:27017"))
                                                                                .build())
        );
        assertTrue(description.isCompatibleWithDriver());
    }

    @Test
    public void testObjectOverrides() {
        ClusterDescription description = new ClusterDescription(MULTIPLE,
                                                                UNKNOWN,
                                                                asList(ServerDescription.builder()
                                                                                        .state(CONNECTING)
                                                                                        .address(new ServerAddress("loc:27019"))
                                                                                        .build(),
                                                                       ServerDescription.builder()
                                                                                        .state(CONNECTING)
                                                                                        .address(new ServerAddress("loc:27018"))
                                                                                        .build(),
                                                                       ServerDescription.builder()
                                                                                        .state(CONNECTING)
                                                                                        .address(new ServerAddress("loc:27017"))
                                                                                        .build())
        );
        ClusterDescription descriptionTwo = new ClusterDescription(MULTIPLE,
                                                                   UNKNOWN,
                                                                   asList(ServerDescription.builder()
                                                                                           .state(CONNECTING)
                                                                                           .address(new ServerAddress("loc:27019"))
                                                                                           .build(),
                                                                          ServerDescription.builder()
                                                                                           .state(CONNECTING)
                                                                                           .address(new ServerAddress("loc:27018"))
                                                                                           .build(),
                                                                          ServerDescription.builder()
                                                                                           .state(CONNECTING)
                                                                                           .address(new ServerAddress("loc:27017"))
                                                                                           .build())
        );
        assertEquals(description, descriptionTwo);
        assertEquals(description.hashCode(), descriptionTwo.hashCode());
        assertTrue(description.toString().startsWith("ClusterDescription"));
    }
}
