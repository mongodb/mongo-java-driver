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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mongodb.connection.ClusterConnectionMode.Multiple;
import static org.mongodb.connection.ClusterType.ReplicaSet;
import static org.mongodb.connection.ServerConnectionState.Connected;

public class ServerAddressSelectorTest {
    @Test
    public void testAll() throws UnknownHostException {
        ServerAddressSelector selector = new ServerAddressSelector(new ServerAddress("localhost:27018"));

        assertEquals(new ServerAddressSelector(new ServerAddress("localhost:27018")), selector);
        assertNotEquals(new ServerAddressSelector(new ServerAddress()), selector);
        assertNotEquals(new Object(), selector);

        assertTrue(selector.toString().startsWith("ServerAddressSelector"));

        assertEquals(selector.getServerAddress(), selector.getServerAddress());

        assertEquals(selector.getServerAddress().hashCode(), selector.hashCode());

        final ServerDescription primary = ServerDescription.builder()
                .state(Connected)
                .address(new ServerAddress())
                .ok(true)
                .type(ServerType.ReplicaSetPrimary)
                .build();
        final ServerDescription secondary = ServerDescription.builder()
                .state(Connected)
                .address(new ServerAddress("localhost:27018"))
                .ok(true)
                .type(ServerType.ReplicaSetSecondary)
                .build();
        assertEquals(Arrays.asList(secondary), selector.choose(new ClusterDescription(Multiple, ReplicaSet,
                Arrays.asList(primary, secondary))));
   }
}