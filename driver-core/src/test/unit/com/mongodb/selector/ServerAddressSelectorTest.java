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

package com.mongodb.selector;

import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.Arrays;

import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE;
import static com.mongodb.connection.ClusterType.REPLICA_SET;
import static com.mongodb.connection.ServerConnectionState.CONNECTED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ServerAddressSelectorTest {
    @Test
    public void testAll() throws UnknownHostException {
        ServerAddressSelector selector = new ServerAddressSelector(new ServerAddress("localhost:27018"));

        assertTrue(selector.toString().startsWith("ServerAddressSelector"));

        assertEquals(selector.getServerAddress(), selector.getServerAddress());

        ServerDescription primary = ServerDescription.builder()
                                                     .state(CONNECTED)
                                                     .address(new ServerAddress())
                                                     .ok(true)
                                                     .type(ServerType.REPLICA_SET_PRIMARY)
                                                     .build();
        ServerDescription secondary = ServerDescription.builder()
                                                       .state(CONNECTED)
                                                       .address(new ServerAddress("localhost:27018"))
                                                       .ok(true)
                                                       .type(ServerType.REPLICA_SET_SECONDARY)
                                                       .build();
        assertEquals(Arrays.asList(secondary), selector.select(new ClusterDescription(MULTIPLE, REPLICA_SET,
                                                                                      Arrays.asList(primary, secondary))));
    }
}
