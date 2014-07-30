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

package com.mongodb.selector;

import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;
import org.junit.Test;

import java.net.UnknownHostException;

import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE;
import static com.mongodb.connection.ClusterType.REPLICA_SET;
import static com.mongodb.connection.ServerConnectionState.CONNECTED;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class ReadPreferenceServerSelectorTest {
    @Test
    public void testAll() throws UnknownHostException {
        ReadPreferenceServerSelector selector = new ReadPreferenceServerSelector(ReadPreference.primary());

        assertEquals(ReadPreference.primary(), selector.getReadPreference());

        assertEquals("ReadPreferenceServerSelector{readPreference=primary}", selector.toString());

        ServerDescription primary = ServerDescription.builder()
                                                     .state(CONNECTED)
                                                     .address(new ServerAddress())
                                                     .ok(true)
                                                     .type(ServerType.REPLICA_SET_PRIMARY)
                                                     .build();
        ServerDescription secondary = ServerDescription.builder()
                                                     .state(CONNECTED)
                                                     .address(new ServerAddress())
                                                     .ok(true)
                                                     .type(ServerType.REPLICA_SET_SECONDARY)
                                                     .build();
        assertEquals(asList(primary), selector.select(new ClusterDescription(MULTIPLE, REPLICA_SET, asList(primary, secondary))));
    }
}

