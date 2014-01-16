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

import static com.mongodb.ClusterConnectionMode.Multiple;
import static com.mongodb.ClusterType.ReplicaSet;
import static com.mongodb.ServerConnectionState.Connected;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class ReadPreferenceServerSelectorTest {
    @Test
    public void testAll() throws UnknownHostException {
        ReadPreferenceServerSelector selector = new ReadPreferenceServerSelector(ReadPreference.primary());

        assertEquals(ReadPreference.primary(), selector.getReadPreference());

        assertEquals("ReadPreferenceServerSelector{readPreference=primary}", selector.toString());

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
        assertEquals(asList(primary), selector.choose(new ClusterDescription(Multiple, ReplicaSet, asList(secondary, primary))));
    }
}
