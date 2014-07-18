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

import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ServerDescription;
import org.junit.Before;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.Arrays;

import static com.mongodb.ReadPreference.secondary;
import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE;
import static com.mongodb.connection.ClusterType.REPLICA_SET;
import static com.mongodb.connection.ServerConnectionState.CONNECTED;
import static com.mongodb.connection.ServerType.REPLICA_SET_PRIMARY;
import static com.mongodb.connection.ServerType.REPLICA_SET_SECONDARY;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;

public class CompositeServerSelectorTest {
    private CompositeServerSelector selector;
    private ServerDescription second;
    private ServerDescription first;
    private ServerDescription third;

    @Before
    public void setUp() throws UnknownHostException {
        first = ServerDescription.builder()
                                 .state(CONNECTED)
                                 .address(new ServerAddress())
                                 .ok(true)
                                 .roundTripTime(5, MILLISECONDS)
                                 .type(REPLICA_SET_PRIMARY)
                                 .build();

        second = ServerDescription.builder()
                                  .state(CONNECTED)
                                  .address(new ServerAddress("localhost:27018"))
                                  .ok(true)
                                  .roundTripTime(30, MILLISECONDS)
                                  .type(REPLICA_SET_SECONDARY)
                                  .build();

        third = ServerDescription.builder()
                                 .state(CONNECTED)
                                 .address(new ServerAddress("localhost:27019"))
                                 .ok(true)
                                 .roundTripTime(35, MILLISECONDS)
                                 .type(REPLICA_SET_SECONDARY)
                                 .build();
    }

    @Test
    public void shouldApplyServerSelectorsInOrder() {
        selector = new CompositeServerSelector(asList(new ReadPreferenceServerSelector(secondary()),
                                                      new LatencyMinimizingServerSelector(15, MILLISECONDS)));
        assertEquals(selector.select(new ClusterDescription(MULTIPLE, REPLICA_SET, asList(first, second, third))), asList(second, third));
    }

    @Test
    public void shouldCollapseNestedComposite() {
        CompositeServerSelector composedSelector =
        new CompositeServerSelector(asList(new ReadPreferenceServerSelector(secondary()),
                                           new LatencyMinimizingServerSelector(15, MILLISECONDS)));
        selector = new CompositeServerSelector(Arrays.<ServerSelector>asList(composedSelector));
        assertEquals(selector.select(new ClusterDescription(MULTIPLE, REPLICA_SET, asList(first, second, third))), asList(second, third));

    }
}
