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

import org.junit.Before;
import org.junit.Test;

import java.net.UnknownHostException;

import static com.mongodb.ClusterConnectionMode.Multiple;
import static com.mongodb.ClusterType.ReplicaSet;
import static com.mongodb.ServerConnectionState.Connected;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;

public class CompositeServerSelectorTest {
    CompositeServerSelector selector;
    private ServerDescription second;
    private ServerDescription first;
    private ServerDescription third;

    @Before
    public void setUp() throws UnknownHostException {
        first = ServerDescription.builder()
                                 .state(Connected)
                                 .address(new ServerAddress())
                                 .ok(true)
                                 .averagePingTime(5, MILLISECONDS)
                                 .type(ServerType.ReplicaSetPrimary)
                                 .build();

        second = ServerDescription.builder()
                                  .state(Connected)
                                  .address(new ServerAddress("localhost:27018"))
                                  .ok(true)
                                  .averagePingTime(30, MILLISECONDS)
                                  .type(ServerType.ReplicaSetSecondary)
                                  .build();

        third = ServerDescription.builder()
                                 .state(Connected)
                                 .address(new ServerAddress("localhost:27019"))
                                 .ok(true)
                                 .averagePingTime(35, MILLISECONDS)
                                 .type(ServerType.ReplicaSetSecondary)
                                 .build();
    }

    @Test
    public void shouldApplyServerSelectorsInOrder() {
        selector = new CompositeServerSelector(asList(new ReadPreferenceServerSelector(ReadPreference.secondary()),
                                                      new LatencyMinimizingServerSelector(15, MILLISECONDS)));
        assertEquals(selector.choose(new ClusterDescription(Multiple, ReplicaSet, asList(first, second, third))),
                     asList(second, third));
    }
}
