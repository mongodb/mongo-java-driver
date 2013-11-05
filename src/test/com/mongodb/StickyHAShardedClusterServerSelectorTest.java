/*
 * Copyright (c) 2008 - 2013 MongoDB Inc., Inc. <http://mongodb.com>
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

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.UnknownHostException;
import java.util.Collections;

import static com.mongodb.ClusterConnectionMode.Multiple;
import static com.mongodb.ClusterConnectionMode.Single;
import static com.mongodb.ClusterType.ReplicaSet;
import static com.mongodb.ClusterType.Sharded;
import static com.mongodb.ServerConnectionState.Connected;
import static com.mongodb.ServerConnectionState.Connecting;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;

public class StickyHAShardedClusterServerSelectorTest {

    private StickyHAShardedClusterServerSelector selector;
    private ServerDescription first;
    private ServerDescription secondConnecting;
    private ServerDescription secondConnected;

    @BeforeMethod
    public void setUp() throws UnknownHostException {
        selector = new StickyHAShardedClusterServerSelector();
        first = ServerDescription.builder()
                                                   .state(Connected)
                                                   .address(new ServerAddress())
                                                   .ok(true)
                                                   .averagePingTime(10, MILLISECONDS)
                                                   .type(ServerType.ShardRouter)
                                                   .build();

        secondConnecting = ServerDescription.builder()
                                  .state(Connecting)
                                  .address(new ServerAddress("localhost:27018"))
                                  .ok(false)
                                  .type(ServerType.ShardRouter)
                                  .build();

        secondConnected = ServerDescription.builder()
                                  .state(Connected)
                                  .address(new ServerAddress("localhost:27018"))
                                  .ok(true)
                                  .averagePingTime(8, MILLISECONDS)
                                  .type(ServerType.ShardRouter)
                                  .build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNotMultipleConnectionMode() throws UnknownHostException {
        selector.choose(new ClusterDescription(Single, Sharded, Collections.<ServerDescription>emptyList()));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNotShardedClusterType() throws UnknownHostException {
        selector.choose(new ClusterDescription(Multiple, ReplicaSet, Collections.<ServerDescription>emptyList()));
    }

    @Test
    public void testInitial() {
        assertEquals(selector.choose(new ClusterDescription(Multiple, Sharded, asList(secondConnecting, first))),
                     asList(first));
    }

    @Test
    public void testStickiness() {
        // stick the first
        selector.choose(new ClusterDescription(Multiple, Sharded, asList(secondConnecting, first)));

        assertEquals(selector.choose(new ClusterDescription(Multiple, Sharded, asList(secondConnected, first))), asList(secondConnected));
    }

    @Test
    public void testNoneStuck() {
        assertEquals(selector.choose(new ClusterDescription(Multiple, Sharded, asList(secondConnected, first))), asList(secondConnected));
    }

    @Test
    public void testClear() {
        // stick it
        selector.choose(new ClusterDescription(Multiple, Sharded, asList(secondConnecting, first)));

        selector.choose(new ClusterDescription(Multiple, Sharded, asList(secondConnected, first)));

        selector.clear();

        assertEquals(selector.choose(new ClusterDescription(Multiple, Sharded, asList(secondConnected, first))), asList(secondConnected));
    }

    @Test
    public void testUnstick() {
        // stick it
        selector.choose(new ClusterDescription(Multiple, Sharded, asList(secondConnected, first)));

        assertEquals(selector.choose(new ClusterDescription(Multiple, Sharded, asList(secondConnecting, first))), asList(first));
    }
}
