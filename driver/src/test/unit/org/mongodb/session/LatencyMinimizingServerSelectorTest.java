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

package org.mongodb.session;

import org.junit.Test;
import org.mongodb.connection.ClusterDescription;
import org.mongodb.connection.ServerAddress;
import org.mongodb.connection.ServerDescription;
import org.mongodb.connection.ServerType;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.mongodb.connection.ClusterConnectionMode.Multiple;
import static org.mongodb.connection.ClusterType.ReplicaSet;
import static org.mongodb.connection.ServerConnectionState.Connected;

public class LatencyMinimizingServerSelectorTest {
    @Test
    public void testLatencyDifferentialMinimization() throws UnknownHostException {
        LatencyMinimizingServerSelector selector = new LatencyMinimizingServerSelector(20, TimeUnit.MILLISECONDS);
        final ServerDescription primary = ServerDescription.builder()
                .state(Connected)
                .address(new ServerAddress())
                .ok(true)
                .type(ServerType.ReplicaSetPrimary)
                .averagePingTime(10, TimeUnit.MILLISECONDS)
                .build();
        final ServerDescription secondaryOne = ServerDescription.builder()
                .state(Connected)
                .address(new ServerAddress("localhost:27018"))
                .ok(true)
                .type(ServerType.ReplicaSetSecondary)
                .averagePingTime(15, TimeUnit.MILLISECONDS)
                .build();
        final ServerDescription secondaryTwo = ServerDescription.builder()
                .state(Connected)
                .address(new ServerAddress("localhost:27019"))
                .ok(true)
                .type(ServerType.ReplicaSetSecondary)
                .averagePingTime(31, TimeUnit.MILLISECONDS)
                .build();
        final ServerDescription secondaryThree = ServerDescription.builder()
                .state(Connected)
                .address(new ServerAddress("localhost:27020"))
                .ok(true)
                .type(ServerType.ReplicaSetSecondary)
                .averagePingTime(30, TimeUnit.MILLISECONDS)
                .build();
        assertEquals(Arrays.asList(primary, secondaryOne, secondaryThree),
                selector.choose(new ClusterDescription(Multiple, ReplicaSet,
                        Arrays.asList(primary, secondaryOne, secondaryTwo, secondaryThree))));
    }

    @Test
    public void testZeroLatencyDifferentialTolerance() throws UnknownHostException {
        LatencyMinimizingServerSelector selector = new LatencyMinimizingServerSelector(0, TimeUnit.NANOSECONDS);
        final ServerDescription primary = ServerDescription.builder()
                .state(Connected)
                .address(new ServerAddress())
                .ok(true)
                .type(ServerType.ReplicaSetPrimary)
                .averagePingTime(10, TimeUnit.NANOSECONDS)
                .build();
        final ServerDescription secondaryOne = ServerDescription.builder()
                .state(Connected)
                .address(new ServerAddress("localhost:27018"))
                .ok(true)
                .type(ServerType.ReplicaSetSecondary)
                .averagePingTime(11, TimeUnit.NANOSECONDS)
                .build();
        assertEquals(Arrays.asList(primary), selector.choose(new ClusterDescription(Multiple, ReplicaSet,
                Arrays.asList(primary, secondaryOne))));
    }
}
