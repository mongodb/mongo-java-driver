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
package com.mongodb.internal.selector;

import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

final class AtMostTwoRandomServerSelectorTest {
    @ParameterizedTest
    @MethodSource("args")
    void select(
            final List<String> hosts,
            final int numberOfSelectIterations,
            final double expectedCount,
            final double frequencyTolerance,
            final int expectedSelectedSize) {
        ClusterDescription clusterDescription = clusterDescription(hosts);
        HashMap<ServerAddress, Integer> actualCounters = new HashMap<>();
        for (int i = 0; i < numberOfSelectIterations; i++) {
            List<ServerDescription> selected = AtMostTwoRandomServerSelector.instance().select(clusterDescription);
            assertEquals(expectedSelectedSize, selected.size(), selected::toString);
            selected.forEach(serverDescription -> actualCounters.merge(serverDescription.getAddress(), 1, Integer::sum));
        }
        actualCounters.forEach((serverAddress, counter) ->
                assertEquals(
                        expectedCount / numberOfSelectIterations,
                        (double) counter / numberOfSelectIterations,
                        frequencyTolerance,
                        () -> String.format("serverAddress=%s, counter=%d, actualCounters=%s", serverAddress, counter, actualCounters)));
    }

    private static Stream<Arguments> args() {
        int smallNumberOfSelectIterations = 10;
        int largeNumberOfSelectIterations = 2_000;
        int maxSelectedSize = 2;
        return Stream.of(
                arguments(emptyList(),
                        smallNumberOfSelectIterations, 0, 0, 0),
                arguments(singletonList("1"),
                        smallNumberOfSelectIterations, smallNumberOfSelectIterations, 0, 1),
                arguments(asList("1", "2"),
                        smallNumberOfSelectIterations, smallNumberOfSelectIterations, 0, maxSelectedSize),
                arguments(asList("1", "2", "3"),
                        largeNumberOfSelectIterations, (double) maxSelectedSize * largeNumberOfSelectIterations / 3, 0.05, maxSelectedSize),
                arguments(asList("1", "2", "3", "4", "5", "6", "7"),
                        largeNumberOfSelectIterations, (double) maxSelectedSize * largeNumberOfSelectIterations / 7, 0.05, maxSelectedSize)
        );
    }

    private static ClusterDescription clusterDescription(final List<String> hosts) {
        return new ClusterDescription(ClusterConnectionMode.MULTIPLE, ClusterType.REPLICA_SET, serverDescriptions(hosts));
    }

    private static List<ServerDescription> serverDescriptions(final Collection<String> hosts) {
        return hosts.stream()
                .map(AtMostTwoRandomServerSelectorTest::serverDescription)
                .collect(toList());
    }

    private static ServerDescription serverDescription(final String host) {
        return ServerDescription.builder()
                .state(ServerConnectionState.CONNECTED)
                .ok(true)
                .address(new ServerAddress(host))
                .build();
    }
}
