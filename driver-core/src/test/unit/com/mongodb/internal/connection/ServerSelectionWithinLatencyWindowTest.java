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

package com.mongodb.internal.connection;

import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.assertions.Assertions;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.internal.selector.ReadPreferenceServerSelector;
import com.mongodb.selector.ServerSelector;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonNumber;
import org.bson.BsonValue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static com.mongodb.connection.ServerSelectionSelectionTest.buildClusterDescription;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;
import static util.JsonPoweredTestHelper.testDir;
import static util.JsonPoweredTestHelper.testDocs;

/**
 * A runner for
 * <a href="https://github.com/mongodb/specifications/tree/master/source/server-selection/tests#selection-within-latency-window-yaml-tests">
 * Selection Within Latency Window Tests</a>.
 */
@RunWith(Parameterized.class)
public class ServerSelectionWithinLatencyWindowTest {
    private final ClusterDescription clusterDescription;
    private final Map<ServerAddress, Server> serverCatalog;
    private final int iterations;
    private final Outcome outcome;

    public ServerSelectionWithinLatencyWindowTest(
            @SuppressWarnings("unused") final Path fileName,
            @SuppressWarnings("unused") final String description,
            final BsonDocument definition) {
        clusterDescription = buildClusterDescription(definition.getDocument("topology_description"), null);
        serverCatalog = serverCatalog(definition.getArray("mocked_topology_state"));
        iterations = definition.getInt32("iterations").getValue();
        outcome = Outcome.parse(definition.getDocument("outcome"));
    }

    @Test
    public void shouldPassAllOutcomes() {
        ServerSelector selector = new ReadPreferenceServerSelector(ReadPreference.nearest());
        Map<ServerAddress, List<ServerTuple>> selectionResultsGroupedByServerAddress = IntStream.range(0, iterations)
                .mapToObj(i -> BaseCluster.selectServer(selector, clusterDescription,
                        address -> Assertions.assertNotNull(serverCatalog.get(address)), ThreadLocalRandom.current()))
                .collect(groupingBy(serverTuple -> serverTuple.getServerDescription().getAddress()));
        Map<ServerAddress, BigDecimal> selectionFrequencies = selectionResultsGroupedByServerAddress.entrySet()
                .stream()
                .collect(toMap(Map.Entry::getKey, entry -> BigDecimal.valueOf(entry.getValue().size())
                        .setScale(2, RoundingMode.UNNECESSARY)
                        .divide(BigDecimal.valueOf(iterations), RoundingMode.HALF_UP)));
        outcome.assertMatches(selectionFrequencies);
    }

    @Parameterized.Parameters(name = "{0}: {1}")
    public static Collection<Object[]> data() {
        return testDocs(testDir("/server-selection/in_window"))
                .entrySet()
                .stream()
                .map(entry -> new Object[] {
                        entry.getKey().getFileName(),
                        entry.getValue().getString("description").getValue(),
                        entry.getValue()})
                .collect(toList());
    }

    private static Map<ServerAddress, Server> serverCatalog(final BsonArray mockedTopologyState) {
        return mockedTopologyState.stream()
                .map(BsonValue::asDocument)
                .collect(toMap(
                        el -> ServerAddressHelper.parse(el.getString("address").getValue()),
                        el -> {
                            int operationCount = el.getInt32("operation_count").getValue();
                            Server server = Mockito.mock(Server.class);
                            when(server.operationCount()).thenReturn(operationCount);
                            return server;
                        }));
    }

    private static final class Outcome {
        private final double tolerance;
        private final Map<ServerAddress, BigDecimal> expectedFrequencies;

        private Outcome(final double tolerance, final Map<ServerAddress, BigDecimal> expectedFrequencies) {
            this.tolerance = tolerance;
            this.expectedFrequencies = expectedFrequencies;
        }

        static Outcome parse(final BsonDocument outcome) {
            return new Outcome(
                    outcome.getNumber("tolerance").doubleValue(),
                    outcome.getDocument("expected_frequencies")
                            .entrySet()
                            .stream()
                            .collect(toMap(
                                    entry -> ServerAddressHelper.parse(entry.getKey()),
                                    entry -> {
                                        BsonNumber frequency = entry.getValue().asNumber();
                                        return frequency.isInt32()
                                                ? BigDecimal.valueOf(frequency.intValue())
                                                : BigDecimal.valueOf(frequency.doubleValue());
                                    })));
        }

        void assertMatches(final Map<ServerAddress, BigDecimal> actualFrequencies) {
            String msg = String.format("Expected %s,%nactual %s", expectedFrequencies, actualFrequencies);
            expectedFrequencies.forEach((address, expectedFrequency) -> {
                BigDecimal actualFrequency = actualFrequencies.getOrDefault(address, BigDecimal.ZERO);
                if (expectedFrequency.compareTo(BigDecimal.ZERO) == 0 || expectedFrequency.compareTo(BigDecimal.ONE) == 0) {
                    assertEquals(msg, 0, expectedFrequency.compareTo(actualFrequency));
                } else {
                    assertEquals(msg, expectedFrequency.doubleValue(), actualFrequency.doubleValue(), tolerance);
                }
            });
        }
    }
}
