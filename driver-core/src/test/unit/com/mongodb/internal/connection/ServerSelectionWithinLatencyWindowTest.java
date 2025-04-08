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
import com.mongodb.connection.ClusterSettings;
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
import util.JsonPoweredTestHelper;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS;
import static com.mongodb.ClusterFixture.createOperationContext;
import static com.mongodb.connection.ServerSelectionSelectionTest.buildClusterDescription;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

/**
 * A runner for
 * <a href="https://github.com/mongodb/specifications/tree/master/source/server-selection/tests#selection-within-latency-window-yaml-tests">
 * Selection Within Latency Window Tests</a>.
 */
@RunWith(Parameterized.class)
public class ServerSelectionWithinLatencyWindowTest {
    private final ClusterDescription clusterDescription;
    private final Cluster.ServersSnapshot serversSnapshot;
    private final int iterations;
    private final Outcome outcome;

    public ServerSelectionWithinLatencyWindowTest(
            @SuppressWarnings("unused") final String fileName,
            @SuppressWarnings("unused") final String description,
            final BsonDocument definition) {
        clusterDescription = buildClusterDescription(definition.getDocument("topology_description"), null);
        serversSnapshot = serverCatalog(definition.getArray("mocked_topology_state"));
        iterations = definition.getInt32("iterations").getValue();
        outcome = Outcome.parse(definition.getDocument("outcome"));
    }

    @Test
    public void shouldPassAllOutcomes() {
        ServerSelector selector = new ReadPreferenceServerSelector(ReadPreference.nearest());
        OperationContext.ServerDeprioritization emptyServerDeprioritization = createOperationContext(TIMEOUT_SETTINGS)
                .getServerDeprioritization();
        ClusterSettings defaultClusterSettings = ClusterSettings.builder().build();
        Map<ServerAddress, List<ServerTuple>> selectionResultsGroupedByServerAddress = IntStream.range(0, iterations)
                .mapToObj(i -> BaseCluster.createCompleteSelectorAndSelectServer(selector, clusterDescription, serversSnapshot,
                        emptyServerDeprioritization, defaultClusterSettings))
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
        List<Object[]> data = new ArrayList<>();
        for (BsonDocument testDocument : JsonPoweredTestHelper.getTestDocuments("/server-selection/in_window")) {
            data.add(new Object[]{testDocument.getString("fileName").getValue(),
                    testDocument.getString("description").getValue(),
                    testDocument});
        }
        return data;
    }

    private static Cluster.ServersSnapshot serverCatalog(final BsonArray mockedTopologyState) {
        Map<ServerAddress, Server> serverMap = mockedTopologyState.stream()
                .map(BsonValue::asDocument)
                .collect(toMap(
                        el -> new ServerAddress(el.getString("address").getValue()),
                        el -> {
                            int operationCount = el.getInt32("operation_count").getValue();
                            Server server = Mockito.mock(Server.class);
                            when(server.operationCount()).thenReturn(operationCount);
                            return server;
                        }));
        return serverAddress -> Assertions.assertNotNull(serverMap.get(serverAddress));
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
                                    entry -> new ServerAddress(entry.getKey()),
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
