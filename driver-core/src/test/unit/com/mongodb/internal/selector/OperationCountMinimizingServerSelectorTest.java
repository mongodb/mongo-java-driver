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
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.connection.Server;
import com.mongodb.internal.mockito.MongoMockito;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.when;

final class OperationCountMinimizingServerSelectorTest {
    @ParameterizedTest
    @MethodSource("args")
    void select(final Map<String, Integer> hostToOperationCount, final List<String> expectedHosts) {
        ClusterDescriptionAndServersSnapshot pair = clusterDescriptionAndServersSnapshot(hostToOperationCount);
        List<String> actualHosts = new OperationCountMinimizingServerSelector(pair.getServersSnapshot())
                .select(pair.getClusterDescription())
                .stream()
                .map(serverDescription -> serverDescription.getAddress().getHost())
                .collect(toList());
        assertEquals(expectedHosts, actualHosts, hostToOperationCount::toString);
    }

    private static Stream<Arguments> args() {
        return Stream.of(
                arguments(emptyMap(), emptyList()),
                arguments(singletonMap("a", 0), singletonList("a")),
                arguments(linkedMap(m -> {
                    m.put("b", 0);
                    m.put("a", 5);
                }), singletonList("b")),
                arguments(linkedMap(m -> {
                    m.put("b", 2);
                    m.put("a", 3);
                    m.put("c", 2);
                }), singletonList("b")),
                arguments(linkedMap(m -> {
                    m.put("b", 5);
                    m.put("a", 5);
                    m.put("e", 0);
                    m.put("c", 5);
                    m.put("d", 8);
                }), singletonList("e"))
        );
    }

    private static ClusterDescriptionAndServersSnapshot clusterDescriptionAndServersSnapshot(final Map<String, Integer> hostToOperationCount) {
        ClusterDescription clusterDescription = new ClusterDescription(
                ClusterConnectionMode.MULTIPLE, ClusterType.REPLICA_SET, serverDescriptions(hostToOperationCount.keySet()));
        Map<ServerAddress, Integer> serverAddressToOperationCount = hostToOperationCount.entrySet()
                .stream().collect(toMap(entry -> new ServerAddress(entry.getKey()), Map.Entry::getValue));
        Cluster.ServersSnapshot serversSnapshot = serverAddress -> {
            int operationCount = serverAddressToOperationCount.get(serverAddress);
            return MongoMockito.mock(Server.class, server ->
                    when(server.operationCount()).thenReturn(operationCount));
        };
        return new ClusterDescriptionAndServersSnapshot(clusterDescription, serversSnapshot);
    }

    private static List<ServerDescription> serverDescriptions(final Collection<String> hosts) {
        return hosts.stream()
                .map(OperationCountMinimizingServerSelectorTest::serverDescription)
                .collect(toList());
    }

    private static ServerDescription serverDescription(final String host) {
        return ServerDescription.builder()
                .state(ServerConnectionState.CONNECTED)
                .ok(true)
                .address(new ServerAddress(host))
                .build();
    }

    private static <K, V> LinkedHashMap<K, V> linkedMap(final Consumer<LinkedHashMap<K, V>> filler) {
        LinkedHashMap<K, V> result = new LinkedHashMap<>();
        filler.accept(result);
        return result;
    }

    private static final class ClusterDescriptionAndServersSnapshot {
        private final ClusterDescription clusterDescription;
        private final Cluster.ServersSnapshot serversSnapshot;

        private ClusterDescriptionAndServersSnapshot(
                final ClusterDescription clusterDescription,
                final Cluster.ServersSnapshot serversSnapshot) {
            this.clusterDescription = clusterDescription;
            this.serversSnapshot = serversSnapshot;
        }

        ClusterDescription getClusterDescription() {
            return clusterDescription;
        }

        Cluster.ServersSnapshot getServersSnapshot() {
            return serversSnapshot;
        }
    }
}
