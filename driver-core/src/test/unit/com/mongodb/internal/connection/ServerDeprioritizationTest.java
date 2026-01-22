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

import com.mongodb.MongoConnectionPoolClearedException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.internal.connection.OperationContext.ServerDeprioritization;
import com.mongodb.selector.ServerSelector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS;
import static com.mongodb.ClusterFixture.createOperationContext;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.of;

final class ServerDeprioritizationTest {
    private static final ServerDescription SERVER_A = serverDescription("a");
    private static final ServerDescription SERVER_B = serverDescription("b");
    private static final ServerDescription SERVER_C = serverDescription("c");
    private static final List<ServerDescription> ALL_SERVERS = unmodifiableList(asList(SERVER_A, SERVER_B, SERVER_C));
    private static final ClusterDescription REPLICA_SET_CLUSTER = multipleModeClusterDescription(ClusterType.REPLICA_SET);
    private static final ClusterDescription SHARDED_CLUSTER = multipleModeClusterDescription(ClusterType.SHARDED);
    private static final ClusterDescription LOAD_BALANCED_CLUSTER = singleModeClusterDescription(ClusterType.LOAD_BALANCED);
    private static final ClusterDescription STANDALONE_CLUSTER = singleModeClusterDescription(ClusterType.STANDALONE);
    private static final ClusterDescription UNKNOWN_CLUSTER = multipleModeClusterDescription(ClusterType.UNKNOWN);

    private ServerDeprioritization serverDeprioritization;

    @BeforeEach
    void beforeEach() {
        serverDeprioritization = createOperationContext(TIMEOUT_SETTINGS).getServerDeprioritization();
    }

    private static Stream<Arguments> selectNoneDeprioritized() {
        return Stream.of(
                of(Named.of(generateArgumentName(emptyList()), emptyList())),
                of(Named.of(generateArgumentName(singletonList(SERVER_A)), singletonList(SERVER_A))),
                of(Named.of(generateArgumentName(singletonList(SERVER_B)), singletonList(SERVER_B))),
                of(Named.of(generateArgumentName(singletonList(SERVER_C)), singletonList(SERVER_C))),
                of(Named.of(generateArgumentName(asList(SERVER_A, SERVER_B)), asList(SERVER_A, SERVER_B))),
                of(Named.of(generateArgumentName(asList(SERVER_B, SERVER_A)), asList(SERVER_B, SERVER_A))),
                of(Named.of(generateArgumentName(asList(SERVER_A, SERVER_C)), asList(SERVER_A, SERVER_C))),
                of(Named.of(generateArgumentName(asList(SERVER_C, SERVER_A)), asList(SERVER_C, SERVER_A))),
                of(Named.of(generateArgumentName(ALL_SERVERS), ALL_SERVERS))
        );
    }
    @ParameterizedTest
    @MethodSource
    void selectNoneDeprioritized(final List<ServerDescription> selectorResult) {
        ServerSelector wrappedSelector = createAssertingSelector(ALL_SERVERS, selectorResult);
        assertAll(
                () -> assertEquals(selectorResult, serverDeprioritization.applyDeprioritization(wrappedSelector).select(SHARDED_CLUSTER)),
                () -> assertEquals(selectorResult, serverDeprioritization.applyDeprioritization(wrappedSelector).select(REPLICA_SET_CLUSTER)),
                () -> assertEquals(selectorResult, serverDeprioritization.applyDeprioritization(wrappedSelector).select(UNKNOWN_CLUSTER))
        );
    }

    @Test
    void selectNoneDeprioritizedSingleServerCluster() {
        ServerSelector wrappedSelector = createAssertingSelector(singletonList(SERVER_A), singletonList(SERVER_A));
        ServerSelector emptyListWrappedSelector = createAssertingSelector(singletonList(SERVER_A), emptyList());
        assertAll(
                () -> assertEquals(singletonList(SERVER_A), serverDeprioritization.applyDeprioritization(wrappedSelector).select(STANDALONE_CLUSTER)),
                () -> assertEquals(emptyList(), serverDeprioritization.applyDeprioritization(emptyListWrappedSelector).select(STANDALONE_CLUSTER)),
                () -> assertEquals(singletonList(SERVER_A), serverDeprioritization.applyDeprioritization(wrappedSelector).select(LOAD_BALANCED_CLUSTER)),
                () -> assertEquals(emptyList(), serverDeprioritization.applyDeprioritization(emptyListWrappedSelector).select(LOAD_BALANCED_CLUSTER))
        );
    }

   private static Stream<Arguments> selectSomeDeprioritized() {
        return Stream.of(
                of(Named.of(generateArgumentName(singletonList(SERVER_A)), singletonList(SERVER_A))),
                of(Named.of(generateArgumentName(singletonList(SERVER_C)), singletonList(SERVER_C))),
                of(Named.of(generateArgumentName(asList(SERVER_A, SERVER_C)), asList(SERVER_A, SERVER_C))),
                of(Named.of(generateArgumentName(asList(SERVER_C, SERVER_A)), asList(SERVER_C, SERVER_A)))
        );
    }

    @ParameterizedTest
    @MethodSource
    void selectSomeDeprioritized(final List<ServerDescription> selectorResult) {
        deprioritize(SERVER_B);
        List<ServerDescription> expectedWrappedSelectorFilteredInput = asList(SERVER_A, SERVER_C);
        ServerSelector selector = createAssertingSelector(expectedWrappedSelectorFilteredInput, selectorResult);
        assertAll(
                () -> assertEquals(selectorResult, serverDeprioritization.applyDeprioritization(selector).select(SHARDED_CLUSTER)),
                () -> assertEquals(selectorResult, serverDeprioritization.applyDeprioritization(selector).select(REPLICA_SET_CLUSTER)),
                () -> assertEquals(selectorResult, serverDeprioritization.applyDeprioritization(selector).select(UNKNOWN_CLUSTER))
        );
    }

    @ParameterizedTest
    @MethodSource("selectNoneDeprioritized")
    void selectAllDeprioritized(final List<ServerDescription> selectorResult) {
        deprioritize(SERVER_A);
        deprioritize(SERVER_B);
        deprioritize(SERVER_C);
        ServerSelector selector = createAssertingSelector(ALL_SERVERS, selectorResult);
        assertAll(
                () -> assertEquals(selectorResult, serverDeprioritization.applyDeprioritization(selector).select(SHARDED_CLUSTER)),
                () -> assertEquals(selectorResult, serverDeprioritization.applyDeprioritization(selector).select(REPLICA_SET_CLUSTER)),
                () -> assertEquals(selectorResult, serverDeprioritization.applyDeprioritization(selector).select(UNKNOWN_CLUSTER))
        );
    }

    @Test
    void selectAllDeprioritizedSingleServerCluster() {
        deprioritize(SERVER_A);
        ServerSelector selector = createAssertingSelector(singletonList(SERVER_A), singletonList(SERVER_A));
        assertAll(
                () -> assertEquals(singletonList(SERVER_A), serverDeprioritization.applyDeprioritization(selector).select(STANDALONE_CLUSTER)),
                () -> assertEquals(singletonList(SERVER_A),
                        serverDeprioritization.applyDeprioritization(selector).select(LOAD_BALANCED_CLUSTER))
        );
    }

    @ParameterizedTest
    @MethodSource("selectSomeDeprioritized")
    void selectWithRetryWhenWrappedReturnsEmpty(final List<ServerDescription> selectorResult) {
        deprioritize(SERVER_B);
        Supplier<ServerSelector> selector = () -> new ServerSelector() {
            private boolean firstCall = true;

            @Override
            public List<ServerDescription> select(final ClusterDescription clusterDescription) {
                List<ServerDescription> servers = clusterDescription.getServerDescriptions();
                if (firstCall) {
                    firstCall = false;
                    assertEquals(asList(SERVER_A, SERVER_C), servers);
                    return emptyList();
                }
                assertEquals(ALL_SERVERS, servers);
                return selectorResult;
            }
        };

        assertAll(
                () -> assertEquals(selectorResult, serverDeprioritization.applyDeprioritization(selector.get()).select(SHARDED_CLUSTER)),
                () -> assertEquals(selectorResult, serverDeprioritization.applyDeprioritization(selector.get()).select(REPLICA_SET_CLUSTER)),
                () -> assertEquals(selectorResult, serverDeprioritization.applyDeprioritization(selector.get()).select(UNKNOWN_CLUSTER))
        );
    }

    @Test
    void onAttemptFailureIgnoresIfPoolClearedException() {
        serverDeprioritization.updateCandidate(SERVER_A.getAddress());
        serverDeprioritization.onAttemptFailure(
                new MongoConnectionPoolClearedException(new ServerId(new ClusterId(), new ServerAddress()), null));
        ServerSelector selector = createAssertingSelector(ALL_SERVERS, singletonList(SERVER_A));
        assertEquals(singletonList(SERVER_A), serverDeprioritization.applyDeprioritization(selector).select(SHARDED_CLUSTER));
    }

    @Test
    void onAttemptFailureDoesNotThrowIfNoCandidate() {
        assertDoesNotThrow(() -> serverDeprioritization.onAttemptFailure(new RuntimeException()));
    }

    private void deprioritize(final ServerDescription... serverDescriptions) {
        for (ServerDescription serverDescription : serverDescriptions) {
            serverDeprioritization.updateCandidate(serverDescription.getAddress());
            serverDeprioritization.onAttemptFailure(new RuntimeException());
        }
    }

    private static ServerSelector createAssertingSelector(
            final List<ServerDescription> expectedInput,
            final List<ServerDescription> selectorResult) {
        return clusterDescription -> {
            assertEquals(expectedInput, clusterDescription.getServerDescriptions());
            return selectorResult;
        };
    }

    private static ServerDescription serverDescription(final String host) {
        return ServerDescription.builder()
                .state(ServerConnectionState.CONNECTED)
                .ok(true)
                .address(new ServerAddress(host))
                .build();
    }

    private static ClusterDescription multipleModeClusterDescription(final ClusterType clusterType) {
        return new ClusterDescription(ClusterConnectionMode.MULTIPLE, clusterType, ALL_SERVERS);
    }

    private static ClusterDescription singleModeClusterDescription(final ClusterType clusterType) {
        return new ClusterDescription(ClusterConnectionMode.SINGLE, clusterType, singletonList(SERVER_A));
    }

    private static String generateArgumentName(final List<ServerDescription> servers) {
        return "[" + servers.stream()
                .map(ServerDescription::getAddress)
                .map(ServerAddress::getHost)
                .collect(Collectors.joining(", ")) + "]";
    }
}
