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
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.internal.connection.OperationContext.ServerDeprioritization;
import com.mongodb.internal.mockito.MongoMockito;
import com.mongodb.selector.ServerSelector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.util.List;
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
import static org.mockito.ArgumentMatchers.any;

final class ServerDeprioritizationTest {
    private static final ServerDescription SERVER_A = serverDescription("a");
    private static final ServerDescription SERVER_B = serverDescription("b");
    private static final ServerDescription SERVER_C = serverDescription("c");
    private static final List<ServerDescription> ALL_SERVERS = unmodifiableList(asList(SERVER_A, SERVER_B, SERVER_C));
    private static final ClusterDescription REPLICA_SET_CLUSTER = multipleModeClusterDescription(ClusterType.REPLICA_SET);
    private static final ClusterDescription SHARDED_CLUSTER = multipleModeClusterDescription(ClusterType.SHARDED);
    private static final ClusterDescription UNKNOWN_CLUSTER = multipleModeClusterDescription(ClusterType.UNKNOWN);
    private static final List<ClusterDescription> CLUSTERS = asList(SHARDED_CLUSTER, REPLICA_SET_CLUSTER, UNKNOWN_CLUSTER);
    private ServerDeprioritization serverDeprioritization;

    @BeforeEach
    void beforeEach() {
        serverDeprioritization = createOperationContext(TIMEOUT_SETTINGS).getServerDeprioritization();
    }

    private static Stream<Arguments> selectNoneDeprioritized() {
        return CLUSTERS.stream().flatMap(clusterDescription ->
                Stream.of(
                        namedArguments(clusterDescription),
                        namedArguments(clusterDescription, SERVER_A),
                        namedArguments(clusterDescription, SERVER_B),
                        namedArguments(clusterDescription, SERVER_C),
                        namedArguments(clusterDescription, SERVER_A, SERVER_B),
                        namedArguments(clusterDescription, SERVER_B, SERVER_A),
                        namedArguments(clusterDescription, SERVER_A, SERVER_C),
                        namedArguments(clusterDescription, SERVER_C, SERVER_A),
                        namedArguments(clusterDescription, SERVER_A, SERVER_B, SERVER_C)
                ));
    }

    @ParameterizedTest
    @MethodSource
    void selectNoneDeprioritized(final ClusterDescription clusterDescription, final List<ServerDescription> selectorResult) {
        ServerSelector wrappedSelector = createAssertingSelector(ALL_SERVERS, selectorResult);
        assertEquals(selectorResult, serverDeprioritization.apply(wrappedSelector).select(clusterDescription));
    }

    @ParameterizedTest
    @EnumSource(value = ClusterType.class, names = {"STANDALONE", "LOAD_BALANCED"})
    void selectNoneDeprioritizedSingleServerCluster(final ClusterType clusterType) {
        ClusterDescription cluster = singleModeClusterDescription(clusterType);
        ServerSelector wrappedSelector = createAssertingSelector(singletonList(SERVER_A), singletonList(SERVER_A));
        ServerSelector emptyListWrappedSelector = createAssertingSelector(singletonList(SERVER_A), emptyList());
        assertAll(
                () -> assertEquals(singletonList(SERVER_A), serverDeprioritization.apply(wrappedSelector).select(cluster)),
                () -> assertEquals(emptyList(), serverDeprioritization.apply(emptyListWrappedSelector).select(cluster))
        );
    }

    private static Stream<Arguments> deprioritizableClusters() {
        return Stream.of(
                of(SHARDED_CLUSTER, new RuntimeException()),
                of(SHARDED_CLUSTER, new MongoException(0, "test")),
                of(REPLICA_SET_CLUSTER, createSystemOverloadedError()),
                of(UNKNOWN_CLUSTER, createSystemOverloadedError())
        );
    }

    private static Stream<Arguments> selectSomeDeprioritized() {
        return deprioritizableClusters().flatMap(args -> {
            ClusterDescription clusterDescription = (ClusterDescription) args.get()[0];
            Throwable exception = (Throwable) args.get()[1];
            return Stream.of(
                    namedArguments(clusterDescription, exception, SERVER_A),
                    namedArguments(clusterDescription, exception, SERVER_C),
                    namedArguments(clusterDescription, exception, SERVER_A, SERVER_C),
                    namedArguments(clusterDescription, exception, SERVER_C, SERVER_A)
            );
        });
    }

    @ParameterizedTest
    @MethodSource
    void selectSomeDeprioritized(final ClusterDescription clusterDescription, final Throwable exception,
                                 final List<ServerDescription> selectorResult) {
        deprioritize(clusterDescription.getType(), exception, SERVER_B);
        List<ServerDescription> expectedWrappedSelectorFilteredInput = asList(SERVER_A, SERVER_C);
        ServerSelector wrappedSelector = createAssertingSelector(expectedWrappedSelectorFilteredInput, selectorResult);
        assertEquals(selectorResult, serverDeprioritization.apply(wrappedSelector).select(clusterDescription));
    }

    private static Stream<Arguments> selectAllDeprioritized() {
        return deprioritizableClusters().flatMap(args -> {
            ClusterDescription clusterDescription = (ClusterDescription) args.get()[0];
            Throwable exception = (Throwable) args.get()[1];
            return Stream.of(
                    namedArguments(clusterDescription, exception),
                    namedArguments(clusterDescription, exception, SERVER_A),
                    namedArguments(clusterDescription, exception, SERVER_B),
                    namedArguments(clusterDescription, exception, SERVER_C),
                    namedArguments(clusterDescription, exception, SERVER_A, SERVER_B),
                    namedArguments(clusterDescription, exception, SERVER_B, SERVER_A),
                    namedArguments(clusterDescription, exception, SERVER_A, SERVER_C),
                    namedArguments(clusterDescription, exception, SERVER_C, SERVER_A),
                    namedArguments(clusterDescription, exception, SERVER_A, SERVER_B, SERVER_C)
            );
        });
    }

    @ParameterizedTest
    @MethodSource
    void selectAllDeprioritized(final ClusterDescription clusterDescription, final Throwable exception,
                                final List<ServerDescription> selectorResult) {
        deprioritize(clusterDescription.getType(), exception, SERVER_A);
        deprioritize(clusterDescription.getType(), exception, SERVER_B);
        deprioritize(clusterDescription.getType(), exception, SERVER_C);
        ServerSelector selector = createAssertingSelector(ALL_SERVERS, selectorResult);
        assertEquals(selectorResult, serverDeprioritization.apply(selector).select(clusterDescription));
    }

    @ParameterizedTest
    @EnumSource(value = ClusterType.class, names = {"STANDALONE", "LOAD_BALANCED"})
    void selectAllDeprioritizedSingleServerCluster(final ClusterType clusterType) {
        ClusterDescription cluster = singleModeClusterDescription(clusterType);
        deprioritize(clusterType, createSystemOverloadedError(), SERVER_A);
        ServerSelector selector = createAssertingSelector(singletonList(SERVER_A), singletonList(SERVER_A));
        assertEquals(singletonList(SERVER_A), serverDeprioritization.apply(selector).select(cluster));
    }

    @ParameterizedTest
    @MethodSource("selectSomeDeprioritized")
    void selectWithRetryWhenWrappedReturnsEmpty(final ClusterDescription clusterDescription,
                                                final Throwable exception,
                                                final List<ServerDescription> selectorResult) {
        deprioritize(clusterDescription.getType(), exception, SERVER_B);
        ServerSelector selector = MongoMockito.mock(ServerSelector.class, tuner ->
                Mockito.when(tuner.select(any(ClusterDescription.class)))
                        .thenAnswer(invocation -> {
                            assertEquals(asList(SERVER_A, SERVER_C), invocation.<ClusterDescription>getArgument(0).getServerDescriptions());
                            return emptyList();
                        })
                        .thenAnswer(invocation -> {
                            assertEquals(ALL_SERVERS, invocation.<ClusterDescription>getArgument(0).getServerDescriptions());
                            return selectorResult;
                        })
        );
        assertEquals(selectorResult, serverDeprioritization.apply(selector).select(clusterDescription));
    }

    @Test
    void onAttemptFailureIgnoresIfPoolClearedException() {
        serverDeprioritization.updateCandidate(SERVER_A.getAddress(), ClusterType.SHARDED);
        serverDeprioritization.onAttemptFailure(
                new MongoConnectionPoolClearedException(new ServerId(new ClusterId(), new ServerAddress()), null));
        ServerSelector selector = createAssertingSelector(ALL_SERVERS, ALL_SERVERS);
        assertEquals(ALL_SERVERS, serverDeprioritization.apply(selector).select(SHARDED_CLUSTER));
    }

    @Test
    void onAttemptFailureDoesNotThrowIfNoCandidate() {
        assertDoesNotThrow(() -> serverDeprioritization.onAttemptFailure(new RuntimeException()));
    }

    @ParameterizedTest
    @EnumSource(value = ClusterType.class, names = "SHARDED", mode = EnumSource.Mode.EXCLUDE)
    void onAttemptFailureIgnoresIfNonShardedWithoutOverloadError(final ClusterType clusterType) {
        ServerSelector selector = createAssertingSelector(ALL_SERVERS, singletonList(SERVER_A));

        assertAll(() -> {
                    serverDeprioritization.updateCandidate(SERVER_B.getAddress(), clusterType);
                    serverDeprioritization.onAttemptFailure(new RuntimeException());
                    assertEquals(singletonList(SERVER_A), serverDeprioritization.apply(selector).select(SHARDED_CLUSTER),
                            "Expected no deprioritization for " + clusterType + " with RuntimeException");
                }, () -> {
                    serverDeprioritization = createOperationContext(TIMEOUT_SETTINGS).getServerDeprioritization();
                    serverDeprioritization.updateCandidate(SERVER_B.getAddress(), clusterType);
                    serverDeprioritization.onAttemptFailure(new MongoException(1, "error"));
                    assertEquals(singletonList(SERVER_A), serverDeprioritization.apply(selector).select(SHARDED_CLUSTER),
                            "Expected no deprioritization for " + clusterType + " with no SystemOverloadedError MongoException");
                }
        );
    }

    private void deprioritize(final ClusterType clusterType, final Throwable exception, final ServerDescription... serverDescriptions) {
        for (ServerDescription serverDescription : serverDescriptions) {
            serverDeprioritization.updateCandidate(serverDescription.getAddress(), clusterType);
            serverDeprioritization.onAttemptFailure(exception);
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

    private static MongoException createSystemOverloadedError() {
        MongoException e = new MongoException(6, "overloaded");
        e.addLabel("SystemOverloadedError");
        return e;
    }

    private static Arguments namedArguments(final ClusterDescription clusterDescription, final ServerDescription... serverDescriptions) {
        return of(Named.of(generateArgumentName(clusterDescription), clusterDescription),
                Named.of(generateArgumentName(asList(serverDescriptions)), asList(serverDescriptions)));
    }

    private static Arguments namedArguments(final ClusterDescription clusterDescription, final Throwable exception, final ServerDescription... serverDescriptions) {
        return of(Named.of(generateArgumentName(clusterDescription), clusterDescription),
                exception,
                Named.of(generateArgumentName(asList(serverDescriptions)), asList(serverDescriptions)));
    }

    private static String generateArgumentName(final List<ServerDescription> servers) {
        return "[" + servers.stream()
                .map(ServerDescription::getAddress)
                .map(ServerAddress::getHost)
                .collect(Collectors.joining(", ")) + "]";
    }

    private static String generateArgumentName(final ClusterDescription clusterDescription) {
        return "[" + clusterDescription.getType() + ", " + generateArgumentName(clusterDescription.getServerDescriptions()) + "]";
    }
}
