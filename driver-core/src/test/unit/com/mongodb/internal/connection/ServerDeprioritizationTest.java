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
import com.mongodb.internal.selector.ServerAddressSelector;
import com.mongodb.selector.ServerSelector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.EnumSource.Mode.EXCLUDE;

final class ServerDeprioritizationTest {
    private static final ServerDescription SERVER_A = serverDescription("a");
    private static final ServerDescription SERVER_B = serverDescription("b");
    private static final ServerDescription SERVER_C = serverDescription("c");
    private static final List<ServerDescription> ALL_SERVERS = unmodifiableList(asList(SERVER_A, SERVER_B, SERVER_C));
    private static final ClusterDescription REPLICA_SET = clusterDescription(ClusterType.REPLICA_SET);
    private static final ClusterDescription SHARDED_CLUSTER = clusterDescription(ClusterType.SHARDED);
    private static final ServerSelector ALL_SELECTOR = new AllServerSelector();

    private ServerDeprioritization serverDeprioritization;

    @BeforeEach
    void beforeEach() {
        serverDeprioritization = new OperationContext().getServerDeprioritization();
    }

    @Test
    void applyNoneDeprioritized() {
        assertAll(
                () -> assertEquals(ALL_SERVERS, serverDeprioritization.apply(ALL_SELECTOR).select(SHARDED_CLUSTER)),
                () -> assertEquals(ALL_SERVERS, serverDeprioritization.apply(ALL_SELECTOR).select(REPLICA_SET))
        );
    }

    @Test
    void applySomeDeprioritized() {
        deprioritize(SERVER_B);
        assertAll(
                () -> assertEquals(asList(SERVER_A, SERVER_C), serverDeprioritization.apply(ALL_SELECTOR).select(SHARDED_CLUSTER)),
                () -> assertEquals(ALL_SERVERS, serverDeprioritization.apply(ALL_SELECTOR).select(REPLICA_SET))
        );
    }

    @Test
    void applyFallsBack() {
        assertAll(
                () -> {
                    deprioritize(SERVER_A);
                    deprioritize(SERVER_B);
                    deprioritize(SERVER_C);
                    assertAll(
                            () -> assertEquals(ALL_SERVERS, serverDeprioritization.apply(ALL_SELECTOR).select(SHARDED_CLUSTER)),
                            () -> assertEquals(ALL_SERVERS, serverDeprioritization.apply(ALL_SELECTOR).select(REPLICA_SET))
                    );
                },
                () -> {
                    ServerSelector serverBSelector = new ServerAddressSelector(SERVER_B.getAddress());
                    deprioritize(SERVER_B);
                    assertAll(
                            () -> assertEquals(singletonList(SERVER_B),
                                    serverDeprioritization.apply(serverBSelector).select(SHARDED_CLUSTER)),
                            () -> assertEquals(singletonList(SERVER_B), serverDeprioritization.apply(serverBSelector).select(REPLICA_SET))
                    );
                }
        );
    }

    @ParameterizedTest
    @EnumSource(value = ClusterType.class, mode = EXCLUDE, names = {"SHARDED"})
    void updateCandidateIgnoresIfNotShardedCluster(final ClusterType clusterType) {
        serverDeprioritization.updateCandidate(SERVER_A.getAddress(), clusterType);
        serverDeprioritization.onAttemptFailure(new RuntimeException());
        assertEquals(ALL_SERVERS, serverDeprioritization.apply(ALL_SELECTOR).select(SHARDED_CLUSTER));
    }

    @Test
    void onAttemptFailureIgnoresIfPoolClearedException() {
        serverDeprioritization.updateCandidate(SERVER_A.getAddress(), ClusterType.SHARDED);
        serverDeprioritization.onAttemptFailure(
                new MongoConnectionPoolClearedException(new ServerId(new ClusterId(), new ServerAddress()), null));
        assertEquals(ALL_SERVERS, serverDeprioritization.apply(ALL_SELECTOR).select(SHARDED_CLUSTER));
    }

    @Test
    void onAttemptFailureDoesNotThrowIfNoCandidate() {
        assertDoesNotThrow(() -> serverDeprioritization.onAttemptFailure(new RuntimeException()));
    }

    private void deprioritize(final ServerDescription... serverDescriptions) {
        for (ServerDescription serverDescription : serverDescriptions) {
            serverDeprioritization.updateCandidate(serverDescription.getAddress(), ClusterType.SHARDED);
            serverDeprioritization.onAttemptFailure(new RuntimeException());
        }
    }

    private static ServerDescription serverDescription(final String host) {
        return ServerDescription.builder()
                .state(ServerConnectionState.CONNECTED)
                .ok(true)
                .address(new ServerAddress(host))
                .build();
    }

    private static ClusterDescription clusterDescription(final ClusterType clusterType) {
        return new ClusterDescription(ClusterConnectionMode.MULTIPLE, clusterType, asList(SERVER_A, SERVER_B, SERVER_C));
    }

    private static final class AllServerSelector implements ServerSelector {
        AllServerSelector() {
        }

        @Override
        public List<ServerDescription> select(final ClusterDescription clusterDescription) {
            return clusterDescription.getServerDescriptions();
        }
    }
}
