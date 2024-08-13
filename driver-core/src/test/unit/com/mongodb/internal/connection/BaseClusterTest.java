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

import com.mongodb.ClusterFixture;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.mockito.MongoMockito;
import com.mongodb.internal.selector.ServerAddressSelector;
import org.junit.jupiter.api.Test;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.when;

/**
 * @see BaseClusterSpecification
 */
final class BaseClusterTest {
    @Test
    void selectServerToleratesWhenThereIsNoServerForTheSelectedAddress() {
        ServerAddress serverAddressA = new ServerAddress("a");
        ServerAddress serverAddressB = new ServerAddress("b");
        Server serverB = MongoMockito.mock(Server.class, server ->
                when(server.operationCount()).thenReturn(0));
        ClusterDescription clusterDescriptionAB = new ClusterDescription(ClusterConnectionMode.MULTIPLE, ClusterType.SHARDED,
                asList(serverDescription(serverAddressA), serverDescription(serverAddressB)));
        Cluster.ServersSnapshot serversSnapshotB = serverAddress -> serverAddress.equals(serverAddressB) ? serverB : null;
        assertDoesNotThrow(() -> BaseCluster.createCompleteSelectorAndSelectServer(
                new ServerAddressSelector(serverAddressA),
                clusterDescriptionAB,
                serversSnapshotB,
                ClusterFixture.OPERATION_CONTEXT.getServerDeprioritization(),
                ClusterSettings.builder().build()));
    }

    private static ServerDescription serverDescription(final ServerAddress serverAddress) {
        return ServerDescription.builder()
                .state(ServerConnectionState.CONNECTED)
                .ok(true)
                .address(serverAddress)
                .build();
    }
}
