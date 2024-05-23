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
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.connection.Cluster.ServersSnapshot;
import com.mongodb.internal.connection.Server;
import com.mongodb.selector.ServerSelector;

import java.util.Collections;
import java.util.List;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static java.util.Collections.emptyList;
import static java.util.Comparator.comparingInt;

/**
 * {@linkplain #select(ClusterDescription) Selects} at most one {@link ServerDescription}
 * corresponding to a {@link ServersSnapshot#getServer(ServerAddress) server} with the smallest {@link Server#operationCount()}.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
@ThreadSafe
public final class MinimumOperationCountServerSelector implements ServerSelector {
    private final ServersSnapshot serversSnapshot;

    /**
     * @param serversSnapshot Must {@linkplain ServersSnapshot#containsServer(ServerAddress) contain} {@link Server}s corresponding to
     * {@linkplain ClusterDescription#getServerDescriptions() all} {@link ServerDescription}s
     * in the {@link ClusterDescription} passed to {@link #select(ClusterDescription)}.
     */
    public MinimumOperationCountServerSelector(final ServersSnapshot serversSnapshot) {
        this.serversSnapshot = serversSnapshot;
    }

    @Override
    public List<ServerDescription> select(final ClusterDescription clusterDescription) {
        return clusterDescription.getServerDescriptions()
                .stream()
                .min(comparingInt(serverDescription ->
                        assertNotNull(serversSnapshot.getServer(serverDescription.getAddress()))
                                .operationCount()))
                .map(Collections::singletonList)
                .orElse(emptyList());
    }
}
