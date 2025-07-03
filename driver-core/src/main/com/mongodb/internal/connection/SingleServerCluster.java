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

import com.mongodb.MongoConfigurationException;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;
import com.mongodb.event.ServerDescriptionChangedEvent;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;
import com.mongodb.internal.time.Timeout;

import java.util.concurrent.atomic.AtomicReference;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.connection.ServerConnectionState.CONNECTING;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * This class needs to be final because we are leaking a reference to "this" from the constructor
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class SingleServerCluster extends BaseCluster {
    private static final Logger LOGGER = Loggers.getLogger("cluster");

    private final AtomicReference<ClusterableServer> server;

    public SingleServerCluster(final ClusterId clusterId, final ClusterSettings settings, final ClusterableServerFactory serverFactory,
                               final ClientMetadata clientMetadata) {
        super(clusterId, settings, serverFactory, clientMetadata);
        isTrue("one server in a direct cluster", settings.getHosts().size() == 1);
        isTrue("connection mode is single", settings.getMode() == ClusterConnectionMode.SINGLE);

        server = new AtomicReference<>();
        // synchronized in the constructor because the change listener is re-entrant to this instance.
        // In other words, we are leaking a reference to "this" from the constructor.
        withLock(() -> {
            publishDescription(ServerDescription.builder().state(CONNECTING).address(settings.getHosts().get(0))
                    .build());
            server.set(createServer(settings.getHosts().get(0)));
        });
    }

    @Override
    protected void connect() {
        assertNotNull(server.get()).connect();
    }

    @Override
    public ServersSnapshot getServersSnapshot(
            final Timeout serverSelectionTimeout,
            final TimeoutContext timeoutContext) {
        isTrue("open", !isClosed());
        ClusterableServer server = assertNotNull(this.server.get());
        return serverAddress -> server;
    }

    @Override
    public void close() {
        if (!isClosed()) {
            assertNotNull(server.get()).close();
            super.close();
        }
    }

    @Override
    public void onChange(final ServerDescriptionChangedEvent event) {
        withLock(() -> {
            ServerDescription newDescription = event.getNewDescription();
            if (newDescription.isOk()) {
                if (getSettings().getRequiredClusterType() != ClusterType.UNKNOWN
                        && getSettings().getRequiredClusterType() != newDescription.getClusterType()) {
                    newDescription = null;
                } else if (getSettings().getRequiredClusterType() == ClusterType.REPLICA_SET
                        && getSettings().getRequiredReplicaSetName() != null) {
                    if (!getSettings().getRequiredReplicaSetName().equals(newDescription.getSetName())) {
                        newDescription = ServerDescription.builder(newDescription)
                                .exception(new MongoConfigurationException(
                                        format("Replica set name '%s' does not match required replica set name of '%s'",
                                                newDescription.getSetName(), getSettings().getRequiredReplicaSetName())))
                                .type(ServerType.UNKNOWN)
                                .setName(null)
                                .ok(false)
                                .build();
                        publishDescription(ClusterType.UNKNOWN, newDescription);
                        return;
                    }
                }
            }
            publishDescription(newDescription);
        });
    }

    private void publishDescription(final ServerDescription serverDescription) {
        ClusterType clusterType = getSettings().getRequiredClusterType();
        if (clusterType == ClusterType.UNKNOWN && serverDescription != null) {
            clusterType = serverDescription.getClusterType();
        }
        publishDescription(clusterType, serverDescription);
    }

    private void publishDescription(final ClusterType clusterType, final ServerDescription serverDescription) {
        ClusterDescription currentDescription = getCurrentDescription();
        ClusterDescription description = new ClusterDescription(ClusterConnectionMode.SINGLE, clusterType,
                serverDescription == null ? emptyList() : singletonList(serverDescription), getSettings(),
                getServerFactory().getSettings());

        updateDescription(description);
        fireChangeEvent(description, currentDescription);
    }
}
