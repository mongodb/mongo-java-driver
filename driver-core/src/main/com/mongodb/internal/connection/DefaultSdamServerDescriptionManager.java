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

import com.mongodb.annotations.ThreadSafe;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.ServerType;
import com.mongodb.event.ServerDescriptionChangedEvent;
import com.mongodb.event.ServerListener;

import static com.mongodb.assertions.Assertions.assertFalse;
import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.connection.ServerType.UNKNOWN;
import static com.mongodb.internal.connection.EventHelper.wouldDescriptionsGenerateEquivalentEvents;
import static com.mongodb.internal.connection.ServerDescriptionHelper.unknownConnectingServerDescription;

@ThreadSafe
final class DefaultSdamServerDescriptionManager implements SdamServerDescriptionManager {
    private final Cluster cluster;
    private final ServerId serverId;
    private final ServerListener serverListener;
    private final ServerMonitor serverMonitor;
    private final ConnectionPool connectionPool;
    private final ClusterConnectionMode connectionMode;
    private volatile ServerDescription description;

    DefaultSdamServerDescriptionManager(final Cluster cluster,
                                        final ServerId serverId,
                                        final ServerListener serverListener, final ServerMonitor serverMonitor,
                                        final ConnectionPool connectionPool,
                                        final ClusterConnectionMode connectionMode) {
        this.cluster = cluster;
        this.serverId = assertNotNull(serverId);
        this.serverListener = assertNotNull(serverListener);
        this.serverMonitor = assertNotNull(serverMonitor);
        this.connectionPool = assertNotNull(connectionPool);
        this.connectionMode = assertNotNull(connectionMode);
        description = unknownConnectingServerDescription(serverId, null);
    }

    @Override
    public void update(final ServerDescription candidateDescription) {
        cluster.withLock(() -> {
            if (TopologyVersionHelper.newer(description.getTopologyVersion(), candidateDescription.getTopologyVersion())) {
                return;
            }
            ServerType newServerType = candidateDescription.getType();
            boolean markedPoolReady = false;
            /* A paused pool should not be exposed and used. Calling `ready` before updating description and calling `invalidate` after
             * facilitates achieving this. However, because once the pool is observed, it may be used concurrently with the pool being
             * invalidated by either the current method or the `handleException` method, the pool still may be used in a paused state.
             * For those cases `MongoConnectionPoolClearedException` was introduced. */
            if (ServerTypeHelper.isDataBearing(newServerType)
                    || (newServerType != UNKNOWN && connectionMode == ClusterConnectionMode.SINGLE)) {
                connectionPool.ready();
                markedPoolReady = true;
            }
            updateDescription(candidateDescription);
            Throwable candidateDescriptionException = candidateDescription.getException();
            if (candidateDescriptionException != null) {
                assertTrue(newServerType == UNKNOWN);
                assertFalse(markedPoolReady);
                connectionPool.invalidate(candidateDescriptionException);
            }
        });
    }

    @Override
    public void handleExceptionBeforeHandshake(final SdamIssue sdamIssue) {
        cluster.withLock(() -> handleException(sdamIssue, true));
    }

    @Override
    public void handleExceptionAfterHandshake(final SdamIssue sdamIssue) {
        cluster.withLock(() -> handleException(sdamIssue, false));
    }

    @Override
    public SdamIssue.Context context() {
        return new SdamIssue.Context(serverId, connectionPool.getGeneration(), description.getMaxWireVersion());
    }

    @Override
    public SdamIssue.Context context(final InternalConnection connection) {
        return new SdamIssue.Context(serverId, connection.getGeneration(), connection.getDescription().getMaxWireVersion());
    }

    private void updateDescription(final ServerDescription newDescription) {
        ServerDescription previousDescription = description;
        description = newDescription;
        ServerDescriptionChangedEvent serverDescriptionChangedEvent = new ServerDescriptionChangedEvent(
                serverId, newDescription, previousDescription);
        if (!wouldDescriptionsGenerateEquivalentEvents(newDescription, previousDescription)) {
            serverListener.serverDescriptionChanged(serverDescriptionChangedEvent);
        }
        cluster.onChange(serverDescriptionChangedEvent);
    }

    private void handleException(final SdamIssue sdamIssue, final boolean beforeHandshake) {
        if (sdamIssue.stale(connectionPool, description)) {
            return;
        }
        if (sdamIssue.relatedToStateChange()) {
            updateDescription(sdamIssue.serverDescription());
            if (sdamIssue.serverIsLessThanVersionFourDotTwo() || sdamIssue.relatedToShutdown()) {
                connectionPool.invalidate(sdamIssue.exception().orElse(null));
            }
            serverMonitor.connect();
        } else if (sdamIssue.relatedToNetworkNotTimeout()
                || (beforeHandshake && (sdamIssue.relatedToNetworkTimeout() || sdamIssue.relatedToAuth()))) {
            updateDescription(sdamIssue.serverDescription());
            connectionPool.invalidate(sdamIssue.exception().orElse(null));
            serverMonitor.cancelCurrentCheck();
        } else if (sdamIssue.relatedToWriteConcern() || !sdamIssue.specific()) {
            updateDescription(sdamIssue.serverDescription());
            if (sdamIssue.serverIsLessThanVersionFourDotTwo()) {
                connectionPool.invalidate(sdamIssue.exception().orElse(null));
            }
            serverMonitor.connect();
        }
    }
}
