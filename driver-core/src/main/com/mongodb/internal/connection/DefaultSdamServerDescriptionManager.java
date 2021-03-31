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
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.event.ServerDescriptionChangedEvent;
import com.mongodb.event.ServerListener;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.connection.ServerType.UNKNOWN;
import static com.mongodb.internal.connection.EventHelper.wouldDescriptionsGenerateEquivalentEvents;
import static com.mongodb.internal.connection.ServerDescriptionHelper.unknownConnectingServerDescription;

@ThreadSafe
final class DefaultSdamServerDescriptionManager implements SdamServerDescriptionManager {
    private final ServerId serverId;
    private final ServerDescriptionChangedListener serverDescriptionChangedListener;
    private final ServerListener serverListener;
    private final ServerMonitor serverMonitor;
    private final ConnectionPool connectionPool;
    private final Lock lock;
    private volatile ServerDescription description;

    DefaultSdamServerDescriptionManager(final ServerId serverId,
                                        final ServerDescriptionChangedListener serverDescriptionChangedListener,
                                        final ServerListener serverListener, final ServerMonitor serverMonitor,
                                        final ConnectionPool connectionPool) {
        this.serverId = assertNotNull(serverId);
        this.serverDescriptionChangedListener = assertNotNull(serverDescriptionChangedListener);
        this.serverListener = assertNotNull(serverListener);
        this.serverMonitor = assertNotNull(serverMonitor);
        this.connectionPool = assertNotNull(connectionPool);
        description = unknownConnectingServerDescription(serverId, null);
        lock = new ReentrantLock();
    }

    @Override
    public void update(final ServerDescription candidateDescription) {
        lock.lock();
        try {
            if (TopologyVersionHelper.newer(description.getTopologyVersion(), candidateDescription.getTopologyVersion())) {
                return;
            }
            /* A paused pool should not be exposed and used. Calling `ready` before updating description and calling `invalidate` after
             * facilitates achieving this. However, because once the pool is observed, it may be used concurrently with the pool being
             * invalidated by either the current method or the `handleException` method, the pool still may be used in a paused state.
             * For those cases `MongoConnectionPoolClearedException` was introduced. */
            if (candidateDescription.getType().isDataBearing()) {
                connectionPool.ready();
            }
            updateDescription(candidateDescription);
            if (candidateDescription.getException() != null) {
                assertTrue(candidateDescription.getType() == UNKNOWN);
                connectionPool.invalidate();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void handleExceptionBeforeHandshake(final SdamIssue sdamIssue) {
        lock.lock();
        try {
            handleException(sdamIssue, true);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void handleExceptionAfterHandshake(final SdamIssue sdamIssue) {
        lock.lock();
        try {
            handleException(sdamIssue, false);
        } finally {
            lock.unlock();
        }
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
        serverDescriptionChangedListener.serverDescriptionChanged(serverDescriptionChangedEvent);
        if (!wouldDescriptionsGenerateEquivalentEvents(newDescription, previousDescription)) {
            serverListener.serverDescriptionChanged(serverDescriptionChangedEvent);
        }
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
