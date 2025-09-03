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

package com.mongodb.event;

import com.mongodb.connection.ConnectionId;
import com.mongodb.connection.ServerMonitoringMode;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * An event for the start of a server heartbeat.
 *
 * @since 3.3
 */
public final class ServerHeartbeatStartedEvent {
    private final ConnectionId connectionId;
    private final boolean awaited;

    /**
     * Construct an instance.
     *
     * @param connectionId the non-null connnectionId
     * @param awaited {@code true} if and only if the heartbeat is for an awaitable `hello` / legacy hello.
     * @since 5.1
     */
    public ServerHeartbeatStartedEvent(final ConnectionId connectionId, final boolean awaited) {
        this.connectionId = notNull("connectionId", connectionId);
        this.awaited = awaited;
    }

    /**
     * Construct an instance.
     *
     * @param connectionId the non-null connnectionId
     * @deprecated Prefer {@link #ServerHeartbeatStartedEvent(ConnectionId, boolean)}.
     * If this constructor is used then {@link #isAwaited()} is {@code false}.
     */
    @Deprecated
    public ServerHeartbeatStartedEvent(final ConnectionId connectionId) {
        this(connectionId, false);
    }

    /**
     * Gets the connectionId.
     *
     * @return the connectionId
     */
    public ConnectionId getConnectionId() {
        return connectionId;
    }

    /**
     * Gets whether the heartbeat is for an awaitable `hello` / legacy hello.
     *
     * @return {@code true} if and only if the heartbeat is for an awaitable `hello` / legacy hello.
     * @see ServerMonitoringMode#STREAM
     * @since 5.1
     */
    public boolean isAwaited() {
        return awaited;
    }

    @Override
    public String toString() {
        return "ServerHeartbeatStartedEvent{"
                + "connectionId=" + connectionId
                + ", server=" + connectionId.getServerId().getAddress()
                + ", clusterId=" + connectionId.getServerId().getClusterId()
                + ", awaited=" + awaited
                + "} " + super.toString();
    }
}
