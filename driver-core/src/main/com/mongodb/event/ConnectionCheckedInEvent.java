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

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * An event for checking in a connection to the pool.
 *
 * @since 3.5
 */
public final class ConnectionCheckedInEvent {
    private final ConnectionId connectionId;
    private final long operationId;
    private final long elapsedTimeNanos;


    /**
     * Construct an instance
     *
     * @param connectionId the connectionId
     * @param operationId the operation id
     * @param elapsedTimeNanos the elapsed time between check out and check in
     * @since 4.10
     */
    public ConnectionCheckedInEvent(final ConnectionId connectionId, final long operationId, final long elapsedTimeNanos) {
        this.connectionId = notNull("connectionId", connectionId);
        this.operationId = operationId;
        this.elapsedTimeNanos = elapsedTimeNanos;
    }

    /**
     * Construct an instance
     *
     * @param connectionId the connectionId
     * @deprecated Prefer {@link ConnectionCheckedInEvent#ConnectionCheckedInEvent(ConnectionId, long, long)}
     */
    @Deprecated
    public ConnectionCheckedInEvent(final ConnectionId connectionId) {
        this(connectionId, -1, -1);
    }

    /**
     * Gets the connection id
     *
     * @return the connection id
     */
    public ConnectionId getConnectionId() {
        return connectionId;
    }

    /**
     * Gets the operation identifier
     *
     * @return the operation identifier
     * @since 4.10
     */
    public long getOperationId() {
        return operationId;
    }

    /**
     * Gets the elapsed time between check out and check in.
     *
     * @param timeUnit the time unit
     * @return the elapsed time
     * @since 4.10
     */
    public long getElapsedTime(final TimeUnit timeUnit) {
        return timeUnit.convert(elapsedTimeNanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public String toString() {
        return "ConnectionCheckedInEvent{"
                + "connectionId=" + connectionId
                + ", server=" + connectionId.getServerId().getAddress()
                + ", clusterId=" + connectionId.getServerId().getClusterId()
                + ", operationId=" + operationId
                + ", elapsedTimeNanos=" + elapsedTimeNanos
                + '}';
    }
}
