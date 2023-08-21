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
import com.mongodb.connection.ConnectionPoolSettings;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * An event for checking out a connection from the pool.
 *
 * @since 3.5
 */
public final class ConnectionCheckedOutEvent {
    private final ConnectionId connectionId;
    private final long operationId;
    private final long elapsedTimeNanos;

    /**
     * Constructs an instance.
     *
     * @param connectionId The connection ID. See {@link #getConnectionId()}.
     * @param operationId The operation ID. See {@link #getOperationId()}.
     * @param elapsedTimeNanos The time it took to check out the connection. See {@link #getElapsedTime(TimeUnit)}.
     * @since 4.11
     */
    public ConnectionCheckedOutEvent(final ConnectionId connectionId, final long operationId, final long elapsedTimeNanos) {
        this.connectionId = notNull("connectionId", connectionId);
        this.operationId = operationId;
        isTrueArgument("waited time is not negative", elapsedTimeNanos >= 0);
        this.elapsedTimeNanos = elapsedTimeNanos;
    }

    /**
     * Construct an instance
     *
     * @param connectionId the connectionId
     * @param operationId the operation id
     * @since 4.10
     * @deprecated Prefer {@link ConnectionCheckedOutEvent#ConnectionCheckedOutEvent(ConnectionId, long, long)}.
     * If this constructor is used, then {@link #getElapsedTime(TimeUnit)} is 0.
     */
    @Deprecated
    public ConnectionCheckedOutEvent(final ConnectionId connectionId, final long operationId) {
        this(connectionId, operationId, 0);
    }

    /**
     * Construct an instance
     *
     * @param connectionId the connectionId
     * @deprecated Prefer {@link #ConnectionCheckedOutEvent(ConnectionId, long)}.
     * If this constructor is used, then {@link #getOperationId()} is -1.
     */
    @Deprecated
    public ConnectionCheckedOutEvent(final ConnectionId connectionId) {
        this(connectionId, -1);
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
     * The time it took to check out the connection.
     * More specifically, the time elapsed between the {@link ConnectionCheckOutStartedEvent} emitted by the same checking out and this event.
     * <p>
     * Naturally, if a new connection was not {@linkplain ConnectionCreatedEvent created}
     * and {@linkplain ConnectionReadyEvent established} as part of checking out,
     * this duration is usually not greater than {@link ConnectionPoolSettings#getMaxWaitTime(TimeUnit)},
     * but may occasionally be greater than that, because the driver does not provide hard real-time guarantees.</p>
     * <p>
     * This duration does not include the time to deliver the {@link ConnectionCheckOutStartedEvent}.
     * Be warned that this may change.</p>
     *
     * @param timeUnit The time unit of the result.
     * {@link TimeUnit#convert(long, TimeUnit)} specifies how the conversion from nanoseconds to {@code timeUnit} is done.
     * @return The time it took to establish the connection.
     * @since 4.11
     */
    public long getElapsedTime(final TimeUnit timeUnit) {
        return timeUnit.convert(elapsedTimeNanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public String toString() {
        return "ConnectionCheckedOutEvent{"
                + "connectionId=" + connectionId
                + ", server=" + connectionId.getServerId().getAddress()
                + ", clusterId=" + connectionId.getServerId().getClusterId()
                + ", operationId=" + operationId
                + ", elapsedTimeNanos=" + elapsedTimeNanos
                + '}';
    }
}
