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
 * Such a connection is considered in use until it becomes {@linkplain ConnectionCheckedInEvent available}.
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
     * More specifically, the time elapsed between emitting a {@link ConnectionCheckOutStartedEvent}
     * and emitting this event as part of the same checking out.
     * <p>
     * Naturally, if a new connection was not {@linkplain ConnectionCreatedEvent created}
     * and {@linkplain ConnectionReadyEvent established} as part of checking out,
     * this duration is usually not greater than {@link ConnectionPoolSettings#getMaxWaitTime(TimeUnit)},
     * but may occasionally be greater than that, because the driver does not provide hard real-time guarantees.</p>
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
