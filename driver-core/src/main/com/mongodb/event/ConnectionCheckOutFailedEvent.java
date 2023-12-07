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

import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ServerId;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * An event for when checking out a connection fails.
 *
 * @since 4.0
 */
public final class ConnectionCheckOutFailedEvent {

    /**
     * An enumeration of the reasons checking out a connection failed
     */
    public enum Reason {
        /**
         * The pool was previously closed and cannot provide new connections
         */
        POOL_CLOSED,

        /**
         * The connection check out attempt exceeded the specific timeout
         */
        TIMEOUT,

        /**
         * The connection check out attempt experienced an error while setting up a new connection
         */
        CONNECTION_ERROR,

        /**
         * Reason unknown
         */
        UNKNOWN,
    }

    private final ServerId serverId;
    private final long operationId;
    private final Reason reason;
    private final long elapsedTimeNanos;

    /**
     * Constructs an instance.
     *
     * @param serverId The server ID. See {@link #getServerId()}.
     * @param operationId The operation ID. See {@link #getOperationId()}.
     * @param reason The reason the connection check out failed. See {@link #getReason()}.
     * @param elapsedTimeNanos The time it took while trying to check out the connection. See {@link #getElapsedTime(TimeUnit)}.
     * @since 4.11
     */
    public ConnectionCheckOutFailedEvent(final ServerId serverId, final long operationId, final Reason reason, final long elapsedTimeNanos) {
        this.serverId = notNull("serverId", serverId);
        this.operationId = operationId;
        this.reason = notNull("reason", reason);
        isTrueArgument("waited time is not negative", elapsedTimeNanos >= 0);
        this.elapsedTimeNanos = elapsedTimeNanos;
    }

    /**
     * Gets the server id
     *
     * @return the server id
     */
    public ServerId getServerId() {
        return serverId;
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
     * Gets the reason for the check out failure.
     *
     * @return the reason
     * @since 4.3
     */
    public Reason getReason() {
        return reason;
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
     * This duration does not currently include the time to deliver the {@link ConnectionCheckOutStartedEvent}.
     * Subject to change.</p>
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
        return "ConnectionCheckOutFailedEvent{"
                + "server=" + serverId.getAddress()
                + ", clusterId=" + serverId.getClusterId()
                + ", operationId=" + operationId
                + ", reason=" + reason
                + ", elapsedTimeNanos=" + elapsedTimeNanos
                + '}';
    }
}
