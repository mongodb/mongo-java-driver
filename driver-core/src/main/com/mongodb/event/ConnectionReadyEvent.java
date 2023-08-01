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

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * An event for when a connection in the pool has finished being established and is ready for use.
 *
 * @since 4.0
 */
public final class ConnectionReadyEvent {
    private final ConnectionId connectionId;
    private final long elapsedTimeNanos;

    /**
     * Constructs an instance.
     *
     * @param connectionId The connection ID. See {@link #getConnectionId()}.
     * @param elapsedTimeNanos The time it took to establish the connection. See {@link #getElapsedTime(TimeUnit)}.
     * @since VAKOTODO
     */
    public ConnectionReadyEvent(final ConnectionId connectionId, final long elapsedTimeNanos) {
        this.connectionId = notNull("connectionId", connectionId);
        isTrueArgument("elapsed time is not negative", elapsedTimeNanos >= 0);
        this.elapsedTimeNanos = elapsedTimeNanos;
    }

    /**
     * Construct an instance
     *
     * @param connectionId the connection id
     * @deprecated Prefer {@link ConnectionReadyEvent#ConnectionReadyEvent(ConnectionId, long)}.
     * If this constructor is used, then {@link #getElapsedTime(TimeUnit)} is 0.
     */
    @Deprecated
    public ConnectionReadyEvent(final ConnectionId connectionId) {
        this(connectionId, 0);
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
     * The time it took to establish the connection.
     * More specifically, the time elapsed between the {@link ConnectionCreatedEvent} emitted by the same checking out and this event.
     * <p>
     * Naturally, when establishing a connection is part of checking out,
     * this duration is not greater than
     * {@link ConnectionCheckedOutEvent#getElapsedTime(TimeUnit)}/{@link ConnectionCheckOutFailedEvent#getElapsedTime(TimeUnit)}.</p>
     * <p>
     * This duration does not include the time to deliver the {@link ConnectionCreatedEvent}.
     * Be warned that this may change.</p>
     *
     * @param timeUnit The time unit of the result.
     * {@link TimeUnit#convert(long, TimeUnit)} specifies how the conversion from nanoseconds to {@code timeUnit} is done.
     * @return The time it took to establish the connection.
     * @since VAKOTODO
     */
    public long getElapsedTime(final TimeUnit timeUnit) {
        return timeUnit.convert(elapsedTimeNanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public String toString() {
        return "ConnectionReadyEvent{"
                + "connectionId=" + connectionId
                + ", server=" + connectionId.getServerId().getAddress()
                + ", clusterId=" + connectionId.getServerId().getClusterId()
                + ", elapsedTimeNanos=" + elapsedTimeNanos
                + '}';
    }
}
