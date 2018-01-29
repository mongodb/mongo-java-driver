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
 * An event for server heartbeat failures.
 *
 * @since 3.3
 */
public final class ServerHeartbeatFailedEvent {
    private final ConnectionId connectionId;
    private final long elapsedTimeNanos;
    private final Throwable throwable;

    /**
     * Construct an instance.
     *
     * @param connectionId the non-null connectionId
     * @param elapsedTimeNanos the non-negative elapsed time in nanoseconds
     * @param throwable the non-null exception that caused the failure
     */
    public ServerHeartbeatFailedEvent(final ConnectionId connectionId, final long elapsedTimeNanos, final Throwable throwable) {
        this.connectionId = notNull("connectionId", connectionId);
        isTrueArgument("elapsed time is not negative", elapsedTimeNanos >= 0);
        this.elapsedTimeNanos = elapsedTimeNanos;
        this.throwable = notNull("throwable", throwable);
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
     * Gets the elapsed time in the given time unit.
     *
     * @param timeUnit the non-null timeUnit
     *
     * @return the elapsed time in the given time unit
     */
    public long getElapsedTime(final TimeUnit timeUnit) {
        return timeUnit.convert(elapsedTimeNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Gets the exceptions that caused the failure
     *
     * @return the exception
     */
    public Throwable getThrowable() {
        return throwable;
    }

    @Override
    public String toString() {
        return "ServerHeartbeatFailedEvent{"
                + "connectionId=" + connectionId
                + ", elapsedTimeNanos=" + elapsedTimeNanos
                + ", throwable=" + throwable
                + "} " + super.toString();
    }
}
