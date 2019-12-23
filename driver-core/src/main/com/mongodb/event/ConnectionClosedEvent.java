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

import static com.mongodb.assertions.Assertions.notNull;

/**
 * An event for when a connection pool closes a connection.
 *
 * @since 4.0
 */
public final class ConnectionClosedEvent {

    /**
     * An enumeration of the reasons a connection could be closed
     */
    public enum Reason {
        /**
         * The connection is no longer valid because the pool has been cleared
         */
        STALE,

        /**
         * The connection became stale by being idle for too long
         */
        IDLE,

        /**
         * The connection experienced an error, making it no longer valid
         */
        ERROR,

        /**
         * The pool was closed, making the connection no longer valid
         */
        POOL_CLOSED,
    }

    private final ConnectionId connectionId;
    private final Reason reason;

    /**
     * Construct an instance
     *
     * @param connectionId the connection id
     * @param reason the reason the connection was closed
     */
    public ConnectionClosedEvent(final ConnectionId connectionId, final Reason reason) {
        this.connectionId = notNull("connectionId", connectionId);
        this.reason = notNull("reason", reason);
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
     * Get the reason the connection was removed.
     *
     * @return the reason
     */
    public Reason getReason() {
        return reason;
    }

    @Override
    public String toString() {
        return "ConnectionClosedEvent{"
                       + " connectionId=" + connectionId
                       + " reason=" + reason
                       + '}';
    }
}
