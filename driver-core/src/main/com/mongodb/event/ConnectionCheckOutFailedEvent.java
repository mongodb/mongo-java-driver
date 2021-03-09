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

import com.mongodb.connection.ServerId;

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
    private final Reason reason;

    /**
     * Construct an instance
     *
     * @param serverId the server id
     * @param reason the reason the connection check out failed
     */
    public ConnectionCheckOutFailedEvent(final ServerId serverId, final Reason reason) {
        this.serverId = notNull("serverId", serverId);
        this.reason = notNull("reason", reason);
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
     * Gets the reason for the check out failure.
     *
     * @return the reason
     * @since 4.3
     */
    public Reason getReason() {
        return reason;
    }

    @Override
    public String toString() {
        return "ConnectionCheckOutFailedEvent{"
                       + "serverId=" + serverId
                       + " reason=" + reason
                       + '}';
    }
}
