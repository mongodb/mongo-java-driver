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
 * An event signifying when a connection pool is cleared.
 *
 * @since 4.0
 */
public final class ConnectionPoolClearedEvent {
    private final ServerId serverId;

    /**
     * Constructs a new instance of the event.
     *
     * @param serverId the server id
     */
    public ConnectionPoolClearedEvent(final ServerId serverId) {
        this.serverId = notNull("serverId", serverId);
    }

    /**
     * Gets the server id
     *
     * @return the server id
     */
    public ServerId getServerId() {
        return serverId;
    }

    @Override
    public String toString() {
        return "ConnectionPoolClearedEvent{"
                       + "serverId=" + serverId
                       + '}';
    }
}
