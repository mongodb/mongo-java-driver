/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.mongodb.event;

import com.mongodb.annotations.Beta;
import com.mongodb.connection.ConnectionId;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * An event for adding a new connection to the pool.
 */
@Beta
public final class ConnectionAddedEvent {
    private final ConnectionId connectionId;

    /**
     * Construct an instance
     *
     * @param connectionId the connectionId
     */
    public ConnectionAddedEvent(final ConnectionId connectionId) {
        this.connectionId = notNull("connectionId", connectionId);
    }

    /**
     * Gets the connection id
     *
     * @return the connection id
     */
    public ConnectionId getConnectionId() {
        return connectionId;
    }

    @Override
    public String toString() {
        return "ConnectionAddedEvent{"
                       + "connectionId=" + connectionId
                       + '}';
    }
}
