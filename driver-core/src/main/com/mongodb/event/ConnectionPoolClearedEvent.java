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
import com.mongodb.lang.Nullable;
import org.bson.types.ObjectId;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * An event signifying when a connection pool is cleared.
 *
 * @since 4.0
 */
public final class ConnectionPoolClearedEvent {
    @Nullable private final ServerId serverId;
    private final ObjectId serviceId;

    /**
     * Constructs a new instance of the event.
     *
     * @param serverId the server id
     */
    public ConnectionPoolClearedEvent(final ServerId serverId) {
        this(serverId, null);
    }

    /**
     * Constructs a new instance of the event.
     *
     * @param serverId the server id
     * @param serviceId the service id, which may be null
     * @since 4.3
     */
    public ConnectionPoolClearedEvent(final ServerId serverId, @Nullable final ObjectId serviceId) {
        this.serverId = notNull("serverId", serverId);
        this.serviceId = serviceId;
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
     * Gets the service id.
     *
     * <p>
     *     When connected to a load balancer, in some cases the driver clears only a subset of connections in the pool, based on the
     *     service id reported on the connection on which the error occurred.
     * </p>
     *
     * @return the service id, which may be null
     * @since 4.3
     */
    @Nullable
    public ObjectId getServiceId() {
        return serviceId;
    }

    @Override
    public String toString() {
        return "ConnectionPoolClearedEvent{"
                + "serverId=" + serverId
                + ", serviceId=" + serviceId
                + '}';
    }
}
