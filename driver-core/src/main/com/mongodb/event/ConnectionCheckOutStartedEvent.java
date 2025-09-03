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
 * An event for when the driver starts to check out a connection.
 *
 * @since 4.0
 */
public final class ConnectionCheckOutStartedEvent {
    private final ServerId serverId;
    private final long operationId;

    /**
     * Construct an instance
     *
     * @param serverId the server id
     * @param operationId the operation id
     * @since 4.10
     */
    public ConnectionCheckOutStartedEvent(final ServerId serverId, final long operationId) {
        this.serverId = notNull("serverId", serverId);
        this.operationId = operationId;
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

    @Override
    public String toString() {
        return "ConnectionCheckOutStartedEvent{"
                + "server=" + serverId.getAddress()
                + ", clusterId=" + serverId.getClusterId()
                + ", operationId=" + operationId
                + '}';
    }
}
