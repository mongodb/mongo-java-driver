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

import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerId;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * An event for changes to the description of a server.
 *
 * @since 3.3
 */
public final class ServerDescriptionChangedEvent {

    private final ServerId serverId;
    private final ServerDescription newDescription;
    private final ServerDescription previousDescription;

    /**
     * Construct an instance.
     *
     * @param serverId            the non-null serverId
     * @param newDescription      the non-null new description
     * @param previousDescription the non-null previous description
     */
    public ServerDescriptionChangedEvent(final ServerId serverId, final ServerDescription newDescription,
                                         final ServerDescription previousDescription) {
        this.serverId = notNull("serverId", serverId);
        this.newDescription = notNull("newDescription", newDescription);
        this.previousDescription = notNull("previousDescription", previousDescription);
    }


    /**
     * Gets the serverId.
     *
     * @return the serverId
     */
    public ServerId getServerId() {
        return serverId;
    }

    /**
     * Gets the new server description.
     *
     * @return the new server description
     */
    public ServerDescription getNewDescription() {
        return newDescription;
    }

    /**
     * Gets the previous server description.
     *
     * @return the previous server description
     */
    public ServerDescription getPreviousDescription() {
        return previousDescription;
    }

    @Override
    public String toString() {
        return "ServerDescriptionChangedEvent{"
                       + "serverId=" + serverId
                       + ", newDescription=" + newDescription
                       + ", previousDescription=" + previousDescription
                       + '}';
    }
}
