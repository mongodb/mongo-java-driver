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

import com.mongodb.RequestContext;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.lang.Nullable;

/**
 * An event representing a MongoDB database command.
 *
 * @since 3.1
 */
public abstract class CommandEvent {
    private final RequestContext requestContext;
    private final int requestId;
    private final ConnectionDescription connectionDescription;
    private final String commandName;

    /**
     * Construct an instance.
     * @param requestContext the request context
     * @param requestId the request id
     * @param connectionDescription the connection description
     * @param commandName the command name
     * @since 4.4
     */
    public CommandEvent(final RequestContext requestContext, final int requestId, final ConnectionDescription connectionDescription,
            final String commandName) {
        this.requestContext = requestContext;
        this.requestId = requestId;
        this.connectionDescription = connectionDescription;
        this.commandName = commandName;
    }

    /**
     * Construct an instance.
     * @param requestId the request id
     * @param connectionDescription the connection description
     * @param commandName the command name
     */
    public CommandEvent(final int requestId, final ConnectionDescription connectionDescription, final String commandName) {
        this(null, requestId, connectionDescription, commandName);
    }

    /**
     * Gets the request identifier
     *
     * @return the request identifier
     */
    public int getRequestId() {
        return requestId;
    }

    /**
     * Gets the description of the connection to which the operation will be sent.
     *
     * @return the connection description
     */
    public ConnectionDescription getConnectionDescription() {
        return connectionDescription;
    }

    /**
     * Gets the name of the command.
     *
     * @return the command name
     */
    public String getCommandName() {
        return commandName;
    }

    /**
     * Gets the request context associated with this event.
     *
     * @return the request context
     * @since 4.4
     */
    @Nullable
    public RequestContext getRequestContext() {
        return requestContext;
    }
}


