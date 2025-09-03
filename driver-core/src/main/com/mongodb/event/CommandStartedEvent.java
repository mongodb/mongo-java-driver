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
import org.bson.BsonDocument;

/**
 * An event representing the start of a command execution.
 *
 * @since 3.1
 */
public final class CommandStartedEvent extends CommandEvent {
    private final BsonDocument command;

    /**
     * Construct an instance.
     *
     * @param requestContext        the request context
     * @param operationId           the operation id
     * @param requestId             the request id
     * @param connectionDescription the connection description
     * @param databaseName          the database name
     * @param commandName           the command name
     * @param command               the command as a BSON document
     * @since 4.10
     */
    public CommandStartedEvent(@Nullable final RequestContext requestContext, final long operationId, final int requestId,
            final ConnectionDescription connectionDescription, final String databaseName, final String commandName,
            final BsonDocument command) {
        super(requestContext, operationId, requestId, connectionDescription, databaseName, commandName);
        this.command = command;
    }

    /**
     * Gets the command document. The document is only usable within the method that delivered the event.  If it's needed for longer, it
     * must be cloned via {@link Object#clone()}.
     *
     * @return the command document
     */
    public BsonDocument getCommand() {
        return command;
    }
}
