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
import org.bson.BsonDocument;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.isTrueArgument;

/**
 * An event representing the completion of a MongoDB database command.
 *
 * @since 3.1
 */
public final class CommandSucceededEvent extends CommandEvent {
    private final BsonDocument response;
    private final long elapsedTimeNanos;

    /**
     * Construct an instance.
     * @param requestContext
     * @param requestId the request id
     * @param connectionDescription the connection description
     * @param commandName the command name
     * @param response the command response
     * @param elapsedTimeNanos the non-negative elapsed time in nanoseconds for the operation to complete
     */
    public CommandSucceededEvent(final RequestContext requestContext, final int requestId, final ConnectionDescription connectionDescription,
            final String commandName, final BsonDocument response, final long elapsedTimeNanos) {
        super(requestContext, requestId, connectionDescription, commandName);
        this.response = response;
        isTrueArgument("elapsed time is not negative", elapsedTimeNanos >= 0);
        this.elapsedTimeNanos = elapsedTimeNanos;
    }

    /**
     * Gets the elapsed time in the given unit of time.
     *
     * @param timeUnit the time unit in which to get the elapsed time
     * @return the elapsed time
     */
    public long getElapsedTime(final TimeUnit timeUnit) {
        return timeUnit.convert(elapsedTimeNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Gets the response document. The document is only usable within the method that delivered the event.  If it's needed for longer, it
     * must be cloned via {@link Object#clone()}.
     *
     * @return the response document
     */
    public BsonDocument getResponse() {
        return response;
    }
}
