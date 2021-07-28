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

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.isTrueArgument;

/**
 * An event representing the failure of a MongoDB database command.
 *
 * @since 3.1
 */
public final class CommandFailedEvent extends CommandEvent {

    private final long elapsedTimeNanos;
    private final Throwable throwable;

    /**
     * Construct an instance.
     * @param requestContext the request context
     * @param requestId the requestId
     * @param connectionDescription the connection description
     * @param commandName the command name
     * @param elapsedTimeNanos the non-negative elapsed time in nanoseconds for the operation to complete
     * @param throwable the throwable cause of the failure
     */
    public CommandFailedEvent(final RequestContext requestContext, final int requestId, final ConnectionDescription connectionDescription,
            final String commandName, final long elapsedTimeNanos, final Throwable throwable) {
        super(requestContext, requestId, connectionDescription, commandName);
        isTrueArgument("elapsed time is not negative", elapsedTimeNanos >= 0);
        this.elapsedTimeNanos = elapsedTimeNanos;
        this.throwable = throwable;
    }

    /**
     * Construct an instance.
     * @param requestId the requestId
     * @param connectionDescription the connection description
     * @param commandName the command name
     * @param elapsedTimeNanos the non-negative elapsed time in nanoseconds for the operation to complete
     * @param throwable the throwable cause of the failure
     */
    public CommandFailedEvent(final int requestId, final ConnectionDescription connectionDescription,
            final String commandName, final long elapsedTimeNanos, final Throwable throwable) {
        this(null, requestId, connectionDescription, commandName, elapsedTimeNanos, throwable);
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
     * Gets the throwable cause of the failure
     *
     * @return the throwable cause of the failuer
     */
    public Throwable getThrowable() {
        return throwable;
    }
}
