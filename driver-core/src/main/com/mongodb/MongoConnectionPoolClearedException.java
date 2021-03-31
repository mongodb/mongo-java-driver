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
package com.mongodb;

import com.mongodb.connection.ServerId;
import com.mongodb.lang.Nullable;

import static com.mongodb.assertions.Assertions.assertNotNull;

/* This exception is our way to deal with a race condition existing due to threads concurrently
 * checking out connections from ConnectionPool and invalidating it.*/
/**
 * An exception that may happen usually as a result of another thread clearing a connection pool.
 * Such clearing usually itself happens as a result of an exception,
 * in which case it may be specified via the {@link #getCause()} method.
 */
public final class MongoConnectionPoolClearedException extends MongoException {
    private static final long serialVersionUID = 1;

    /**
     * Not part of the public API.
     *
     * @param connectionPoolServerId A {@link ServerId} specifying the server used by the connection pool that creates a new exception.
     * @param cause The {@linkplain #getCause() cause}.
     */
    public MongoConnectionPoolClearedException(final ServerId connectionPoolServerId, @Nullable final Throwable cause) {
        super("Connection pool for " + assertNotNull(connectionPoolServerId) + " is paused"
                + (cause == null ? "" : " because another operation failed"), cause);
    }
}
