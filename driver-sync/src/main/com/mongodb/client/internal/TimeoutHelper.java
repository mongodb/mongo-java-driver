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

package com.mongodb.client.internal;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.time.Timeout;
import com.mongodb.lang.Nullable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class TimeoutHelper {
    private static final String DEFAULT_TIMEOUT_MESSAGE = "Operation exceeded the timeout limit.";

    private TimeoutHelper() {
        //NOP
    }

    public static <T> MongoCollection<T> collectionWithTimeout(final MongoCollection<T> collection,
                                                               final String message,
                                                               @Nullable final Timeout timeout) {
        // TODO-CSOT why not nanoseconds here, and below?
        return Timeout.run(timeout, MILLISECONDS,
                () -> collection,
                (ms) -> collection.withTimeout(ms, MILLISECONDS),
                () -> {
                    throw TimeoutContext.createMongoTimeoutException(message);
                },
                () -> collection);
    }

    public static <T> MongoCollection<T> collectionWithTimeout(final MongoCollection<T> collection,
                                                               @Nullable final Timeout timeout) {
        return collectionWithTimeout(collection, DEFAULT_TIMEOUT_MESSAGE, timeout);
    }

    public static MongoDatabase databaseWithTimeout(final MongoDatabase database,
                                                    final String message,
                                                    @Nullable final Timeout timeout) {
        return Timeout.run(timeout, MILLISECONDS,
                () -> database,
                (ms) -> database.withTimeout(ms, MILLISECONDS),
                () -> {
                    throw TimeoutContext.createMongoTimeoutException(message);
                },
                () -> database);
    }

    public static MongoDatabase databaseWithTimeout(final MongoDatabase database,
                                                    @Nullable final Timeout timeout) {
        return databaseWithTimeout(database, DEFAULT_TIMEOUT_MESSAGE, timeout);
    }

}
