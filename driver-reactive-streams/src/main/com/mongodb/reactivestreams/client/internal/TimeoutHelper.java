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

package com.mongodb.reactivestreams.client.internal;

import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.time.Timeout;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import reactor.core.publisher.Mono;

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
                                                               @Nullable final Timeout timeout) {
        return collectionWithTimeout(collection, timeout, DEFAULT_TIMEOUT_MESSAGE);
    }

    public static <T> MongoCollection<T> collectionWithTimeout(final MongoCollection<T> collection,
                                                               @Nullable final Timeout timeout,
                                                               final String message) {
        if (timeout != null) {
            return timeout.call(MILLISECONDS,
                    () -> collection.withTimeout(0, MILLISECONDS),
                    ms -> collection.withTimeout(ms, MILLISECONDS),
                    () -> TimeoutContext.throwMongoTimeoutException(message));
        }
        return collection;
    }

    public static <T> Mono<MongoCollection<T>> collectionWithTimeoutMono(final MongoCollection<T> collection,
                                                                         @Nullable final Timeout timeout) {
       return collectionWithTimeoutMono(collection, timeout, DEFAULT_TIMEOUT_MESSAGE);
    }

    public static <T> Mono<MongoCollection<T>> collectionWithTimeoutMono(final MongoCollection<T> collection,
                                                                         @Nullable final Timeout timeout,
                                                                         final String message) {
        try {
            return Mono.just(collectionWithTimeout(collection, timeout, message));
        } catch (MongoOperationTimeoutException e) {
            return Mono.error(e);
        }
    }

    public static <T> Mono<MongoCollection<T>> collectionWithTimeoutDeferred(final MongoCollection<T> collection,
                                                                             @Nullable final Timeout timeout) {
        return collectionWithTimeoutDeferred(collection, timeout, DEFAULT_TIMEOUT_MESSAGE);
    }

    public static <T> Mono<MongoCollection<T>> collectionWithTimeoutDeferred(final MongoCollection<T> collection,
                                                                             @Nullable final Timeout timeout,
                                                                             final String message) {
        return Mono.defer(() -> collectionWithTimeoutMono(collection, timeout, message));
    }

    public static MongoDatabase databaseWithTimeout(final MongoDatabase database,
                                                    @Nullable final Timeout timeout) {
        return databaseWithTimeout(database, DEFAULT_TIMEOUT_MESSAGE, timeout);
    }

    public static MongoDatabase databaseWithTimeout(final MongoDatabase database,
                                                    final String message,
                                                    @Nullable final Timeout timeout) {
        if (timeout != null) {
            return timeout.call(MILLISECONDS,
                    () -> database.withTimeout(0, MILLISECONDS),
                    ms -> database.withTimeout(ms, MILLISECONDS),
                    () -> TimeoutContext.throwMongoTimeoutException(message));
        }
        return database;
    }

    private static Mono<MongoDatabase> databaseWithTimeoutMono(final MongoDatabase database,
                                                               final String message,
                                                               @Nullable final Timeout timeout) {
        try {
            return Mono.just(databaseWithTimeout(database, message, timeout));
        } catch (MongoOperationTimeoutException e) {
            return Mono.error(e);
        }
    }

    public static Mono<MongoDatabase> databaseWithTimeoutDeferred(final MongoDatabase database,
                                                                  @Nullable final Timeout timeout) {
        return databaseWithTimeoutDeferred(database, DEFAULT_TIMEOUT_MESSAGE, timeout);
    }

    public static Mono<MongoDatabase> databaseWithTimeoutDeferred(final MongoDatabase database,
                                                                  final String message,
                                                                  @Nullable final Timeout timeout) {
        return Mono.defer(() -> databaseWithTimeoutMono(database, message, timeout));
    }
}
