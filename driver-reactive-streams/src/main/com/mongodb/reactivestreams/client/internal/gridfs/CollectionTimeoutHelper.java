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

package com.mongodb.reactivestreams.client.internal.gridfs;

import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.internal.time.Timeout;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.MongoCollection;
import reactor.core.publisher.Mono;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

final class CollectionTimeoutHelper {
    private CollectionTimeoutHelper() {
        //NOP
    }

    public static <T> MongoCollection<T> collectionWithTimeout(final MongoCollection<T> collection,
                                                               @Nullable final Timeout timeout) {
        if (timeout != null && !timeout.isInfinite()) {
            long remainingMs = timeout.remaining(MILLISECONDS);
            if (remainingMs <= 0) {
                throw new MongoOperationTimeoutException("GridFS timed out");
            }
            return collection.withTimeout(remainingMs, MILLISECONDS);
        }
        return collection;
    }

    public static <T> Mono<MongoCollection<T>> collectionWithTimeoutMono(final MongoCollection<T> collection,
                                                                         @Nullable final Timeout timeout) {
        try {
            return Mono.just(collectionWithTimeout(collection, timeout));
        } catch (MongoOperationTimeoutException e) {
            return Mono.error(e);
        }
    }

    public static <T> Mono<MongoCollection<T>> collectionWithTimeoutDeferred(final MongoCollection<T> collection,
                                                                             @Nullable final Timeout timeout) {
        return Mono.defer(() -> collectionWithTimeoutMono(collection, timeout));
    }
}
