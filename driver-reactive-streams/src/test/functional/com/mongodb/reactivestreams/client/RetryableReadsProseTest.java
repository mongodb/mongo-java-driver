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

package com.mongodb.reactivestreams.client;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.RetryableWritesProseTest;
import com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static com.mongodb.client.model.Filters.eq;

/**
 * See
 * <a href="https://github.com/mongodb/specifications/tree/master/source/retryable-reads/tests">Retryable Reads Tests</a>.
 */
final class RetryableReadsProseTest {
    /**
     * See
     * <a href="https://github.com/mongodb/specifications/tree/master/source/retryable-reads/tests#poolclearederror-retryability-test">
     * PoolClearedError Retryability Test</a>.
     */
    @Test
    void poolClearedExceptionMustBeRetryable() throws InterruptedException, ExecutionException, TimeoutException {
        RetryableWritesProseTest.poolClearedExceptionMustBeRetryable(
                mongoClientSettings -> new SyncMongoClient(MongoClients.create(mongoClientSettings)),
                mongoCollection -> mongoCollection.find(eq(0)).iterator().hasNext(), "find", false);
    }

    /**
     * See
     * <a href="https://github.com/mongodb/specifications/tree/master/source/retryable-reads/tests#21-retryable-reads-are-retried-on-a-different-mongos-when-one-is-available">
     * Retryable Reads Are Retried on a Different mongos When One is Available</a>.
     */
    @Test
    void retriesOnDifferentMongosWhenAvailable() {
        RetryableWritesProseTest.retriesOnDifferentMongosWhenAvailable(
                mongoClientSettings -> new SyncMongoClient(MongoClients.create(mongoClientSettings)),
                mongoCollection -> {
                    try (MongoCursor<Document> cursor = mongoCollection.find().iterator()) {
                        return cursor.hasNext();
                    }
                }, "find", false);
    }

    /**
     * See
     * <a href="https://github.com/mongodb/specifications/tree/master/source/retryable-reads/tests#22-retryable-reads-are-retried-on-the-same-mongos-when-no-others-are-available">
     * Retryable Reads Are Retried on the Same mongos When No Others are Available</a>.
     */
    @Test
    void retriesOnSameMongosWhenAnotherNotAvailable() {
        RetryableWritesProseTest.retriesOnSameMongosWhenAnotherNotAvailable(
                mongoClientSettings -> new SyncMongoClient(MongoClients.create(mongoClientSettings)),
                mongoCollection -> {
                    try (MongoCursor<Document> cursor = mongoCollection.find().iterator()) {
                        return cursor.hasNext();
                    }
                }, "find", false);
    }
}
