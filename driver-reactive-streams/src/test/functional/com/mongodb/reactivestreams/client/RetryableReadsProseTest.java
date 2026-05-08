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

import static com.mongodb.client.model.Filters.eq;

/**
 * <a href="https://github.com/mongodb/specifications/blob/master/source/retryable-reads/tests/README.md#prose-tests">
 * Prose Tests</a>.
 */
final class RetryableReadsProseTest {
    /**
     * <a href="https://github.com/mongodb/specifications/blob/master/source/retryable-reads/tests/README.md#1-poolclearederror-retryability-test">
     * 1. PoolClearedError Retryability Test</a>.
     */
    @Test
    void poolClearedExceptionMustBeRetryable() throws Exception {
        RetryableWritesProseTest.poolClearedExceptionMustBeRetryable(
                SyncMongoClient::new,
                mongoCollection -> mongoCollection.find(eq(0)).iterator().hasNext(), "find", false);
    }

    /**
     * <a href="https://github.com/mongodb/specifications/blob/master/source/retryable-reads/tests/README.md#21-retryable-reads-are-retried-on-a-different-mongos-when-one-is-available">
     * 2.1 Retryable Reads Are Retried on a Different mongos When One is Available</a>.
     */
    @Test
    void retriesOnDifferentMongosWhenAvailable() {
        RetryableWritesProseTest.retriesOnDifferentMongosWhenAvailable(
                SyncMongoClient::new,
                mongoCollection -> {
                    try (MongoCursor<Document> cursor = mongoCollection.find().iterator()) {
                        return cursor.hasNext();
                    }
                }, "find", false);
    }

    /**
     * <a href="https://github.com/mongodb/specifications/blob/master/source/retryable-reads/tests/README.md#22-retryable-reads-are-retried-on-the-same-mongos-when-no-others-are-available">
     * 2.2 Retryable Reads Are Retried on the Same mongos When No Others are Available</a>.
     */
    @Test
    void retriesOnSameMongosWhenAnotherNotAvailable() {
        RetryableWritesProseTest.retriesOnSameMongosWhenAnotherNotAvailable(
                SyncMongoClient::new,
                mongoCollection -> {
                    try (MongoCursor<Document> cursor = mongoCollection.find().iterator()) {
                        return cursor.hasNext();
                    }
                }, "find", false);
    }
}
