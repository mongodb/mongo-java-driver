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

import com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient;
import org.bson.Document;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeoutException;

/**
 * <a href="https://github.com/mongodb/specifications/blob/master/source/retryable-writes/tests/README.md#prose-tests">
 * Prose Tests</a>.
 */
final class RetryableWritesProseTest {
    /**
     * <a href="https://github.com/mongodb/specifications/blob/master/source/retryable-writes/tests/README.md#2-test-that-drivers-properly-retry-after-encountering-poolclearederrors">
     * 2. Test that drivers properly retry after encountering PoolClearedErrors</a>.
     */
    @Test
    void poolClearedExceptionMustBeRetryable() throws Exception {
        com.mongodb.client.RetryableWritesProseTest.poolClearedExceptionMustBeRetryable(
                SyncMongoClient::new,
                mongoCollection -> mongoCollection.insertOne(new Document()), "insert", true);
    }

    /**
     * <a href="https://github.com/mongodb/specifications/blob/master/source/retryable-writes/tests/README.md#3-test-that-drivers-return-the-original-error-after-encountering-a-writeconcernerror-with-a-retryablewriteerror-label">
     * 3. Test that drivers return the original error after encountering a WriteConcernError with a RetryableWriteError label</a>.
     */
    @Test
    void originalErrorMustBePropagatedIfNoWritesPerformed() throws Exception {
        com.mongodb.client.RetryableWritesProseTest.originalErrorMustBePropagatedIfNoWritesPerformed(
                SyncMongoClient::new);
    }

    /**
     * <a href="https://github.com/mongodb/specifications/blob/master/source/retryable-writes/tests/README.md#4-test-that-in-a-sharded-cluster-writes-are-retried-on-a-different-mongos-when-one-is-available">
     * 4. Test that in a sharded cluster writes are retried on a different mongos when one is available</a>.
     */
    @Test
    void retriesOnDifferentMongosWhenAvailable() throws InterruptedException, TimeoutException {
        com.mongodb.client.RetryableWritesProseTest.retriesOnDifferentMongosWhenAvailable(
                SyncMongoClient::new,
                mongoCollection -> mongoCollection.insertOne(new Document()), "insert", true);
    }

    /**
     * <a href="https://github.com/mongodb/specifications/blob/master/source/retryable-writes/tests/README.md#5-test-that-in-a-sharded-cluster-writes-are-retried-on-the-same-mongos-when-no-others-are-available">
     * 5. Test that in a sharded cluster writes are retried on the same mongos when no others are available</a>.
     */
    @Test
    void retriesOnSameMongosWhenAnotherNotAvailable() {
        com.mongodb.client.RetryableWritesProseTest.retriesOnSameMongosWhenAnotherNotAvailable(
                SyncMongoClient::new,
                mongoCollection -> mongoCollection.insertOne(new Document()), "insert", true);
    }

    /**
     * <a href="https://github.com/mongodb/specifications/blob/master/source/retryable-writes/tests/README.md#case-1-test-that-drivers-return-the-correct-error-when-receiving-only-errors-without-nowritesperformed">
     * 6. Test error propagation after encountering multiple errors.
     * Case 1: Test that drivers return the correct error when receiving only errors without NoWritesPerformed</a>.
     */
    @Test
    @Disabled("TODO-BACKPRESSURE Valentin Enable when implementing JAVA-6055")
    void errorPropagationAfterEncounteringMultipleErrorsCase1() throws Exception {
        com.mongodb.client.RetryableWritesProseTest.errorPropagationAfterEncounteringMultipleErrorsCase1(SyncMongoClient::new);
    }

    /**
     * <a href="https://github.com/mongodb/specifications/blob/master/source/retryable-writes/tests/README.md#case-2-test-that-drivers-return-the-correct-error-when-receiving-only-errors-with-nowritesperformed">
     * 6. Test error propagation after encountering multiple errors.
     * Case 2: Test that drivers return the correct error when receiving only errors with NoWritesPerformed</a>.
     */
    @Test
    void errorPropagationAfterEncounteringMultipleErrorsCase2() throws Exception {
        com.mongodb.client.RetryableWritesProseTest.errorPropagationAfterEncounteringMultipleErrorsCase2(SyncMongoClient::new);
    }

    /**
     * <a href="https://github.com/mongodb/specifications/blob/master/source/retryable-writes/tests/README.md#case-3-test-that-drivers-return-the-correct-error-when-receiving-some-errors-with-nowritesperformed-and-some-without-nowritesperformed">
     * 6. Test error propagation after encountering multiple errors.
     * Case 3: Test that drivers return the correct error when receiving some errors with NoWritesPerformed and some without NoWritesPerformed</a>.
     */
    @Test
    void errorPropagationAfterEncounteringMultipleErrorsCase3() throws Exception {
        com.mongodb.client.RetryableWritesProseTest.errorPropagationAfterEncounteringMultipleErrorsCase3(SyncMongoClient::new);
    }
}
