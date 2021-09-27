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

import com.mongodb.client.RetryableWritesProseTest;
import com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static com.mongodb.client.model.Filters.eq;

/**
 * See
 * <a href="https://github.com/mongodb/specifications/tree/master/source/retryable-reads/tests">Retryable Reads Tests</a>.
 */
public class RetryableReadsProseTest {
    /**
     * See
     * <a href="https://github.com/mongodb/specifications/tree/master/source/retryable-reads/tests#poolclearederror-retryability-test">
     * PoolClearedError Retryability Test</a>.
     */
    @Test
    public void poolClearedExceptionMustBeRetryable() throws InterruptedException, ExecutionException, TimeoutException {
        RetryableWritesProseTest.poolClearedExceptionMustBeRetryable(
                mongoClientSettings -> new SyncMongoClient(MongoClients.create(mongoClientSettings)),
                mongoCollection -> mongoCollection.find(eq(0)).iterator().hasNext(), "find", false);
    }
}
