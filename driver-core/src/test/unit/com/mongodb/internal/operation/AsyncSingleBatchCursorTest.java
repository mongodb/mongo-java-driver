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

package com.mongodb.internal.operation;

import com.mongodb.MongoException;
import com.mongodb.async.FutureResultCallback;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.TIMEOUT;
import static com.mongodb.internal.operation.AsyncSingleBatchCursor.createEmptyBatchCursor;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


class AsyncSingleBatchCursorTest {

    private static final List<Document> SINGLE_BATCH = asList(new Document("a", 1), new Document("b", 2));

    @Test
    @DisplayName("should work as expected")
    void shouldWorkAsExpected() {
        try (AsyncSingleBatchCursor<Document> cursor = new AsyncSingleBatchCursor<>(SINGLE_BATCH, 0)) {

            assertIterableEquals(SINGLE_BATCH, nextBatch(cursor));
            assertIterableEquals(emptyList(), nextBatch(cursor));
            assertTrue(cursor.isClosed());

            assertThrows(MongoException.class, () -> nextBatch(cursor));
        }
    }

    @Test
    @DisplayName("should work as expected emptyCursor")
    void shouldWorkAsExpectedEmptyCursor() {
        try (AsyncSingleBatchCursor<Document> cursor = createEmptyBatchCursor(0)) {
            assertIterableEquals(emptyList(), nextBatch(cursor));
            assertTrue(cursor.isClosed());

            assertThrows(MongoException.class, () -> nextBatch(cursor));
        }
    }

    @Test
    @DisplayName("should not support setting batch size")
    void shouldNotSupportSettingBatchSize() {
        try (AsyncSingleBatchCursor<Document> cursor = new AsyncSingleBatchCursor<>(SINGLE_BATCH, 0)) {

            assertEquals(0, cursor.getBatchSize());

            cursor.setBatchSize(1);
            assertEquals(0, cursor.getBatchSize());
        }
    }

    List<Document> nextBatch(final AsyncSingleBatchCursor<Document> cursor) {
        FutureResultCallback<List<Document>> futureResultCallback = new FutureResultCallback<>();
        cursor.next(futureResultCallback);
        return futureResultCallback.get(TIMEOUT, TimeUnit.MILLISECONDS);
    }

}
