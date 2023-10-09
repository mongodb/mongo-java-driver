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

import com.mongodb.ServerAddress;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.NoSuchElementException;

import static com.mongodb.assertions.Assertions.assertFalse;
import static com.mongodb.assertions.Assertions.assertNull;
import static com.mongodb.internal.connection.tlschannel.util.Util.assertTrue;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


class SingleBatchCursorTest {

    private static final List<Document> SINGLE_BATCH = asList(new Document("a", 1), new Document("b", 2));
    private static final ServerAddress SERVER_ADDRESS = new ServerAddress();

    @Test
    @DisplayName("should work as expected")
    void shouldWorkAsExpected() {

        try (SingleBatchCursor<Document> cursor = new SingleBatchCursor<>(SINGLE_BATCH, 0, SERVER_ADDRESS)) {
            assertEquals(SERVER_ADDRESS, cursor.getServerAddress());
            assertEquals(1, cursor.available());
            assertNull(cursor.getServerCursor());

            assertTrue(cursor.hasNext());
            assertIterableEquals(SINGLE_BATCH, cursor.next());
            assertEquals(0, cursor.available());

            assertFalse(cursor.hasNext());
            assertThrows(NoSuchElementException.class, cursor::next);
        }
    }

    @Test
    @DisplayName("should work as expected emptyCursor")
    void shouldWorkAsExpectedEmptyCursor() {
        try (SingleBatchCursor<Document> cursor = SingleBatchCursor.createEmptyBatchCursor(SERVER_ADDRESS, 0)) {
            assertEquals(SERVER_ADDRESS, cursor.getServerAddress());
            assertEquals(0, cursor.available());
            assertNull(cursor.getServerCursor());

            assertFalse(cursor.hasNext());
            assertThrows(NoSuchElementException.class, cursor::next);
        }
    }

    @Test
    @DisplayName("should work as expected with try methods")
    void shouldWorkAsExpectedWithTryMethods() {
        try (SingleBatchCursor<Document> cursor = new SingleBatchCursor<>(SINGLE_BATCH, 0, SERVER_ADDRESS)) {
            assertIterableEquals(SINGLE_BATCH, cursor.tryNext());
            assertNull(cursor.tryNext());
        }
    }

    @Test
    @DisplayName("should not support setting batch size")
    void shouldNotSupportSettingBatchSize() {
        try (SingleBatchCursor<Document> cursor = new SingleBatchCursor<>(SINGLE_BATCH, 0, SERVER_ADDRESS)) {
            assertEquals(0, cursor.getBatchSize());

            cursor.setBatchSize(1);
            assertEquals(0, cursor.getBatchSize());
        }
    }

}
