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

package com.mongodb.client.gridfs;

import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.client.MongoCollection;
import com.mongodb.internal.time.Timeout;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static com.mongodb.client.internal.TimeoutHelper.collectionWithTimeout;
import static com.mongodb.internal.mockito.MongoMockito.mock;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.longThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
class CollectionTimeoutHelperTest {

    private static final String TIMEOUT_ERROR_MESSAGE = "message";

    @Test
    void shouldNotSetRemainingTimeoutWhenTimeoutIsNull() {
        //given
        MongoCollection<Document> collection = mock(MongoCollection.class);

        //when
        MongoCollection<Document> result = collectionWithTimeout(collection, TIMEOUT_ERROR_MESSAGE, null);

        //then
        assertEquals(collection, result);
    }

    @Test
    void shouldNotSetRemainingTimeoutWhenTimeoutIsInfinite() {
        //given
        MongoCollection<Document> collection = mock(MongoCollection.class);

        //when
        MongoCollection<Document> result = collectionWithTimeout(collection, TIMEOUT_ERROR_MESSAGE, Timeout.infinite());

        //then
        assertEquals(collection, result);
    }

    @Test
    void shouldSetRemainingTimeoutWhenTimeout() {
        //given
        MongoCollection<Document> collectionWithTimeout = mock(MongoCollection.class);
        MongoCollection<Document> collection = mock(MongoCollection.class, mongoCollection -> {
            when(mongoCollection.withTimeout(anyLong(), eq(TimeUnit.MILLISECONDS))).thenReturn(mongoCollection);
        });
        Timeout timeout = Timeout.expiresIn(1, TimeUnit.DAYS);

        //when
        MongoCollection<Document> result = collectionWithTimeout(collection, TIMEOUT_ERROR_MESSAGE, timeout);

        //then
        verify(collection).withTimeout(longThat(remaining -> remaining > 0), eq(TimeUnit.MILLISECONDS));
        assertNotEquals(collectionWithTimeout, result);
    }

    @Test
    void shouldThrowErrorWhenTimeoutHasExpired() {
        //given
        MongoCollection<Document> collection = mock(MongoCollection.class);
        Timeout timeout = Timeout.expiresIn(1, TimeUnit.MICROSECONDS);

        //when
        MongoOperationTimeoutException mongoExecutionTimeoutException =
                assertThrows(MongoOperationTimeoutException.class, () -> collectionWithTimeout(collection, TIMEOUT_ERROR_MESSAGE, timeout));

        //then
        assertEquals(TIMEOUT_ERROR_MESSAGE, mongoExecutionTimeoutException.getMessage());
        verifyNoInteractions(collection);
    }

    @Test
    void shouldThrowErrorWhenTimeoutHasExpiredWithZeroRemaining() {
        //given
        MongoCollection<Document> collection = mock(MongoCollection.class);
        Timeout timeout = mock(Timeout.class, timeout1 -> {
            when(timeout1.hasExpired()).thenReturn(true);
            when(timeout1.isInfinite()).thenReturn(false);
            when(timeout1.remaining(TimeUnit.MILLISECONDS)).thenReturn(0L);
        });

        //when
        assertThrows(MongoOperationTimeoutException.class, () -> collectionWithTimeout(collection, TIMEOUT_ERROR_MESSAGE, timeout));

        //then
        verifyNoInteractions(collection);
    }
}
