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

import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.internal.time.Timeout;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static com.mongodb.client.internal.TimeoutHelper.collectionWithTimeout;
import static com.mongodb.client.internal.TimeoutHelper.databaseWithTimeout;
import static com.mongodb.internal.mockito.MongoMockito.mock;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.longThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
class TimeoutHelperTest {

    private static final String TIMEOUT_ERROR_MESSAGE = "message";

    @Test
    void shouldNotSetRemainingTimeoutOnCollectionWhenTimeoutIsNull() {
        //given
        MongoCollection<Document> collection = mock(MongoCollection.class);

        //when
        MongoCollection<Document> result = collectionWithTimeout(collection, TIMEOUT_ERROR_MESSAGE, null);

        //then
        assertEquals(collection, result);
    }

    @Test
    void shouldNotSetRemainingTimeoutDatabaseWhenTimeoutIsNull() {
        //given
        MongoDatabase database = mock(MongoDatabase.class);

        //when
        MongoDatabase result = databaseWithTimeout(database, TIMEOUT_ERROR_MESSAGE, null);

        //then
        assertEquals(database, result);
    }

    @Test
    void shouldSetRemainingTimeoutOnCollectionWhenTimeoutIsInfinite() {
        //given
        MongoCollection<Document> collectionWithTimeout = mock(MongoCollection.class);
        MongoCollection<Document> collection = mock(MongoCollection.class, mongoCollection -> {
            when(mongoCollection.withTimeout(anyLong(), eq(TimeUnit.MILLISECONDS))).thenReturn(collectionWithTimeout);
        });

        //when
        MongoCollection<Document> result = collectionWithTimeout(collection, TIMEOUT_ERROR_MESSAGE, Timeout.infinite());

        //then
        assertEquals(collectionWithTimeout, result);
        verify(collection).withTimeout(0L, TimeUnit.MILLISECONDS);
    }

    @Test
    void shouldNotSetRemainingTimeoutOnDatabaseWhenTimeoutIsInfinite() {
        //given
        MongoDatabase databaseWithTimeout = mock(MongoDatabase.class);
        MongoDatabase database = mock(MongoDatabase.class, mongoDatabase -> {
            when(mongoDatabase.withTimeout(anyLong(), eq(TimeUnit.MILLISECONDS))).thenReturn(databaseWithTimeout);
        });

        //when
        MongoDatabase result = databaseWithTimeout(database, TIMEOUT_ERROR_MESSAGE, Timeout.infinite());

        //then
        assertEquals(databaseWithTimeout, result);
        verify(database).withTimeout(0L, TimeUnit.MILLISECONDS);
    }

    @Test
    void shouldSetRemainingTimeoutOnCollectionWhenTimeout() {
        //given
        MongoCollection<Document> collectionWithTimeout = mock(MongoCollection.class);
        MongoCollection<Document> collection = mock(MongoCollection.class, mongoCollection -> {
            when(mongoCollection.withTimeout(anyLong(), eq(TimeUnit.MILLISECONDS))).thenReturn(collectionWithTimeout);
        });
        Timeout timeout = Timeout.expiresIn(1, TimeUnit.DAYS);

        //when
        MongoCollection<Document> result = collectionWithTimeout(collection, TIMEOUT_ERROR_MESSAGE, timeout);

        //then
        verify(collection).withTimeout(longThat(remaining -> remaining > 0), eq(TimeUnit.MILLISECONDS));
        assertEquals(collectionWithTimeout, result);
    }

    @Test
    void shouldSetRemainingTimeoutOnDatabaseWhenTimeout() {
        //given
        MongoDatabase databaseWithTimeout = mock(MongoDatabase.class);
        MongoDatabase database = mock(MongoDatabase.class, mongoDatabase -> {
            when(mongoDatabase.withTimeout(anyLong(), eq(TimeUnit.MILLISECONDS))).thenReturn(databaseWithTimeout);
        });
        Timeout timeout = Timeout.expiresIn(1, TimeUnit.DAYS);

        //when
        MongoDatabase result = databaseWithTimeout(database, TIMEOUT_ERROR_MESSAGE, timeout);

        //then
        verify(database).withTimeout(longThat(remaining -> remaining > 0), eq(TimeUnit.MILLISECONDS));
        assertEquals(databaseWithTimeout, result);
    }

    @Test
    void shouldThrowErrorWhenTimeoutHasExpiredOnCollection() {
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
    void shouldThrowErrorWhenTimeoutHasExpiredOnDatabase() {
        //given
        MongoDatabase database = mock(MongoDatabase.class);
        Timeout timeout = Timeout.expiresIn(1, TimeUnit.MICROSECONDS);

        //when
        MongoOperationTimeoutException mongoExecutionTimeoutException =
                assertThrows(MongoOperationTimeoutException.class, () -> databaseWithTimeout(database, TIMEOUT_ERROR_MESSAGE, timeout));

        //then
        assertEquals(TIMEOUT_ERROR_MESSAGE, mongoExecutionTimeoutException.getMessage());
        verifyNoInteractions(database);
    }

    @Test
    void shouldThrowErrorWhenTimeoutHasExpiredWithZeroRemainingOnCollection() {
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

    }

    @Test
    void shouldThrowErrorWhenTimeoutHasExpiredWithZeroRemainingOnDatabase() {
        //given
        MongoDatabase database = mock(MongoDatabase.class);
        Timeout timeout = mock(Timeout.class, timeout1 -> {
            when(timeout1.hasExpired()).thenReturn(true);
            when(timeout1.isInfinite()).thenReturn(false);
            when(timeout1.remaining(TimeUnit.MILLISECONDS)).thenReturn(0L);
        });

        //when
        assertThrows(MongoOperationTimeoutException.class, () -> databaseWithTimeout(database, TIMEOUT_ERROR_MESSAGE, timeout));

        //then
        verifyNoInteractions(database);
    }

}
