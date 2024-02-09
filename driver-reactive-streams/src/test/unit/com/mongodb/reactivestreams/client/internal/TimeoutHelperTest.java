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
import com.mongodb.internal.time.Timeout;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static com.mongodb.internal.mockito.MongoMockito.mock;
import static com.mongodb.reactivestreams.client.internal.TimeoutHelper.collectionWithTimeout;
import static com.mongodb.reactivestreams.client.internal.TimeoutHelper.collectionWithTimeoutDeferred;
import static com.mongodb.reactivestreams.client.internal.TimeoutHelper.collectionWithTimeoutMono;
import static com.mongodb.reactivestreams.client.internal.TimeoutHelper.databaseWithTimeout;
import static com.mongodb.reactivestreams.client.internal.TimeoutHelper.databaseWithTimeoutDeferred;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.longThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class TimeoutHelperTest {

    private static final String TIMEOUT_ERROR_MESSAGE = "message";
    private static final String DEFAULT_TIMEOUT_ERROR_MESSAGE = "Operation exceeded the timeout limit.";

    @Test
    void shouldNotSetRemainingTimeoutOnCollectionWhenTimeoutIsNull() {
        //given
        MongoCollection<Document> collection = mock(MongoCollection.class);

        //when
        MongoCollection<Document> result = collectionWithTimeout(collection, null);
        MongoCollection<Document> monoResult = collectionWithTimeoutMono(collection, null).block();
        MongoCollection<Document> monoResultDeferred = collectionWithTimeoutDeferred(collection, null).block();

        //then
        assertEquals(collection, result);
        assertEquals(collection, monoResult);
        assertEquals(collection, monoResultDeferred);
    }

    @Test
    void shouldNotSetRemainingTimeoutDatabaseWhenTimeoutIsNull() {
        //given
        MongoDatabase database = mock(MongoDatabase.class);

        //when
        MongoDatabase result = databaseWithTimeout(database, TIMEOUT_ERROR_MESSAGE, null);
        MongoDatabase monoResultDeferred = databaseWithTimeoutDeferred(database, TIMEOUT_ERROR_MESSAGE, null).block();

        //then
        assertEquals(database, result);
        assertEquals(database, monoResultDeferred);
    }

    @Test
    void shouldNotSetRemainingTimeoutOnCollectionWhenTimeoutIsInfinite() {
        //given
        MongoCollection<Document> collection = mock(MongoCollection.class);

        //when
        MongoCollection<Document> result = collectionWithTimeout(collection, Timeout.infinite());
        MongoCollection<Document> monoResult = collectionWithTimeoutMono(collection, Timeout.infinite()).block();
        MongoCollection<Document> monoResultDeferred = collectionWithTimeoutDeferred(collection, Timeout.infinite()).block();

        //then
        assertEquals(collection, result);
        assertEquals(collection, monoResult);
        assertEquals(collection, monoResultDeferred);
    }

    @Test
    void shouldNotSetRemainingTimeoutOnDatabaseWhenTimeoutIsInfinite() {
        //given
        MongoDatabase database = mock(MongoDatabase.class);

        //when
        MongoDatabase result = databaseWithTimeout(database, TIMEOUT_ERROR_MESSAGE, Timeout.infinite());
        MongoDatabase monoResultDeferred = databaseWithTimeoutDeferred(database, TIMEOUT_ERROR_MESSAGE, Timeout.infinite()).block();

        //then
        assertEquals(database, result);
        assertEquals(database, monoResultDeferred);
    }

    @Test
    void shouldSetRemainingTimeoutOnCollectionWhenTimeout() {
        //given
        MongoCollection<Document> collectionWithTimeout = mock(MongoCollection.class);
        MongoCollection<Document> collection = mock(MongoCollection.class, mongoCollection -> {
            when(mongoCollection.withTimeout(anyLong(), eq(TimeUnit.MILLISECONDS))).thenReturn(mongoCollection);
        });
        Timeout timeout = Timeout.expiresIn(1, TimeUnit.DAYS);

        //when
        MongoCollection<Document> result = collectionWithTimeout(collection, timeout);
        MongoCollection<Document> monoResult = collectionWithTimeoutMono(collection, timeout).block();
        MongoCollection<Document> monoResultDeferred = collectionWithTimeoutDeferred(collection, timeout).block();

        //then
        verify(collection, times(3))
                .withTimeout(longThat(remaining -> remaining > 0), eq(TimeUnit.MILLISECONDS));
        assertNotEquals(collectionWithTimeout, result);
        assertNotEquals(collectionWithTimeout, monoResult);
        assertNotEquals(collectionWithTimeout, monoResultDeferred);
    }

    @Test
    void shouldSetRemainingTimeoutOnDatabaseWhenTimeout() {
        //given
        MongoDatabase databaseWithTimeout = mock(MongoDatabase.class);
        MongoDatabase database = mock(MongoDatabase.class, mongoDatabase -> {
            when(mongoDatabase.withTimeout(anyLong(), eq(TimeUnit.MILLISECONDS))).thenReturn(mongoDatabase);
        });
        Timeout timeout = Timeout.expiresIn(1, TimeUnit.DAYS);

        //when
        MongoDatabase result = databaseWithTimeout(database, TIMEOUT_ERROR_MESSAGE, timeout);
        MongoDatabase monoResultDeferred = databaseWithTimeoutDeferred(database, TIMEOUT_ERROR_MESSAGE, timeout).block();

        //then
        verify(database, times(2))
                .withTimeout(longThat(remaining -> remaining > 0), eq(TimeUnit.MILLISECONDS));
        assertNotEquals(databaseWithTimeout, result);
        assertNotEquals(databaseWithTimeout, monoResultDeferred);
    }

    @Test
    void shouldThrowErrorWhenTimeoutHasExpiredOnCollection() {
        //given
        MongoCollection<Document> collection = mock(MongoCollection.class);
        Timeout timeout = Timeout.expiresIn(1, TimeUnit.MICROSECONDS);

        //when
        MongoOperationTimeoutException mongoExecutionTimeoutException =
                assertThrows(MongoOperationTimeoutException.class, () -> collectionWithTimeout(collection, timeout));
        MongoOperationTimeoutException mongoExecutionTimeoutExceptionMono =
                assertThrows(MongoOperationTimeoutException.class, () -> collectionWithTimeoutMono(collection, timeout).block());
        MongoOperationTimeoutException mongoExecutionTimeoutExceptionDeferred =
                assertThrows(MongoOperationTimeoutException.class, () -> collectionWithTimeoutDeferred(collection, timeout).block());

        //then
        assertEquals(DEFAULT_TIMEOUT_ERROR_MESSAGE, mongoExecutionTimeoutExceptionMono.getMessage());
        assertEquals(DEFAULT_TIMEOUT_ERROR_MESSAGE, mongoExecutionTimeoutException.getMessage());
        assertEquals(DEFAULT_TIMEOUT_ERROR_MESSAGE, mongoExecutionTimeoutExceptionDeferred.getMessage());
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
        MongoOperationTimeoutException mongoExecutionTimeoutExceptionDeferred =
                assertThrows(MongoOperationTimeoutException.class,
                        () -> databaseWithTimeoutDeferred(database, TIMEOUT_ERROR_MESSAGE, timeout)
                                .block());

        //then
        assertEquals(TIMEOUT_ERROR_MESSAGE, mongoExecutionTimeoutException.getMessage());
        assertEquals(TIMEOUT_ERROR_MESSAGE, mongoExecutionTimeoutExceptionDeferred.getMessage());
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
        assertThrows(MongoOperationTimeoutException.class, () -> collectionWithTimeout(collection, timeout));
        assertThrows(MongoOperationTimeoutException.class, () -> collectionWithTimeoutMono(collection, timeout).block());
        assertThrows(MongoOperationTimeoutException.class, () -> collectionWithTimeoutDeferred(collection, timeout).block());

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
        assertThrows(MongoOperationTimeoutException.class,
                () -> databaseWithTimeoutDeferred(database, TIMEOUT_ERROR_MESSAGE, timeout).block());

        //then
        verifyNoInteractions(database);
    }
}
