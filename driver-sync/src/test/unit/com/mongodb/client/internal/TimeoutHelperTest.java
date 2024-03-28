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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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
    void shouldNotSetRemainingTimeoutOnCollectionWhenTimeoutIsInfinite() {
        //given
        MongoCollection<Document> collection = mock(MongoCollection.class);

        //when
        MongoCollection<Document> result = collectionWithTimeout(collection, TIMEOUT_ERROR_MESSAGE, Timeout.infinite());

        //then
        assertEquals(collection, result);
    }

    @Test
    void shouldNotSetRemainingTimeoutOnDatabaseWhenTimeoutIsInfinite() {
        //given
        MongoDatabase database = mock(MongoDatabase.class);

        //when
        MongoDatabase result = databaseWithTimeout(database, TIMEOUT_ERROR_MESSAGE, Timeout.infinite());

        //then
        assertEquals(database, result);
    }

    @Test
    void shouldSetRemainingTimeoutOnCollectionWhenTimeout() {
        //given
        MongoCollection<Document> collectionWithTimeout = mock(MongoCollection.class);
        MongoCollection<Document> collection = mock(MongoCollection.class, mongoCollection -> {
            when(mongoCollection.withTimeout(anyLong(), eq(TimeUnit.MILLISECONDS))).thenReturn(mongoCollection);
        });
        Timeout timeout = Timeout.expiresIn(1, TimeUnit.DAYS, Timeout.ZeroDurationIs.EXPIRED);

        //when
        MongoCollection<Document> result = collectionWithTimeout(collection, TIMEOUT_ERROR_MESSAGE, timeout);

        //then
        verify(collection).withTimeout(longThat(remaining -> remaining > 0), eq(TimeUnit.MILLISECONDS));
        assertNotEquals(collectionWithTimeout, result);
    }

    @Test
    void shouldSetRemainingTimeoutOnDatabaseWhenTimeout() {
        //given
        MongoDatabase databaseWithTimeout = mock(MongoDatabase.class);
        MongoDatabase database = mock(MongoDatabase.class, mongoDatabase -> {
            when(mongoDatabase.withTimeout(anyLong(), eq(TimeUnit.MILLISECONDS))).thenReturn(mongoDatabase);
        });
        Timeout timeout = Timeout.expiresIn(1, TimeUnit.DAYS, Timeout.ZeroDurationIs.EXPIRED);

        //when
        MongoDatabase result = databaseWithTimeout(database, TIMEOUT_ERROR_MESSAGE, timeout);

        //then
        verify(database).withTimeout(longThat(remaining -> remaining > 0), eq(TimeUnit.MILLISECONDS));
        assertNotEquals(databaseWithTimeout, result);
    }

    @Test
    void shouldThrowErrorWhenTimeoutHasExpiredOnCollection() {
        //given
        MongoCollection<Document> collection = mock(MongoCollection.class);
        Timeout timeout = Timeout.expiresIn(1, TimeUnit.MICROSECONDS, Timeout.ZeroDurationIs.EXPIRED);

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
        Timeout timeout = Timeout.expiresIn(1, TimeUnit.MICROSECONDS, Timeout.ZeroDurationIs.EXPIRED);

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
        Timeout timeout = Timeout.expiresIn(0, TimeUnit.NANOSECONDS, Timeout.ZeroDurationIs.EXPIRED);

        //when
        assertThrows(MongoOperationTimeoutException.class, () -> collectionWithTimeout(collection, TIMEOUT_ERROR_MESSAGE, timeout));

        //then

    }

    @Test
    void shouldThrowErrorWhenTimeoutHasExpiredWithZeroRemainingOnDatabase() {
        //given
        MongoDatabase database = mock(MongoDatabase.class);
        Timeout timeout = Timeout.expiresIn(0, TimeUnit.NANOSECONDS, Timeout.ZeroDurationIs.EXPIRED);

        //when
        assertThrows(MongoOperationTimeoutException.class, () -> databaseWithTimeout(database, TIMEOUT_ERROR_MESSAGE, timeout));

        //then
        verifyNoInteractions(database);
    }

}
