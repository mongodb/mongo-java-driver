package com.mongodb.client.gridfs;

import com.mongodb.MongoExecutionTimeoutException;
import com.mongodb.client.MongoCollection;
import com.mongodb.internal.time.Timeout;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static com.mongodb.client.gridfs.CollectionTimeoutHelper.collectionWithTimeout;
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

class TimeoutUtilsTest {

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
        MongoCollection<Document> collection = mock(MongoCollection.class);
        MongoCollection<Document> collectionWithTimeout = mock(MongoCollection.class);
        when(collection.withTimeout(anyLong(), eq(TimeUnit.MILLISECONDS))).thenReturn(collectionWithTimeout);
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
        assertThrows(MongoExecutionTimeoutException.class, () -> collectionWithTimeout(collection, TIMEOUT_ERROR_MESSAGE, timeout));

        //then
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
        assertThrows(MongoExecutionTimeoutException.class, () -> collectionWithTimeout(collection, TIMEOUT_ERROR_MESSAGE, timeout));

        //then
        verifyNoInteractions(collection);
    }
}