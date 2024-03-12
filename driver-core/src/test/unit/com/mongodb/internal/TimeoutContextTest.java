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
package com.mongodb.internal;

import com.mongodb.MongoOperationTimeoutException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.function.Supplier;

import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS;
import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS_WITH_INFINITE_TIMEOUT;
import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS_WITH_MAX_AWAIT_TIME;
import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS_WITH_MAX_COMMIT;
import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS_WITH_MAX_TIME;
import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS_WITH_MAX_TIME_AND_AWAIT_TIME;
import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS_WITH_TIMEOUT;
import static com.mongodb.ClusterFixture.sleep;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class TimeoutContextTest {

    @Test
    @DisplayName("test defaults")
    void testDefaults() {
        TimeoutContext timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS);

        assertFalse(timeoutContext.hasTimeoutMS());
        assertEquals(0, timeoutContext.getMaxTimeMS());
        assertEquals(0, timeoutContext.getMaxAwaitTimeMS());
        assertEquals(0, timeoutContext.getMaxCommitTimeMS());
        assertEquals(0, timeoutContext.getReadTimeoutMS());
    }

    @Test
    @DisplayName("Uses timeoutMS if set")
    void testUsesTimeoutMSIfSet() {
        TimeoutContext timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS_WITH_TIMEOUT.withMaxAwaitTimeMS(9));

        assertTrue(timeoutContext.hasTimeoutMS());
        assertTrue(timeoutContext.getMaxTimeMS() > 0);
        assertEquals(0, timeoutContext.getMaxAwaitTimeMS());
    }

    @Test
    @DisplayName("infinite timeoutMS")
    void testInfiniteTimeoutMS() {
        TimeoutContext timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS_WITH_INFINITE_TIMEOUT);

        assertTrue(timeoutContext.hasTimeoutMS());
        assertEquals(0, timeoutContext.getMaxTimeMS());
        assertEquals(0, timeoutContext.getMaxAwaitTimeMS());
    }

    @Test
    @DisplayName("MaxTimeMS set")
    void testMaxTimeMSSet() {
        TimeoutContext timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS_WITH_MAX_TIME);

        assertFalse(timeoutContext.hasTimeoutMS());
        assertEquals(100, timeoutContext.getMaxTimeMS());
        assertEquals(0, timeoutContext.getMaxAwaitTimeMS());
    }

    @Test
    @DisplayName("MaxAwaitTimeMS set")
    void testMaxAwaitTimeMSSet() {
        TimeoutContext timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS_WITH_MAX_AWAIT_TIME);

        assertFalse(timeoutContext.hasTimeoutMS());
        assertEquals(0, timeoutContext.getMaxTimeMS());
        assertEquals(101, timeoutContext.getMaxAwaitTimeMS());
    }

    @Test
    @DisplayName("MaxTimeMS and MaxAwaitTimeMS set")
    void testMaxTimeMSAndMaxAwaitTimeMSSet() {
        TimeoutContext timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS_WITH_MAX_TIME_AND_AWAIT_TIME);

        assertFalse(timeoutContext.hasTimeoutMS());
        assertEquals(101, timeoutContext.getMaxTimeMS());
        assertEquals(1001, timeoutContext.getMaxAwaitTimeMS());
    }

    @Test
    @DisplayName("MaxCommitTimeMS set")
    void testMaxCommitTimeMSSet() {
        TimeoutContext timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS_WITH_MAX_COMMIT);

        assertFalse(timeoutContext.hasTimeoutMS());
        assertEquals(0, timeoutContext.getMaxTimeMS());
        assertEquals(0, timeoutContext.getMaxAwaitTimeMS());
        assertEquals(999L, timeoutContext.getMaxCommitTimeMS());
    }

    @Test
    @DisplayName("All deprecated options set")
    void testAllDeprecatedOptionsSet() {
        TimeoutContext timeoutContext =
                new TimeoutContext(TIMEOUT_SETTINGS_WITH_MAX_TIME_AND_AWAIT_TIME
                        .withMaxCommitMS(999L));

        assertFalse(timeoutContext.hasTimeoutMS());
        assertEquals(101, timeoutContext.getMaxTimeMS());
        assertEquals(1001, timeoutContext.getMaxAwaitTimeMS());
        assertEquals(999, timeoutContext.getMaxCommitTimeMS());
    }

    @Test
    @DisplayName("Use timeout if available or the alternative")
    void testUseTimeoutIfAvailableOrTheAlternative() {
        TimeoutContext timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS);
        assertEquals(99L, timeoutContext.timeoutOrAlternative(99));

        timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS.withTimeoutMS(0L));
        assertEquals(0L, timeoutContext.timeoutOrAlternative(99));

        timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS.withTimeoutMS(999L));
        assertTrue(timeoutContext.timeoutOrAlternative(0) <= 999);

        timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS.withTimeoutMS(999L));
        assertTrue(timeoutContext.timeoutOrAlternative(999999) <= 999);

        timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS);
        assertEquals(timeoutContext.getMaxCommitTimeMS(), 0);

        timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS.withTimeoutMS(999L));
        assertTrue(timeoutContext.getMaxCommitTimeMS() <= 999);
    }

    @Test
    @DisplayName("Calculate min works as expected")
    void testCalculateMin() {
        TimeoutContext timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS);
        assertEquals(99L, timeoutContext.calculateMin(99));

        timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS.withTimeoutMS(0L));
        assertEquals(99L, timeoutContext.calculateMin(99));

        timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS.withTimeoutMS(999L));
        assertTrue(timeoutContext.calculateMin(0) <= 999);

        timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS.withTimeoutMS(999L));
        assertTrue(timeoutContext.calculateMin(999999) <= 999);
    }

    @Test
    @DisplayName("withAdditionalReadTimeout works as expected")
    void testWithAdditionalReadTimeout() {
        TimeoutContext timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS.withReadTimeoutMS(0));
        assertEquals(0L, timeoutContext.withAdditionalReadTimeout(101).getReadTimeoutMS());

        timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS.withReadTimeoutMS(10_000L));
        assertEquals(10_101L, timeoutContext.withAdditionalReadTimeout(101).getReadTimeoutMS());

        long originalValue = Long.MAX_VALUE - 100;
        timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS.withReadTimeoutMS(originalValue));
        assertEquals(Long.MAX_VALUE, timeoutContext.withAdditionalReadTimeout(101).getReadTimeoutMS());

        assertThrows(AssertionError.class, () -> new TimeoutContext(TIMEOUT_SETTINGS.withTimeoutMS(0L)).withAdditionalReadTimeout(1));

        assertThrows(AssertionError.class, () -> new TimeoutContext(TIMEOUT_SETTINGS.withTimeoutMS(10_000L)).withAdditionalReadTimeout(1));
    }

    @Test
    @DisplayName("Expired works as expected")
    void testExpired() {
        TimeoutContext smallTimeout = new TimeoutContext(TIMEOUT_SETTINGS.withTimeoutMS(1L));
        TimeoutContext longTimeout =
                new TimeoutContext(TIMEOUT_SETTINGS.withTimeoutMS(9999999L));
        TimeoutContext noTimeout = new TimeoutContext(TIMEOUT_SETTINGS);
        sleep(100);
        assertFalse(noTimeout.hasExpired());
        assertFalse(longTimeout.hasExpired());
        assertTrue(smallTimeout.hasExpired());
    }

    @Test
    @DisplayName("throws when calculating timeout if expired")
    void testThrowsWhenExpired() {
        TimeoutContext smallTimeout = new TimeoutContext(TIMEOUT_SETTINGS.withTimeoutMS(1L));
        TimeoutContext longTimeout = new TimeoutContext(TIMEOUT_SETTINGS.withTimeoutMS(9999999L));
        TimeoutContext noTimeout = new TimeoutContext(TIMEOUT_SETTINGS);
        sleep(100);

        assertThrows(MongoOperationTimeoutException.class, smallTimeout::getReadTimeoutMS);
        assertThrows(MongoOperationTimeoutException.class, smallTimeout::getWriteTimeoutMS);
        assertThrows(MongoOperationTimeoutException.class, smallTimeout::getMaxTimeMS);
        assertThrows(MongoOperationTimeoutException.class, smallTimeout::getMaxCommitTimeMS);
        assertThrows(MongoOperationTimeoutException.class, () -> smallTimeout.timeoutOrAlternative(1));
        assertDoesNotThrow(longTimeout::getReadTimeoutMS);
        assertDoesNotThrow(longTimeout::getWriteTimeoutMS);
        assertDoesNotThrow(longTimeout::getMaxTimeMS);
        assertDoesNotThrow(longTimeout::getMaxCommitTimeMS);
        assertDoesNotThrow(() -> longTimeout.timeoutOrAlternative(1));
        assertDoesNotThrow(noTimeout::getReadTimeoutMS);
        assertDoesNotThrow(noTimeout::getWriteTimeoutMS);
        assertDoesNotThrow(noTimeout::getMaxTimeMS);
        assertDoesNotThrow(noTimeout::getMaxCommitTimeMS);
        assertDoesNotThrow(() -> noTimeout.timeoutOrAlternative(1));
    }

    @Test
    @DisplayName("validates minRoundTripTime for maxTimeMS")
    void testValidatedMinRoundTripTime() {
        Supplier<TimeoutContext> supplier = () -> new TimeoutContext(TIMEOUT_SETTINGS.withTimeoutMS(100L));

        assertTrue(supplier.get().getMaxTimeMS() <= 100);
        assertTrue(supplier.get().minRoundTripTimeMS(10).getMaxTimeMS() <= 90);
        assertThrows(MongoOperationTimeoutException.class, () -> supplier.get().minRoundTripTimeMS(101).getMaxTimeMS());
        assertThrows(MongoOperationTimeoutException.class, () -> supplier.get().minRoundTripTimeMS(100).getMaxTimeMS());
    }

    private TimeoutContextTest() {
    }
}
