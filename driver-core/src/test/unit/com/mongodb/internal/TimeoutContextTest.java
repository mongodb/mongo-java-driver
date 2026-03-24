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
import com.mongodb.internal.time.Timeout;
import com.mongodb.lang.Nullable;
import com.mongodb.session.ClientSession;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS;
import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS_WITH_INFINITE_TIMEOUT;
import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS_WITH_LEGACY_SETTINGS;
import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS_WITH_MAX_AWAIT_TIME;
import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS_WITH_MAX_COMMIT;
import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS_WITH_MAX_TIME;
import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS_WITH_MAX_TIME_AND_AWAIT_TIME;
import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS_WITH_TIMEOUT;
import static com.mongodb.ClusterFixture.sleep;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class TimeoutContextTest {

    public static long getMaxTimeMS(final TimeoutContext timeoutContext) {
        long[] result = {0L};
        timeoutContext.runMaxTimeMS((ms) -> result[0] = ms);
        return result[0];
    }

    @Test
    @DisplayName("test defaults")
    void testDefaults() {
        TimeoutContext timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS);

        assertFalse(timeoutContext.hasTimeoutMS());
        assertEquals(0, getMaxTimeMS(timeoutContext));
        assertEquals(0, timeoutContext.getMaxAwaitTimeMS());
        assertEquals(0, timeoutContext.getMaxCommitTimeMS());
        assertEquals(0, timeoutContext.getReadTimeoutMS());
    }

    @Test
    @DisplayName("Uses timeoutMS if set")
    void testUsesTimeoutMSIfSet() {
        TimeoutContext timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS_WITH_TIMEOUT);

        assertTrue(timeoutContext.hasTimeoutMS());
        assertTrue(getMaxTimeMS(timeoutContext) > 0);
        assertEquals(0, timeoutContext.getMaxAwaitTimeMS());
    }

    @Test
    @DisplayName("infinite timeoutMS")
    void testInfiniteTimeoutMS() {
        TimeoutContext timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS_WITH_INFINITE_TIMEOUT);

        assertTrue(timeoutContext.hasTimeoutMS());
        assertEquals(0, getMaxTimeMS(timeoutContext));
        assertEquals(0, timeoutContext.getMaxAwaitTimeMS());
    }

    @Test
    @DisplayName("MaxTimeMS set")
    void testMaxTimeMSSet() {
        TimeoutContext timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS_WITH_MAX_TIME);

        assertFalse(timeoutContext.hasTimeoutMS());
        assertEquals(100, getMaxTimeMS(timeoutContext));
        assertEquals(0, timeoutContext.getMaxAwaitTimeMS());
    }

    @Test
    @DisplayName("MaxAwaitTimeMS set")
    void testMaxAwaitTimeMSSet() {
        TimeoutContext timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS_WITH_MAX_AWAIT_TIME);

        assertFalse(timeoutContext.hasTimeoutMS());
        assertEquals(0, getMaxTimeMS(timeoutContext));
        assertEquals(101, timeoutContext.getMaxAwaitTimeMS());
    }

    @Test
    @DisplayName("MaxTimeMS and MaxAwaitTimeMS set")
    void testMaxTimeMSAndMaxAwaitTimeMSSet() {
        TimeoutContext timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS_WITH_MAX_TIME_AND_AWAIT_TIME);

        assertFalse(timeoutContext.hasTimeoutMS());
        assertEquals(101, getMaxTimeMS(timeoutContext));
        assertEquals(1001, timeoutContext.getMaxAwaitTimeMS());
    }

    @Test
    @DisplayName("MaxCommitTimeMS set")
    void testMaxCommitTimeMSSet() {
        TimeoutContext timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS_WITH_MAX_COMMIT);

        assertFalse(timeoutContext.hasTimeoutMS());
        assertEquals(0, getMaxTimeMS(timeoutContext));
        assertEquals(0, timeoutContext.getMaxAwaitTimeMS());
        assertEquals(999L, timeoutContext.getMaxCommitTimeMS());
    }

    @Test
    @DisplayName("All deprecated options set")
    void testAllDeprecatedOptionsSet() {
        TimeoutContext timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS_WITH_LEGACY_SETTINGS);

        assertFalse(timeoutContext.hasTimeoutMS());
        assertEquals(101, getMaxTimeMS(timeoutContext));
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
        assertEquals(0, timeoutContext.getMaxCommitTimeMS());

        timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS.withTimeoutMS(999L));
        assertTrue(timeoutContext.getMaxCommitTimeMS() <= 999);
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
        assertFalse(hasExpired(noTimeout.getTimeout()));
        assertFalse(hasExpired(longTimeout.getTimeout()));
        assertTrue(hasExpired(smallTimeout.getTimeout()));
    }

    private static boolean hasExpired(@Nullable final Timeout timeout) {
        return Timeout.nullAsInfinite(timeout).call(NANOSECONDS, () -> false, (ns) -> false, () -> true);
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
        assertThrows(MongoOperationTimeoutException.class, smallTimeout::getConnectTimeoutMs);
        assertThrows(MongoOperationTimeoutException.class, () -> getMaxTimeMS(smallTimeout));
        assertThrows(MongoOperationTimeoutException.class, smallTimeout::getMaxCommitTimeMS);
        assertThrows(MongoOperationTimeoutException.class, () -> smallTimeout.timeoutOrAlternative(1));
        assertDoesNotThrow(longTimeout::getReadTimeoutMS);
        assertDoesNotThrow(longTimeout::getWriteTimeoutMS);
        assertDoesNotThrow(longTimeout::getConnectTimeoutMs);
        assertDoesNotThrow(() -> getMaxTimeMS(longTimeout));
        assertDoesNotThrow(longTimeout::getMaxCommitTimeMS);
        assertDoesNotThrow(() -> longTimeout.timeoutOrAlternative(1));
        assertDoesNotThrow(noTimeout::getReadTimeoutMS);
        assertDoesNotThrow(noTimeout::getWriteTimeoutMS);
        assertDoesNotThrow(noTimeout::getConnectTimeoutMs);
        assertDoesNotThrow(() -> getMaxTimeMS(noTimeout));
        assertDoesNotThrow(noTimeout::getMaxCommitTimeMS);
        assertDoesNotThrow(() -> noTimeout.timeoutOrAlternative(1));
    }

    @Test
    @DisplayName("validates minRoundTripTime for maxTimeMS")
    void testValidatedMinRoundTripTime() {
        Supplier<TimeoutContext> supplier = () -> new TimeoutContext(TIMEOUT_SETTINGS.withTimeoutMS(100L));

        assertTrue(getMaxTimeMS(supplier.get()) <= 100);
        assertTrue(getMaxTimeMS(supplier.get().withMinRoundTripTime(10)) <= 90);
        assertThrows(MongoOperationTimeoutException.class, () -> getMaxTimeMS(supplier.get().withMinRoundTripTimeMS(101)));
        assertThrows(MongoOperationTimeoutException.class, () -> getMaxTimeMS(supplier.get().withMinRoundTripTimeMS(100)));
    }

    @Test
    @DisplayName("Test createTimeoutContext handles legacy settings")
    void testCreateTimeoutContextLegacy() {
        TimeoutContext sessionTimeoutContext = new TimeoutContext(TIMEOUT_SETTINGS);
        TimeoutContext timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS_WITH_LEGACY_SETTINGS);

        ClientSession clientSession = Mockito.mock(ClientSession.class);
        Mockito.when(clientSession.getTimeoutContext()).thenReturn(sessionTimeoutContext);

        TimeoutContext actualTimeoutContext = TimeoutContext.createTimeoutContext(clientSession, timeoutContext.getTimeoutSettings());
        assertEquals(timeoutContext, actualTimeoutContext);
    }

    @Test
    @DisplayName("Test createTimeoutContext with timeout legacy settings")
    void testCreateTimeoutContextWithTimeoutLegacy() {
        TimeoutContext sessionTimeoutContext = new TimeoutContext(TIMEOUT_SETTINGS_WITH_TIMEOUT);
        TimeoutContext timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS_WITH_LEGACY_SETTINGS);

        ClientSession clientSession = Mockito.mock(ClientSession.class);
        Mockito.when(clientSession.getTimeoutContext()).thenReturn(sessionTimeoutContext);

        TimeoutContext actualTimeoutContext = TimeoutContext.createTimeoutContext(clientSession, timeoutContext.getTimeoutSettings());
        assertEquals(sessionTimeoutContext, actualTimeoutContext);
    }

    @Test
    @DisplayName("Test createTimeoutContext with timeout")
    void testCreateTimeoutContextWithTimeout() {
        TimeoutContext sessionTimeoutContext = new TimeoutContext(TIMEOUT_SETTINGS_WITH_TIMEOUT);
        TimeoutContext timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS_WITH_TIMEOUT.withMaxAwaitTimeMS(123));

        ClientSession clientSession = Mockito.mock(ClientSession.class);
        Mockito.when(clientSession.getTimeoutContext()).thenReturn(sessionTimeoutContext);

        TimeoutContext actualTimeoutContext = TimeoutContext.createTimeoutContext(clientSession, timeoutContext.getTimeoutSettings());
        assertEquals(sessionTimeoutContext, actualTimeoutContext);
    }

    @Test
    @DisplayName("should override maxTimeMS when MaxTimeSupplier is set")
    void shouldOverrideMaximeMS() {
        TimeoutContext timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS.withTimeoutMS(100L).withMaxTimeMS(1));

        timeoutContext = timeoutContext.withMaxTimeOverride(2L);

        assertEquals(2, getMaxTimeMS(timeoutContext));
    }

    @Test
    @DisplayName("should reset maxTimeMS to default behaviour")
    void shouldResetMaximeMS() {
        TimeoutContext timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS.withTimeoutMS(100L).withMaxTimeMS(1));
        timeoutContext = timeoutContext.withMaxTimeOverride(1L);

        timeoutContext = timeoutContext.withDefaultMaxTime();

        assertTrue(getMaxTimeMS(timeoutContext) > 1);
    }

    static Stream<Arguments> shouldChooseConnectTimeoutWhenItIsLessThenTimeoutMs() {
        return Stream.of(
                //connectTimeoutMS, timeoutMS, expected
                Arguments.of(500L, 1000L, 500L),
                Arguments.of(0L, null, 0L),
                Arguments.of(1000L, null, 1000L),
                Arguments.of(1000L, 0L, 1000L),
                Arguments.of(0L, 0L, 0L)
        );
    }

    @ParameterizedTest
    @MethodSource
    @DisplayName("should choose connectTimeoutMS when connectTimeoutMS is less than timeoutMS")
    void shouldChooseConnectTimeoutWhenItIsLessThenTimeoutMs(final Long connectTimeoutMS,
                                                          final Long timeoutMS,
                                                          final long expected) {
        TimeoutContext timeoutContext = new TimeoutContext(
                new TimeoutSettings(0,
                connectTimeoutMS,
                0,
                timeoutMS,
                0));

        long calculatedTimeoutMS = timeoutContext.getConnectTimeoutMs();
        assertEquals(expected, calculatedTimeoutMS);
    }


    static Stream<Arguments> shouldChooseTimeoutMsWhenItIsLessThenConnectTimeoutMS() {
        return Stream.of(
                //connectTimeoutMS, timeoutMS, expected
                Arguments.of(1000L, 1000L, 999),
                Arguments.of(1000L, 500L, 499L),
                Arguments.of(0L, 1000L, 999L)
        );
    }

    @DisplayName("should choose timeoutMS when timeoutMS is less than connectTimeoutMS")
    @ParameterizedTest(name = "should choose timeoutMS when timeoutMS is less than connectTimeoutMS. "
            + "Parameters: connectTimeoutMS: {0}, timeoutMS: {1}, expected: {2}")
    @MethodSource
    void shouldChooseTimeoutMsWhenItIsLessThenConnectTimeoutMS(final Long connectTimeoutMS,
                                                          final Long timeoutMS,
                                                          final long expected) {
        TimeoutContext timeoutContext = new TimeoutContext(
                new TimeoutSettings(0,
                        connectTimeoutMS,
                        0,
                        timeoutMS,
                        0));

        long calculatedTimeoutMS = timeoutContext.getConnectTimeoutMs();
        assertTrue(expected - calculatedTimeoutMS <= 2);
    }

    private TimeoutContextTest() {
    }
}
