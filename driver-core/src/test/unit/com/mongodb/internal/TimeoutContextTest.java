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

import com.mongodb.session.ClientSession;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS;
import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS_WITH_LEGACY_SETTINGS;
import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS_WITH_MAX_AWAIT_TIME;
import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS_WITH_MAX_COMMIT;
import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS_WITH_MAX_TIME;
import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS_WITH_MAX_TIME_AND_AWAIT_TIME;
import static com.mongodb.ClusterFixture.sleep;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
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

        assertEquals(0, getMaxTimeMS(timeoutContext));
        assertEquals(0, timeoutContext.getMaxAwaitTimeMS());
        assertEquals(0, timeoutContext.getMaxCommitTimeMS());
        assertEquals(0, timeoutContext.getReadTimeoutMS());
    }

    @Test
    @DisplayName("MaxTimeMS set")
    void testMaxTimeMSSet() {
        TimeoutContext timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS_WITH_MAX_TIME);

        assertEquals(100, getMaxTimeMS(timeoutContext));
        assertEquals(0, timeoutContext.getMaxAwaitTimeMS());
    }

    @Test
    @DisplayName("MaxAwaitTimeMS set")
    void testMaxAwaitTimeMSSet() {
        TimeoutContext timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS_WITH_MAX_AWAIT_TIME);

        assertEquals(0, getMaxTimeMS(timeoutContext));
        assertEquals(101, timeoutContext.getMaxAwaitTimeMS());
    }

    @Test
    @DisplayName("MaxTimeMS and MaxAwaitTimeMS set")
    void testMaxTimeMSAndMaxAwaitTimeMSSet() {
        TimeoutContext timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS_WITH_MAX_TIME_AND_AWAIT_TIME);

        assertEquals(101, getMaxTimeMS(timeoutContext));
        assertEquals(1001, timeoutContext.getMaxAwaitTimeMS());
    }

    @Test
    @DisplayName("MaxCommitTimeMS set")
    void testMaxCommitTimeMSSet() {
        TimeoutContext timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS_WITH_MAX_COMMIT);

        assertEquals(0, getMaxTimeMS(timeoutContext));
        assertEquals(0, timeoutContext.getMaxAwaitTimeMS());
        assertEquals(999L, timeoutContext.getMaxCommitTimeMS());
    }

    @Test
    @DisplayName("All deprecated options set")
    void testAllDeprecatedOptionsSet() {
        TimeoutContext timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS_WITH_LEGACY_SETTINGS);

        assertEquals(101, getMaxTimeMS(timeoutContext));
        assertEquals(1001, timeoutContext.getMaxAwaitTimeMS());
        assertEquals(999, timeoutContext.getMaxCommitTimeMS());
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
    }

    @Test
    @DisplayName("throws when calculating timeout if expired")
    void testThrowsWhenExpired() {
        TimeoutContext noTimeout = new TimeoutContext(TIMEOUT_SETTINGS);
        sleep(100);

        assertDoesNotThrow(noTimeout::getReadTimeoutMS);
        assertDoesNotThrow(noTimeout::getWriteTimeoutMS);
        assertDoesNotThrow(noTimeout::getConnectTimeoutMs);
        assertDoesNotThrow(() -> getMaxTimeMS(noTimeout));
        assertDoesNotThrow(noTimeout::getMaxCommitTimeMS);
        assertDoesNotThrow(() -> noTimeout.timeoutOrAlternative(1));
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
    @DisplayName("should override maxTimeMS when MaxTimeSupplier is set")
    void shouldOverrideMaximeMS() {
        TimeoutContext timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS.withMaxTimeMS(1));

        timeoutContext = timeoutContext.withMaxTimeOverride(2L);

        assertEquals(2, getMaxTimeMS(timeoutContext));
    }

    @Test
    @DisplayName("should reset maxTimeMS to default behaviour")
    void shouldResetMaximeMS() {
        TimeoutContext timeoutContext = new TimeoutContext(TIMEOUT_SETTINGS.withMaxTimeMS(100));
        timeoutContext = timeoutContext.withMaxTimeOverride(1L);

        timeoutContext = timeoutContext.withDefaultMaxTime();

        assertTrue(getMaxTimeMS(timeoutContext) > 1);
    }

    private TimeoutContextTest() {
    }
}
