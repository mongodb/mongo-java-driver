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

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.util.Collection;

import static com.mongodb.ClusterFixture.TIMEOUT_SETTINGS;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

final class TimeoutSettingsTest {

    @TestFactory
    Collection<DynamicTest> timeoutSettingsTest() {
        return asList(
                dynamicTest("test defaults", () -> {
                    TimeoutSettings timeoutSettings = TIMEOUT_SETTINGS;
                    assertAll(
                            () -> assertEquals(30_000, timeoutSettings.getServerSelectionTimeoutMS()),
                            () -> assertEquals(10_000, timeoutSettings.getConnectTimeoutMS()),
                            () -> assertEquals(0, timeoutSettings.getReadTimeoutMS()),
                            () -> assertNull(timeoutSettings.getTimeoutMS()),
                            () -> assertNull(timeoutSettings.getDefaultTimeoutMS()),
                            () -> assertEquals(0, timeoutSettings.getMaxTimeMS()),
                            () -> assertEquals(0, timeoutSettings.getMaxAwaitTimeMS()),
                            () -> assertEquals(0, timeoutSettings.getMaxCommitTimeMS()),
                            () -> assertNull(timeoutSettings.getWTimeoutMS())
                    );
                }),
                dynamicTest("test overrides", () -> {
                    TimeoutSettings timeoutSettings = TIMEOUT_SETTINGS
                            .withTimeoutMS(100)
                            .withDefaultTimeoutMS(1000)
                            .withMaxTimeMS(111)
                            .withMaxAwaitTimeMS(11)
                            .withMaxCommitMS(999L)
                            .withReadTimeoutMS(11_000)
                            .withWTimeoutMS(222L);
                    assertAll(
                            () -> assertEquals(30_000, timeoutSettings.getServerSelectionTimeoutMS()),
                            () -> assertEquals(10_000, timeoutSettings.getConnectTimeoutMS()),
                            () -> assertEquals(11_000, timeoutSettings.getReadTimeoutMS()),
                            () -> assertEquals(100, timeoutSettings.getTimeoutMS()),
                            () -> assertEquals(1000, timeoutSettings.getDefaultTimeoutMS()),
                            () -> assertEquals(111, timeoutSettings.getMaxTimeMS()),
                            () -> assertEquals(11, timeoutSettings.getMaxAwaitTimeMS()),
                            () -> assertEquals(999, timeoutSettings.getMaxCommitTimeMS()),
                            () -> assertEquals(222, timeoutSettings.getWTimeoutMS())
                    );
                })
        );
    }

    @Test
    public void testTimeoutSettingsValidation() {
        assertThrows(IllegalArgumentException.class, () -> TIMEOUT_SETTINGS.withTimeoutMS(-1));
        assertThrows(IllegalArgumentException.class, () -> TIMEOUT_SETTINGS.withMaxAwaitTimeMS(-1));
        assertThrows(IllegalArgumentException.class, () -> TIMEOUT_SETTINGS.withMaxTimeMS(-1));
        assertThrows(IllegalArgumentException.class, () -> TIMEOUT_SETTINGS.withTimeoutMS(10).withMaxAwaitTimeMS(11));
    }

    private TimeoutSettingsTest() {
    }
}
