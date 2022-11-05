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

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static com.mongodb.internal.ExpirableValue.expired;
import static com.mongodb.internal.ExpirableValue.expirable;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ExpirableValueTest {

    @Test
    void testExpired() {
        assertFalse(expired().getValue().isPresent());
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Test
    void testExpirable() {
        assertAll(
                () -> assertThrows(AssertionError.class, () -> expirable(null, Duration.ofNanos(1))),
                () -> assertThrows(AssertionError.class, () -> expirable(1, null)),
                () -> assertFalse(expirable(1, Duration.ofNanos(-1)).getValue().isPresent()),
                () -> assertFalse(expirable(1, Duration.ZERO).getValue().isPresent()),
                () -> assertEquals(1, expirable(1, Duration.ofSeconds(1)).getValue().get()),
                () -> {
                    ExpirableValue<Integer> expirableValue = expirable(1, Duration.ofNanos(1));
                    Thread.sleep(1);
                    assertFalse(expirableValue.getValue().isPresent());
                },
                () -> {
                    ExpirableValue<Integer> expirableValue = expirable(1, Duration.ofMinutes(60), Long.MAX_VALUE);
                    assertEquals(1, expirableValue.getValue(Long.MAX_VALUE + Duration.ofMinutes(30).toNanos()).get());
                },
                () -> {
                    ExpirableValue<Integer> expirableValue = expirable(1, Duration.ofMinutes(60), Long.MAX_VALUE);
                    assertEquals(1, expirableValue.getValue(Long.MAX_VALUE + Duration.ofMinutes(30).toNanos()).get());
                    assertFalse(expirableValue.getValue(Long.MAX_VALUE + Duration.ofMinutes(61).toNanos()).isPresent());
                },
                () -> {
                    ExpirableValue<Integer> expirableValue = expirable(1, Duration.ofNanos(10), Long.MAX_VALUE - 20);
                    assertFalse(expirableValue.getValue(Long.MAX_VALUE - 20 + Duration.ofNanos(30).toNanos()).isPresent());
                });
    }
}
