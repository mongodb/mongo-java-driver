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

import com.mongodb.annotations.ThreadSafe;

import java.time.Duration;
import java.util.Optional;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;

/**
 * A value associated with a lifetime.
 *
 * <p>Instances are shallowly immutable.</p>
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
@ThreadSafe
public final class ExpirableValue<T> {
    private final T value;
    private final long deadline;

    public static <T> ExpirableValue<T> expired() {
        return new ExpirableValue<>(null, Duration.ofSeconds(-1), System.nanoTime());
    }

    public static <T> ExpirableValue<T> unexpired(final T value, final Duration lifetime) {
        return unexpired(value, lifetime, System.nanoTime());
    }

    @VisibleForTesting(otherwise = PRIVATE)
    static <T> ExpirableValue<T> unexpired(final T value, final Duration lifetime, final long currentNanoTime) {
        return new ExpirableValue<>(assertNotNull(value), lifetime, currentNanoTime);
    }

    private ExpirableValue(final T value, final Duration lifetime, final long currentNanoTime) {
        this.value = value;
        deadline = currentNanoTime + lifetime.toNanos();
    }

    /**
     * Returns {@link Optional#empty()} if the value is expired. Otherwise, returns an {@link Optional} describing the value.
     */
    public Optional<T> getValue() {
        return getValue(System.nanoTime());
    }

    @VisibleForTesting(otherwise = PRIVATE)
    Optional<T> getValue(final long currentNanoTime) {
        if (currentNanoTime - deadline > 0) {
            return Optional.empty();
        } else {
            return Optional.of(value);
        }
    }
}
