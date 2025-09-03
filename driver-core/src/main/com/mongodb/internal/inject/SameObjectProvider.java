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
package com.mongodb.internal.inject;

import com.mongodb.annotations.ThreadSafe;
import com.mongodb.lang.Nullable;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertTrue;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
@ThreadSafe
public final class SameObjectProvider<T> implements Provider<T> {
    private final AtomicReference<T> object;

    private SameObjectProvider(@Nullable final T o) {
        object = new AtomicReference<>();
        if (o != null) {
            initialize(o);
        }
    }

    @Override
    public T get() {
        return assertNotNull(object.get());
    }

    @Override
    public Optional<T> optional() {
        return Optional.of(get());
    }

    public void initialize(final T o) {
        assertTrue(object.compareAndSet(null, o));
    }

    public static <T> SameObjectProvider<T> initialized(final T o) {
        return new SameObjectProvider<>(o);
    }

    public static <T> SameObjectProvider<T> uninitialized() {
        return new SameObjectProvider<>(null);
    }
}
