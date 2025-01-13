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
package com.mongodb.internal.async;

import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.lang.Nullable;

import static com.mongodb.assertions.Assertions.assertNotNull;

@NotThreadSafe
public final class MutableValue<T> {
    private T value;

    public MutableValue(@Nullable final T value) {
        this.value = value;
    }

    public MutableValue() {
        this(null);
    }

    public T get() {
        return assertNotNull(value);
    }

    @Nullable
    public T getNullable() {
        return value;
    }

    public void set(@Nullable final T value) {
        this.value = value;
    }
}
