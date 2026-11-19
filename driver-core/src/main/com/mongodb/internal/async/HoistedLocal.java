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
import static com.mongodb.assertions.Assertions.fail;

/**
 * A mutable holder for one value, used only by generated async code (tools/async-generator) for a
 * sync local that must live across an async callback chain. Not for hand-written code.
 */
@NotThreadSafe
public final class HoistedLocal<T> {
    private T value;
    private boolean assigned;

    public HoistedLocal(@Nullable final T value) {
        this.value = value;
        this.assigned = true;
    }

    public HoistedLocal() {
        this(null);
    }

    /**
     * Creates a value that mirrors a declared-but-unassigned local variable. Reading it before {@link #set} fails,
     * which turns a definite-assignment violation into an immediate error rather than a silent {@code null}.
     */
    public static <T> HoistedLocal<T> unassigned() {
        HoistedLocal<T> result = new HoistedLocal<>();
        result.assigned = false;
        return result;
    }

    public T get() {
        return assertNotNull(getNullable());
    }

    @Nullable
    public T getNullable() {
        if (!assigned) {
            throw fail("HoistedLocal read before assignment");
        }
        return value;
    }

    public void set(@Nullable final T value) {
        this.value = value;
        this.assigned = true;
    }
}
