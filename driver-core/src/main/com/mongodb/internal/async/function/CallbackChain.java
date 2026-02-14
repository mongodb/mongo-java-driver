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
package com.mongodb.internal.async.function;

import com.mongodb.lang.Nullable;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertNull;

public final class CallbackChain {
    @Nullable
    private Runnable next;

    public CallbackChain() {
    }

    public static void addOrRun(@Nullable final CallbackChain chain, final Runnable next) {
        if (chain != null) {
            chain.add(next);
        } else {
            next.run();
        }
    }

    public static void run(final @Nullable CallbackChain chain) {
        if (chain != null) {
            chain.run();
        }
    }

    private void add(final Runnable next) {
        assertNotNull(next);
        assertNull(this.next);
        this.next = next;
    }

    private void run() {
        for (Runnable localNext = next; localNext != null; localNext = next) {
            next = null;
            localNext.run();
        }
    }
}
