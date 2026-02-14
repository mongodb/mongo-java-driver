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

import java.util.concurrent.atomic.AtomicReference;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertNull;

final class CallbackChain {
    @Nullable
    private Runnable next;
    private int runEnteredCounter;
    private final AtomicReference<Thread> thread;

    CallbackChain() {
        runEnteredCounter = 0;
        thread = new AtomicReference<>();
    }

    static void execute(@Nullable final CallbackChain chain, final Runnable next) {
        if (chain != null) {
            chain.execute(next);
        } else {
            next.run();
        }
    }

    // VAKOTODO figure out thread safety
    private void execute(final Runnable next) {
        assertNotNull(next);
        assertNull(this.next);
        this.next = next;

//        if (!thread.compareAndSet(null, Thread.currentThread())) {
//            assertTrue(Thread.currentThread() == thread.get());
//        }
        boolean recursive = ++runEnteredCounter > 1;
        try {
            if (recursive) {
                return;
            }
            for (Runnable localNext = next; localNext != null; localNext = this.next) {
                this.next = null;
                localNext.run();
            }
        } finally {
            runEnteredCounter--;
        }
    }
}
