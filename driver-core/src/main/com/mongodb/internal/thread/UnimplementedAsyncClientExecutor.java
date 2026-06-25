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
package com.mongodb.internal.thread;

import com.mongodb.annotations.ThreadSafe;
import com.mongodb.internal.async.SingleResultCallback;

import java.time.Duration;

import static com.mongodb.assertions.Assertions.fail;

@ThreadSafe
final class UnimplementedAsyncClientExecutor implements AsyncClientExecutor {
    private static final UnimplementedAsyncClientExecutor INSTANCE = new UnimplementedAsyncClientExecutor();

    static UnimplementedAsyncClientExecutor instance() {
        return INSTANCE;
    }

    private UnimplementedAsyncClientExecutor() {
    }

    /**
     * Must not be called with any {@link Duration} except {@linkplain Duration#isZero() zero}.
     */
    @Override
    public void sleepAsync(final Duration duration, final SingleResultCallback<Void> callback) {
        DefaultAsyncClientExecutor.sleepAsync(false, duration, callback, () -> {
            throw fail();
        });
    }

    /**
     * Does nothing.
     */
    @Override
    public void close() {
    }
}
