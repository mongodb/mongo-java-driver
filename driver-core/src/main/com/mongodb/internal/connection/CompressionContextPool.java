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

package com.mongodb.internal.connection;

import com.mongodb.annotations.ThreadSafe;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Keeps compression contexts for reuse across messages and connections, so that a message does not pay for creating and
 * initializing native compressor state. A context in use by one message is never handed to another.
 */
@ThreadSafe
final class CompressionContextPool<T> {
    private static final int MAX_POOLED = Runtime.getRuntime().availableProcessors() * 2;

    private final ConcurrentLinkedQueue<T> pooled = new ConcurrentLinkedQueue<>();
    private final AtomicInteger pooledCount = new AtomicInteger();
    private final Supplier<T> factory;
    private final Consumer<T> destroyer;

    CompressionContextPool(final Supplier<T> factory, final Consumer<T> destroyer) {
        this.factory = factory;
        this.destroyer = destroyer;
    }

    T get() {
        T context = pooled.poll();
        if (context == null) {
            return factory.get();
        }
        pooledCount.decrementAndGet();
        return context;
    }

    /**
     * @param reusable false if the context may have been left in an unknown state, in which case it is destroyed
     */
    void release(final T context, final boolean reusable) {
        if (reusable && pooledCount.incrementAndGet() <= MAX_POOLED) {
            pooled.offer(context);
            return;
        }
        if (reusable) {
            pooledCount.decrementAndGet();
        }
        destroyer.accept(context);
    }
}
