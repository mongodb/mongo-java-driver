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
 *
 */

package com.mongodb.internal.crypt.capi;

import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import static org.bson.assertions.Assertions.isTrue;

/**
 * Tracks the closed state of a {@link MongoCryptContextImpl} and prevents native calls on its handle from
 * overlapping with destruction: a call either completes or, once {@link #close} has run, fails with
 * {@link IllegalStateException} instead of dereferencing freed memory.
 *
 * <p>Contract: every method on {@link MongoCryptContextImpl} and on the {@link MongoKeyDecryptorImpl}s it
 * hands out must route its work through {@link #guarded}, which fails if the context is already closed and
 * otherwise holds the lock so {@link #close} cannot free the native handle while the call is in flight.
 * The one instance is shared with the key decryptors because a
 * {@code mongocrypt_kms_ctx_t} is freed with the {@code mongocrypt_ctx_t} that owns it.
 *
 * <p>The {@code com.mongodb.internal.Locks} idiom is not reusable here: it lives in {@code driver-core}, and
 * {@code mongodb-crypt} depends only on {@code bson} and JNA.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
final class MongoCryptContextLifetime {
    private final ReentrantLock lock = new ReentrantLock();
    /** Guarded by {@link #lock} — every read and write holds it, hence not {@code volatile}. */
    private boolean closed;

    <T> T guarded(final Supplier<T> nativeCall) {
        lock.lock();
        try {
            isTrue("open", !closed);
            return nativeCall.get();
        } finally {
            lock.unlock();
        }
    }

    void guarded(final Runnable nativeCall) {
        guarded(() -> {
            nativeCall.run();
            return null;
        });
    }

    /**
     * Marks this lifetime closed and runs {@code destroy} exactly once, under the same lock the guarded calls
     * take. Safe to call concurrently and repeatedly.
     */
    void close(final Runnable destroy) {
        lock.lock();
        try {
            if (!closed) {
                closed = true;
                destroy.run();
            }
        } finally {
            lock.unlock();
        }
    }
}
