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

import com.mongodb.MongoInterruptedException;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import static com.mongodb.internal.thread.InterruptionUtil.interruptAndCreateMongoInterruptedException;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class Locks {
    public static void withUninterruptibleLock(final Lock lock, final Runnable action) {
        withUninterruptibleLock(lock, () -> {
            action.run();
            return null;
        });
    }

    public static <V> V withUninterruptibleLock(final Lock lock, final Supplier<V> supplier) {
        return checkedWithUninterruptibleLock(lock, supplier::get);
    }

    public static <V, E extends Exception> V checkedWithUninterruptibleLock(final Lock lock, final CheckedSupplier<V, E> supplier) throws E {
        lock.lock();
        try {
            return supplier.get();
        } finally {
            lock.unlock();
        }
    }

    public static void withLock(final Lock lock, final Runnable action) {
        withLock(lock, () -> {
            action.run();
            return null;
        });
    }

    public static <V> V withLock(final Lock lock, final Supplier<V> supplier) {
        return checkedWithLock(lock, supplier::get);
    }

    public static <V, E extends Exception> V checkedWithLock(final Lock lock, final CheckedSupplier<V, E> supplier) throws E {
        lockInterruptibly(lock);
        try {
            return supplier.get();
        } finally {
            lock.unlock();
        }
    }

    public static void lockInterruptibly(final Lock lock) throws MongoInterruptedException {
        try {
            lock.lockInterruptibly();
        } catch (InterruptedException e) {
            throw interruptAndCreateMongoInterruptedException("Interrupted waiting for lock", e);
        }
    }

    public static void withUninterruptibleUnfairLock(final ReentrantLock lock, final Runnable action) {
        withUninterruptibleUnfairLock(lock, () -> {
            action.run();
            return null;
        });
    }

    public static <V> V withUninterruptibleUnfairLock(final ReentrantLock lock, final Supplier<V> supplier) {
        lockUninterruptiblyUnfair(lock);
        try {
            return supplier.get();
        } finally {
            lock.unlock();
        }
    }

    private static void lockUninterruptiblyUnfair(
            // The type must be `ReentrantLock`, not `Lock`,
            // because only `ReentrantLock.tryLock` is documented to have the barging (unfair) behavior.
            final ReentrantLock lock) {
        if (!lock.tryLock()) {
            lock.lock();
        }
    }

    public static void lockInterruptiblyUnfair(
            // The type must be `ReentrantLock`, not `Lock`,
            // because only `ReentrantLock.tryLock` is documented to have the barging (unfair) behavior.
            final ReentrantLock lock) throws MongoInterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            throw interruptAndCreateMongoInterruptedException(null, null);
        }
        if (!lock.tryLock()) {
            lockInterruptibly(lock);
        }
    }

    private Locks() {
    }
}
