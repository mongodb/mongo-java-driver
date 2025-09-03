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
import com.mongodb.internal.function.CheckedSupplier;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Supplier;

import static com.mongodb.internal.thread.InterruptionUtil.interruptAndCreateMongoInterruptedException;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class Locks {
    public static void withLock(final Lock lock, final Runnable action) {
        withLock(lock, () -> {
            action.run();
            return null;
        });
    }

    public static void withInterruptibleLock(final StampedLock lock, final Runnable runnable) throws MongoInterruptedException{
        long stamp;
        try {
            stamp = lock.writeLockInterruptibly();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MongoInterruptedException("Interrupted waiting for lock", e);
        }
        try {
            runnable.run();
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    public static <V> V withLock(final Lock lock, final Supplier<V> supplier) {
        return checkedWithLock(lock, supplier::get);
    }

    public static <V, E extends Exception> V checkedWithLock(final Lock lock, final CheckedSupplier<V, E> supplier) throws E {
        lock.lock();
        try {
            return supplier.get();
        } finally {
            lock.unlock();
        }
    }

    public static void withInterruptibleLock(final Lock lock, final Runnable action) throws MongoInterruptedException {
        withInterruptibleLock(lock, () -> {
            action.run();
            return null;
        });
    }

    public static <V> V withInterruptibleLock(final Lock lock, final Supplier<V> supplier) throws MongoInterruptedException {
        return checkedWithInterruptibleLock(lock, supplier::get);
    }

    public static <V, E extends Exception> V checkedWithInterruptibleLock(final Lock lock, final CheckedSupplier<V, E> supplier)
            throws MongoInterruptedException, E {
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

    private Locks() {
    }
}
