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

import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoServerUnavailableException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.internal.VisibleForTesting;
import com.mongodb.internal.connection.ConcurrentLinkedDeque.RemovalReportingIterator;
import com.mongodb.lang.Nullable;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;

/**
 * A concurrent pool implementation.
 *
 * <p>This class should not be considered a part of the public API.</p>
 */
public class ConcurrentPool<T> implements Pool<T> {
    /**
     * {@link Integer#MAX_VALUE}.
     */
    public static final int INFINITE_SIZE = Integer.MAX_VALUE;

    private final int maxSize;
    private final ItemFactory<T> itemFactory;

    private final ConcurrentLinkedDeque<T> available = new ConcurrentLinkedDeque<T>();
    private final StateAndPermits stateAndPermits;
    private final String poolClosedMessage;

    public enum Prune {
        /**
         * Prune this element
         */
        YES,
        /**
         * Don't prone this element
         */
        NO,
        /**
         * Don't prune this element and stop attempting to prune additional elements
         */
        STOP
    }
    /**
     * Factory for creating and closing pooled items.
     *
     * @param <T>
     */
    public interface ItemFactory<T> {
        T create();

        void close(T t);

        Prune shouldPrune(T t);
    }

    /**
     * Initializes a new pool of objects.
     *
     * @param maxSize     max to hold to at any given time, must be positive.
     * @param itemFactory factory used to create and close items in the pool
     */
    public ConcurrentPool(final int maxSize, final ItemFactory<T> itemFactory) {
        this(maxSize, itemFactory, "The pool is closed");
    }

    public ConcurrentPool(final int maxSize, final ItemFactory<T> itemFactory, final String poolClosedMessage) {
        assertTrue(maxSize > 0);
        this.maxSize = maxSize;
        this.itemFactory = itemFactory;
        stateAndPermits = new StateAndPermits(maxSize, this::poolClosedException);
        this.poolClosedMessage = notNull("poolClosedMessage", poolClosedMessage);
    }

    /**
     * Return an instance of T to the pool.  This method simply calls {@code release(t, false)}
     * Must not throw {@link Exception}s.
     *
     * @param t item to return to the pool
     */
    @Override
    public void release(final T t) {
        release(t, false);
    }

    /**
     * call done when you are done with an object from the pool if there is room and the object is ok will get added
     * Must not throw {@link Exception}s.
     *
     * @param t     item to return to the pool
     * @param prune true if the item should be closed, false if it should be put back in the pool
     */
    @Override
    public void release(final T t, final boolean prune) {
        if (t == null) {
            throw new IllegalArgumentException("Can not return a null item to the pool");
        }
        if (stateAndPermits.closed()) {
            close(t);
            return;
        }

        if (prune) {
            close(t);
        } else {
            available.addLast(t);
        }

        stateAndPermits.releasePermit();
    }

    /**
     * Is equivalent to {@link #get(long, TimeUnit)} called with an infinite timeout.
     *
     * @return An object from the pool.
     */
    @Override
    public T get() {
        return get(-1, TimeUnit.MILLISECONDS);
    }

    /**
     * Gets an object from the pool. Blocks until an object is available, or the specified {@code timeout} expires,
     * or the pool is {@linkplain #close() closed}/{@linkplain #pause(Supplier) paused}.
     *
     * @param timeout See {@link com.mongodb.internal.Timeout#startNow(long, TimeUnit)}.
     * @param timeUnit the time unit of the timeout
     * @return An object from the pool, or null if can't get one in the given waitTime
     * @throws MongoTimeoutException if the timeout has been exceeded
     */
    @Override
    public T get(final long timeout, final TimeUnit timeUnit) {
        stateAndPermits.throwIfClosedOrPaused();

        if (!stateAndPermits.acquirePermitFair(timeout, timeUnit)) {
            throw new MongoTimeoutException(String.format("Timeout waiting for a pooled item after %d %s", timeout, timeUnit));
        }

        T t = available.pollLast();
        if (t == null) {
            t = createNewAndReleasePermitIfFailure();
        }

        return t;
    }

    /**
     * This method is similar to {@link #get(long, TimeUnit)} with 0 timeout.
     * The difference is that it never creates a new element
     * and returns {@code null} instead of throwing {@link MongoTimeoutException}.
     */
    @Nullable
    T getImmediateUnfair() {
        stateAndPermits.throwIfClosedOrPaused();
        T element = null;
        if (stateAndPermits.acquirePermitImmediateUnfair()) {
            element = available.pollLast();
            if (element == null) {
                stateAndPermits.releasePermit();
            }
        }
        return element;
    }

    public void prune() {
        for (RemovalReportingIterator<T> iter = available.iterator(); iter.hasNext();) {
            T cur = iter.next();
            Prune shouldPrune = itemFactory.shouldPrune(cur);

            if (shouldPrune == Prune.STOP) {
                break;
            }

            if (shouldPrune == Prune.YES) {
                boolean removed = iter.reportingRemove();
                if (removed) {
                    close(cur);
                }
            }
        }
    }

    /**
     * Try to populate this pool with items so that {@link #getCount()} is not smaller than {@code minSize}.
     * The {@code initAndRelease} action throwing an exception causes this method to stop and re-throw that exception.
     *
     * @param initAndRelease An action applied to non-{@code null} new items.
     * If an exception is thrown by the action, the action must {@linkplain #release(Object, boolean) prune} the item.
     * Otherwise, the action must {@linkplain #release(Object) release} the item.
     */
    public void ensureMinSize(final int minSize, final Consumer<T> initAndRelease) {
        stateAndPermits.throwIfClosedOrPaused();
        while (getCount() < minSize) {
            if (!stateAndPermits.acquirePermitFair(0, TimeUnit.MILLISECONDS)) {
                break;
            }
            initAndRelease.accept(createNewAndReleasePermitIfFailure());
        }
    }

    private T createNewAndReleasePermitIfFailure() {
        try {
            T newMember = itemFactory.create();
            if (newMember == null) {
                throw new MongoInternalException("The factory for the pool created a null item");
            }
            return newMember;
        } catch (RuntimeException e) {
            stateAndPermits.releasePermit();
            throw e;
        }
    }

    /**
     * @param timeout See {@link com.mongodb.internal.Timeout#startNow(long, TimeUnit)}.
     */
    @VisibleForTesting(otherwise = PRIVATE)
    boolean acquirePermit(final long timeout, final TimeUnit timeUnit) {
        return stateAndPermits.acquirePermitFair(timeout, timeUnit);
    }

    /**
     * Clears the pool of all objects.
     * Must not throw {@link Exception}s.
     */
    @Override
    public void close() {
        if (stateAndPermits.close()) {
            Iterator<T> iter = available.iterator();
            while (iter.hasNext()) {
                T t = iter.next();
                close(t);
                iter.remove();
            }
        }
    }

    int getMaxSize() {
        return maxSize;
    }

    public int getInUseCount() {
        return maxSize - stateAndPermits.permits();
    }

    public int getAvailableCount() {
        return available.size();
    }

    public int getCount() {
        return getInUseCount() + getAvailableCount();
    }

    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("pool: ")
           .append(" maxSize: ").append(sizeToString(maxSize))
           .append(" availableCount ").append(getAvailableCount())
           .append(" inUseCount ").append(getInUseCount());
        return buf.toString();
    }

    /**
     * Must not throw {@link Exception}s, so swallow exceptions from {@link ItemFactory#close(Object)}.
     */
    private void close(final T t) {
        try {
            itemFactory.close(t);
        } catch (RuntimeException e) {
            // ItemFactory.close() really should not throw
        }
    }

    void ready() {
        stateAndPermits.ready();
    }

    void pause(final Supplier<MongoException> causeSupplier) {
        stateAndPermits.pause(causeSupplier);
    }

    /**
     * @see #isPoolClosedException(Throwable)
     */
    MongoServerUnavailableException poolClosedException() {
        return new MongoServerUnavailableException(poolClosedMessage);
    }

    /**
     * @see #poolClosedException()
     */
    static boolean isPoolClosedException(final Throwable e) {
        return e instanceof MongoServerUnavailableException;
    }

    /**
     * Package-access methods are thread-safe,
     * and only they should be called outside of the {@link StateAndPermits}'s code.
     */
    @ThreadSafe
    private static final class StateAndPermits {
        private final Supplier<MongoServerUnavailableException> poolClosedExceptionSupplier;
        private final ReadWriteLock lock;
        private final Condition permitAvailableOrClosedOrPausedCondition;
        private volatile boolean paused;
        private volatile boolean closed;
        private final int maxPermits;
        private volatile int permits;
        @Nullable
        private Supplier<MongoException> causeSupplier;

        StateAndPermits(final int maxPermits, final Supplier<MongoServerUnavailableException> poolClosedExceptionSupplier) {
            this.poolClosedExceptionSupplier = poolClosedExceptionSupplier;
            lock = new ReentrantReadWriteLock(true);
            permitAvailableOrClosedOrPausedCondition = lock.writeLock().newCondition();
            paused = false;
            closed = false;
            this.maxPermits = maxPermits;
            permits = maxPermits;
            causeSupplier = null;
        }

        int permits() {
            return permits;
        }

        boolean acquirePermitImmediateUnfair() {
            if (!lock.writeLock().tryLock()) { // unfair
                lock.writeLock().lock();
            }
            try {
                throwIfClosedOrPaused();
                if (permits > 0) {
                    //noinspection NonAtomicOperationOnVolatileField
                    permits--;
                    return true;
                } else {
                    return false;
                }
            } finally {
                lock.writeLock().unlock();
            }
        }

        /**
         * @param timeout See {@link com.mongodb.internal.Timeout#startNow(long, TimeUnit)}.
         */
        boolean acquirePermitFair(final long timeout, final TimeUnit unit) throws MongoInterruptedException {
            long remainingNanos = unit.toNanos(timeout);
            try {
                // preserve the eager InterruptedException behavior of `Semaphore.tryAcquire(long, TimeUnit)`
                lock.writeLock().lockInterruptibly();
            } catch (InterruptedException e) {
                throw new MongoInterruptedException(null, e);
            }
            try {
                while (permits == 0) {
                    throwIfClosedOrPaused();
                    try {
                        if (timeout < 0 || remainingNanos == Long.MAX_VALUE) {
                            permitAvailableOrClosedOrPausedCondition.await();
                        } else if (remainingNanos >= 0) {
                            remainingNanos = permitAvailableOrClosedOrPausedCondition.awaitNanos(remainingNanos);
                        } else {
                            return false;
                        }
                    } catch (InterruptedException e) {
                        throw new MongoInterruptedException(null, e);
                    }
                }
                assertTrue(permits > 0);
                throwIfClosedOrPaused();
                //noinspection NonAtomicOperationOnVolatileField
                permits--;
                return true;
            } finally {
                lock.writeLock().unlock();
            }
        }

        void releasePermit() {
            lock.writeLock().lock();
            try {
                assertTrue(permits < maxPermits);
                //noinspection NonAtomicOperationOnVolatileField
                permits++;
                permitAvailableOrClosedOrPausedCondition.signal();
            } finally {
                lock.writeLock().unlock();
            }
        }

        void pause(final Supplier<MongoException> causeSupplier) {
            lock.writeLock().lock();
            try {
                if (!paused) {
                    this.paused = true;
                    permitAvailableOrClosedOrPausedCondition.signalAll();
                }
                this.causeSupplier = assertNotNull(causeSupplier);
            } finally {
                lock.writeLock().unlock();
            }
        }

        void ready() {
            if (paused) {
                lock.writeLock().lock();
                try {
                    this.paused = false;
                    this.causeSupplier = null;
                } finally {
                    lock.writeLock().unlock();
                }
            }
        }

        /**
         * @return {@code true} if and only if the state changed as a result of the operation.
         */
        boolean close() {
            if (!closed) {
                lock.writeLock().lock();
                try {
                    if (!closed) {
                        closed = true;
                        permitAvailableOrClosedOrPausedCondition.signalAll();
                        return true;
                    }
                } finally {
                    lock.writeLock().unlock();
                }
            }
            return false;
        }

        /**
         * @throws MongoServerUnavailableException If and only if {@linkplain #close() closed}.
         * @throws MongoException If and only if {@linkplain #pause(Supplier) paused}
         * and not {@linkplain #close() closed}. The exception is specified via the {@link #pause(Supplier)} method
         * and may be a subtype of {@link MongoException}.
         */
        void throwIfClosedOrPaused() {
            if (closed) {
                throw poolClosedExceptionSupplier.get();
            }
            if (paused) {
                lock.readLock().lock();
                try {
                    if (paused) {
                        throw assertNotNull(assertNotNull(causeSupplier).get());
                    }
                } finally {
                    lock.readLock().unlock();
                }
            }
        }

        boolean closed() {
            return closed;
        }
    }

    /**
     * @return {@link Integer#toString()} if {@code size} is not {@link #INFINITE_SIZE}, otherwise returns {@code "infinite"}.
     */
    static String sizeToString(final int size) {
        return size == INFINITE_SIZE ? "infinite" : Integer.toString(size);
    }
}
