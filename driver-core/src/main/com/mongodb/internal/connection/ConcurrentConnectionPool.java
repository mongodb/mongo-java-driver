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

import com.mongodb.MongoInternalException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.internal.connection.ConcurrentLinkedDeque.RemovalReportingIterator;

import java.util.Iterator;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * A concurrent pool implementation.
 *
 * <p>This class should not be considered a part of the public API.</p>
 */
public class ConcurrentConnectionPool<T> implements Pool<T> {

    private final int maxSize;

    private final int incrementSize;

    private final String incrementType;


    private final ItemFactory<T> itemFactory;


    private final ConcurrentLinkedDeque<T> available = new ConcurrentLinkedDeque<T>();
    private final Semaphore permits;
    private volatile boolean closed;


    private static final Logger LOGGER = Loggers.getLogger("connection");

    private int powerCount = 1;

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
        T create(boolean initialize);

        void close(T t);

        Prune shouldPrune(T t);
    }

    /**
     * Initializes a new pool of objects.
     *
     * @param maxSize     max to hold to at any given time. if < 0 then no limit
     * @param itemFactory factory used to create and close items in the pool
     */
    public ConcurrentConnectionPool(final int maxSize, final ItemFactory<T> itemFactory, final int incrementSize, final String incrementType) {
        this.maxSize = maxSize;
        this.itemFactory = itemFactory;
        this.incrementSize = incrementSize;
        this.incrementType = incrementType;
        permits = new Semaphore(maxSize, true);
    }

    /**
     * Return an instance of T to the pool.  This method simply calls {@code release(t, false)}
     *
     * @param t item to return to the pool
     */
    @Override
    public void release(final T t) {
        release(t, false);
    }

    /**
     * call done when you are done with an object from the pool if there is room and the object is ok will get added
     *
     * @param t     item to return to the pool
     * @param prune true if the item should be closed, false if it should be put back in the pool
     */
    @Override
    public void release(final T t, final boolean prune) {
        if (t == null) {
            throw new IllegalArgumentException("Can not return a null item to the pool");
        }
        if (closed) {
            close(t);
            return;
        }

        if (prune) {
            close(t);
        } else {
            available.addLast(t);
        }

        releasePermit();
    }

    /**
     * Gets an object from the pool.  This method will block until a permit is available.
     *
     * @return An object from the pool.
     */
    @Override
    public T get() {
        return get(-1, TimeUnit.MILLISECONDS);
    }

    /**
     * Gets an object from the pool - will block if none are available
     *
     * @param timeout  negative - forever 0        - return immediately no matter what positive ms to wait
     * @param timeUnit the time unit of the timeout
     * @return An object from the pool, or null if can't get one in the given waitTime
     * @throws MongoTimeoutException if the timeout has been exceeded
     */
    @Override
    public T get(final long timeout, final TimeUnit timeUnit) {

        if (closed) {
            throw new IllegalStateException("The pool is closed");
        }

        if (!acquirePermit(timeout, timeUnit)) {
            throw new MongoTimeoutException(String.format("Timeout waiting for a pooled item after %d %s", timeout, timeUnit));
        }

        T t = available.pollLast();


        if (t == null) {
//            when pool is empty, create a new item and add it to the pool
//            also create additional items restricted by the maxSize
//            additional items are created by incrementing the powerCount
            t = createNewAndReleasePermitIfFailure(false);

            int incrementBy;
            if (this.incrementType.equals("exponential")) {
                LOGGER.trace("connection pool got exponential increment");
                incrementBy = (int) Math.round(Math.pow(Math.max(this.incrementSize, 2), this.powerCount));
            } else {
                incrementBy = this.incrementSize;
            }

            T t2 = null;
            for (int i = 0; i < incrementBy - 1; i++) {
                LOGGER.trace("Incrementing pool size " + i + " out of " + incrementBy);
                t2 = createNewAndReleasePermitIfFailure(false);
                if (t2 != null) {
                    available.addLast(t2);
                } else {
                    break;
                }
            }
            this.powerCount++;
        }

        return t;
    }

    public void prune() {
        for (RemovalReportingIterator<T> iter = available.iterator(); iter.hasNext(); ) {
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

    public void ensureMinSize(final int minSize, final boolean initialize) {
        while (getCount() < minSize) {
            if (!acquirePermit(10, TimeUnit.MILLISECONDS)) {
                break;
            }
            release(createNewAndReleasePermitIfFailure(initialize));
        }
    }

    private T createNewAndReleasePermitIfFailure(final boolean initialize) {


        try {
            if (maxSize <= 500) {
                LOGGER.info("Concurrentpool > Try > createNewAndReleasePermitIfFailure  : 187");
                LOGGER.info(this.toString());
            }
            T newMember = itemFactory.create(initialize);
            if (newMember == null) {
                throw new MongoInternalException("The factory for the pool created a null item");
            }
            return newMember;
        } catch (RuntimeException e) {
            if (maxSize <= 500) {
                LOGGER.info(" Concurrentpool > Catch > createNewAndReleasePermitIfFailure  : 187");
                LOGGER.info(this.toString());
            }
            permits.release();
            throw e;
        }
    }

    protected boolean acquirePermit(final long timeout, final TimeUnit timeUnit) {
        try {
            if (closed) {
                return false;
            } else if (timeout >= 0) {
                return permits.tryAcquire(timeout, timeUnit);
            } else {
                permits.acquire();
                return true;
            }
        } catch (InterruptedException e) {
            throw new MongoInterruptedException("Interrupted acquiring a permit to retrieve an item from the pool ", e);
        }
    }

    protected void releasePermit() {
        permits.release();
    }

    /**
     * Clears the pool of all objects.
     */
    @Override
    public void close() {
        closed = true;
        Iterator<T> iter = available.iterator();
        while (iter.hasNext()) {
            T t = iter.next();
            close(t);
            iter.remove();
        }
    }

    public int getMaxSize() {
        return maxSize;
    }

    public int getInUseCount() {
        return maxSize - permits.availablePermits();
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
                .append(" maxSize: ").append(maxSize)
                .append(" availableCount ").append(getAvailableCount())
                .append(" inUseCount ").append(getInUseCount())
                .append(" incrementType ").append(incrementType)
                .append(" incrementSize ").append(incrementSize);
        return buf.toString();
    }

    // swallow exceptions from ItemFactory.close()
    private void close(final T t) {
        try {
            itemFactory.close(t);
        } catch (RuntimeException e) {
            // ItemFactory.close() really should not throw
        }
    }
}
