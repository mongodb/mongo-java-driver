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

import com.mongodb.MongoTimeoutException;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.Future;
import java.util.concurrent.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A concurrent pool implementation.
 *
 * <p>This class should not be considered a part of the public API.</p>
 */
public class ConcurrentConnectionPool extends ConcurrentPool<UsageTrackingInternalConnection> {

    private final int incrementSize;
    private final String incrementType;
    private static final Logger LOGGER = Loggers.getLogger("connection");
    private final AtomicInteger powerCount = new AtomicInteger(1);
    final Semaphore growPermits;

    /**
     * Initializes a new pool of objects.
     *
     * @param maxSize       max to hold to at any given time. if < 0 then no limit
     * @param itemFactory   factory used to create and close items in the pool
     * @param incrementSize the size to increment the pool by
     * @param incrementType the type of increment to use
     */
    public ConcurrentConnectionPool(final int maxSize, final ItemFactory<UsageTrackingInternalConnection> itemFactory, final int incrementSize, final String incrementType) {
        super(maxSize, itemFactory);
        this.incrementSize = incrementSize;
        this.incrementType = incrementType;
        this.growPermits = new Semaphore(1);
    }

    private class addConnectionInPool implements Callable<Boolean> {
        private final long timeout;
        private final TimeUnit timeUnit;


        addConnectionInPool(final long timeout, final TimeUnit timeUnit) {
            this.timeout = timeout;
            this.timeUnit = timeUnit;
        }

        int getActualIncSize(int incrementSize, String incrementType) {
            if (incrementType.equals("exponential")) {
                return (int) Math.pow(2, powerCount.getAndIncrement());
            } else if (incrementType.equals("linear")) {
                return incrementSize - 1;
            } else if (incrementType.equals("1.5x")) {
                return getCount() / 2;
            }else if(incrementType.equals("2x")) {
                return getCount();
            }else if (incrementType.equals("2x-1.5x")) {
                return getCount() < 100 ? getCount() : getCount() / 2;
            }else {
                return 0;
            }
        }
        // The call() method is called in order to execute the asynchronous task.
        @Override
        public Boolean call() throws Exception {
            try {
                LOGGER.trace("Acquiring grow permit when powercount = " + powerCount.get());
                if(!growPermits.tryAcquire(0, timeUnit)){
                    throw new MongoTimeoutException("Timeout waiting for grow permit");
                }
                LOGGER.trace("Acquired grow permit");
                int incrementAmount = getActualIncSize(incrementSize, incrementType);

                try {
                    int i = 0;
                    LOGGER.trace("Incrementing pool by " + incrementAmount + " when current count = " + getCount()+ " and where available count = " + getAvailableCount() + " and in use count = " + getInUseCount());
                    while (i < incrementAmount && getCount() < maxSize) {

                        LOGGER.trace("Incrementing pool size " + (i + 1) + " out of " + incrementAmount);

                        if (!acquirePermit(timeout, timeUnit)) {
                            throw new MongoTimeoutException("Timeout waiting for permit");
                        } else {
                            UsageTrackingInternalConnection t2 = itemFactory.create(false);
                            available.addLast(t2);
                            permits.release();

                        }
                        ++i;
                    }
                    if (getCount() < maxSize) {
                        LOGGER.trace("Increasing PowerCount from " + powerCount.get());
                        powerCount.incrementAndGet();
                    }

                } catch (MongoTimeoutException e) {
                    LOGGER.trace("Timeout waiting for permit");
                }
                finally {
                    LOGGER.trace("Releasing grow permit");
                    growPermits.release();
                }
            } catch (final MongoTimeoutException e) {
                LOGGER.trace("Failure to acquire grow permits");
                Thread.currentThread().interrupt();
                return false;

            }

            Thread.currentThread().interrupt();
            return true;
        }
    }


    /**
     * Gets an object from the pool - will block if none are available - also starts a non blocking thread to add more
     * objects to the pool if the pool is not full.
     *
     * @param timeout  negative - forever 0        - return immediately no matter what positive ms to wait
     * @param timeUnit the time unit of the timeout
     * @return An object from the pool, or null if can't get one in the given waitTime
     * @throws MongoTimeoutException if the timeout has been exceeded
     */
    @Override
    public UsageTrackingInternalConnection get(final long timeout, final TimeUnit timeUnit) {

        if (closed) {
            throw new IllegalStateException("The pool is closed");
        }

        if (!acquirePermit(timeout, timeUnit)) {
            throw new MongoTimeoutException(String.format("Timeout waiting for a pooled item after %d %s", timeout, timeUnit));
        }

        UsageTrackingInternalConnection t = available.pollLast();


        if (t == null) {
            t = createNewAndReleasePermitIfFailure(false);

            ExecutorService executorService = Executors.newFixedThreadPool(1);
            Future<Boolean> future = executorService.submit(new addConnectionInPool(timeout, timeUnit));
        }
        return t;
    }

    /**
     * Gives number of more objects that can be generated.
     */
    public int getPotentialCount() {
        return maxSize - getCount();
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("pool: ")
                .append(" maxSize: ").append(maxSize)
                .append(" availableCount ").append(getAvailableCount())
                .append(" inUseCount ").append(getInUseCount())
                .append(" incrementSize ").append(incrementSize)
                .append(" incrementType ").append(incrementType)
                .append(" powerCount ").append(powerCount.get());
        return buf.toString();
    }


}
