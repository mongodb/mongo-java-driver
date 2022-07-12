package com.mongodb.internal.connection;
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
import java.util.concurrent.Future;
import java.util.concurrent.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
public class ConnectionConcurrentPool extends ConcurrentPool<UsageTrackingInternalConnection> {

    private final int incrementSize;

    private final String incrementType;


    private AtomicInteger powerCount = new AtomicInteger(1);

    private class addConnectionInPool implements Callable<Boolean> {
        private final long timeout;
        private final TimeUnit timeUnit;


        addConnectionInPool(final long timeout, final TimeUnit timeUnit) {
            this.timeout = timeout;
            this.timeUnit = timeUnit;
        }

        // The call() method is called in order to execute the asynchronous task.
        @Override
        public Boolean call() throws Exception {
            int incrementAmount = incrementSize;
            if (incrementType.equals("exponential")) {
                incrementAmount = (int) Math.pow(Math.max(2, incrementSize), powerCount.get());
            }
            try {
                int i = 0;
                while (i < incrementAmount && getCount() < maxSize) {
                    LOGGER.info("ConConPool 186 potentialCount " + getPotentialCount());
//                if (this.getPotentialCount() <= 0) {
//                    this.powerCount--;
//                    break;
//                }
                    LOGGER.info("Incrementing pool size " + i + " out of " + incrementAmount);
//                T t2 = itemFactory.create(true);
//                available.addLast(t2);
//                try to aquire permit
//                if (!acquirePermit(timeout, timeUnit)) {
//                    ConcurrentConnectionPool.powerCount--;
//                    break;
//                } else {
//                    T t2 = createNewAndReleasePermitIfFailure(false);
//                    available.addLast(t2);
//                }
                    if (!acquirePermit(timeout, timeUnit)) {
                        throw new InterruptedException();
                    } else {
                        LOGGER.info("Concurrent Connection pool: 97");
                        try {
                            T t2 = createNewAndReleasePermitIfFailure(false);
                            available.addLast(t2);
                            powerCount.incrementAndGet();
                        } catch (RuntimeException e) {
                            LOGGER.info("Concurrent Connection pool: 100");
                            throw e;
                        }
                    }
                    ++i;
                }

            } catch (InterruptedException e) {
                LOGGER.info("Concurrent Connection pool: 102");
                Thread.currentThread().interrupt();
            }
            return true;
        }
    }

    /**
     * Initializes a new pool of objects.
     *
     * @param maxSize     max to hold to at any given time. if < 0 then no limit
     * @param itemFactory factory used to create and close items in the pool
     */
    public ConnectionConcurrentPool(int maxSize, ItemFactory<UsageTrackingInternalConnection> itemFactory, final int incrementSize, final String incrementType) {
        super(maxSize, itemFactory);
        this.incrementSize = incrementSize;
        this.incrementType = incrementType;
    }

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

//            when pool is empty, create a new item and add it to the pool
//            also create additional items restricted by the maxSize
//            additional items are created by incrementing the powerCount

            t = createNewAndReleasePermitIfFailure(false);
            ExecutorService executorService = Executors.newFixedThreadPool(1);
            Future<Boolean> future = executorService.submit(new ConcurrentConnectionPool.addConnectionInPool(timeout, timeUnit));
            LOGGER.info("ConConPool 197: increase power count from :" + powerCount.get());


        }
        return t;
    }
}

