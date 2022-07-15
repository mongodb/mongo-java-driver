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
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CustomGrowableConcurrentPoolTest {

    private CustomGrowableConcurrentPool<ConcurrentPoolTest.TestCloseable> pool;

    @Test
    public void testThatGetDecreasesAvailability() {
        pool = new CustomGrowableConcurrentPool<ConcurrentPoolTest.TestCloseable>(3, new ConcurrentPoolTest.TestItemFactory(), 1, "linear");

        pool.get();
        pool.get();
        pool.get();
        try {
            pool.get(1, MILLISECONDS);
            fail();
        } catch (MongoTimeoutException e) {
            // all good
        }
    }

    @Test
    public void testThatGetDecreasesAvailabilityLinearly() {
        pool = new CustomGrowableConcurrentPool<ConcurrentPoolTest.TestCloseable>(10,
                new ConcurrentPoolTest.TestItemFactory(), 5, "linear");
        pool.ensureMinSize(2, false);
        pool.get();
        pool.get();
        pool.get();

        sleepAndWait(10);
        assertEquals(4, pool.getAvailableCount());
        pool.get();
        assertEquals(3, pool.getAvailableCount());
        pool.get();
        assertEquals(2, pool.getAvailableCount());
        pool.get();
        assertEquals(1, pool.getAvailableCount());
    }

    @Test
    public void testThatGetDecreasesAvailabilityExponentially() {
        pool = new CustomGrowableConcurrentPool<ConcurrentPoolTest.TestCloseable>(10,
                new ConcurrentPoolTest.TestItemFactory(), 1, "exponential");

        pool.ensureMinSize(2, false);
        pool.get();
        pool.get();
        pool.get();
        sleepAndWait(10);
        assertEquals(1, pool.getAvailableCount());
        pool.get();
        assertEquals(0, pool.getAvailableCount());
        pool.get();
        sleepAndWait(10);
        assertEquals(3, pool.getAvailableCount());
        pool.get();
        assertEquals(8, pool.getCount());
    }

    @Test
    public void testThatGetDecreasesAvailability2x() {
        pool = new CustomGrowableConcurrentPool<ConcurrentPoolTest.TestCloseable>(10,
                new ConcurrentPoolTest.TestItemFactory(), 1, "exponential");
        pool.ensureMinSize(2, false);
        pool.get();
        pool.get();
        pool.get();
        sleepAndWait(10);
        assertEquals(1, pool.getAvailableCount());
        pool.get();
        assertEquals(0, pool.getAvailableCount());
        pool.get();
        sleepAndWait(10);
        assertEquals(3, pool.getAvailableCount());
        assertEquals(8, pool.getCount());
    }

    @Test
    public void testThatGetDecreasesAvailability1point5x() {
        pool = new CustomGrowableConcurrentPool<ConcurrentPoolTest.TestCloseable>(10,
                new ConcurrentPoolTest.TestItemFactory(), 1, "1.5x");
        pool.ensureMinSize(2, false);
        pool.get();
        pool.get();
        pool.get();
        sleepAndWait(10);
        assertEquals(0, pool.getAvailableCount());
        pool.get();
        sleepAndWait(10);
        assertEquals(0, pool.getAvailableCount());
        pool.get();
        sleepAndWait(10);
        assertEquals(1, pool.getAvailableCount());
        assertEquals(6, pool.getCount());
        pool.get();
        assertEquals(6, pool.getCount());
    }

    @Test
    public void testDoesNotExceedMax() {
        pool = new CustomGrowableConcurrentPool<ConcurrentPoolTest.TestCloseable>(10,
                new ConcurrentPoolTest.TestItemFactory(), 20, "linear");
        pool.ensureMinSize(2, false);
        pool.get();
        pool.get();
        pool.get();
        sleepAndWait(10);
        assertEquals(10, pool.getCount());

    }

    @Test
    public void testNoSimultaneousGrow() {
        pool = new CustomGrowableConcurrentPool<ConcurrentPoolTest.TestCloseable>(30,
                new ConcurrentPoolTest.TestItemFactory(), 20, "linear");
        pool.ensureMinSize(2, false);
        pool.get();
        pool.get();
        pool.get();
        pool.get();
        sleepAndWait(10);
        assertEquals(23, pool.getCount());
    }

    public void sleepAndWait(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            fail();
        }
        while (pool.growPermitStatus() != 1) {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                fail();
            }
        }
    }

}
