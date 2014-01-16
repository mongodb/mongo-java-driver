/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb;


import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConcurrentPoolTest {
    private ConcurrentPool<TestCloseable> pool;

    static class TestCloseable implements Closeable {
        private boolean closed;
        private boolean shouldPrune;

        @Override
        public void close() {
            closed = true;
        }

        boolean isClosed() {
            return closed;
        }

        public boolean shouldPrune() {
            return shouldPrune;
        }
    }

    @Before
    public void setUp() {
        pool = new ConcurrentPool<TestCloseable>(3, new ConcurrentPool.ItemFactory<TestCloseable>() {
            @Override
            public TestCloseable create() {
                return new TestCloseable();
            }

            @Override
            public void close(final TestCloseable closeable) {
                closeable.close();
            }

            @Override
            public boolean shouldPrune(final TestCloseable testCloseable) {
                return testCloseable.shouldPrune();
            }
        });
    }

    @Test
    public void testThatGetDecreasesAvailability() {
        pool.get();
        pool.get();
        pool.get();
        try {
            pool.get(1, MILLISECONDS);
            fail("should have timed out");
        } catch (MongoTimeoutException e) {
            // all good
        }
    }

    @Test
    public void testThatReleaseIncreasesAvailability() {
        pool.get();
        pool.get();
        pool.release(pool.get());
        assertNotNull(pool.get());
    }

    @Test
    public void testInUseCount() {
        assertEquals(0, pool.getInUseCount());
        TestCloseable closeable = pool.get();
        assertEquals(1, pool.getInUseCount());
        pool.release(closeable);
        assertEquals(0, pool.getInUseCount());
    }

    @Test
    public void testAvailableCount() {
        assertEquals(0, pool.getAvailableCount());
        TestCloseable closeable = pool.get();
        assertEquals(0, pool.getAvailableCount());
        pool.release(closeable);
        assertEquals(1, pool.getAvailableCount());
        closeable = pool.get();
        pool.release(closeable, true);
        assertEquals(0, pool.getAvailableCount());
    }

    @Test
    public void testAddItemToPoolOnRelease() {
        TestCloseable closeable = pool.get();
        pool.release(closeable, false);
        assertFalse(closeable.isClosed());
    }

    @Test
    public void testCloseItemOnReleaseWithDiscard() {
        TestCloseable closeable = pool.get();
        pool.release(closeable, true);
        assertTrue(closeable.isClosed());
    }

    @Test
    public void testCloseAllItemsAfterPoolClosed() {
        TestCloseable c1 = pool.get();
        TestCloseable c2 = pool.get();
        pool.release(c1);
        pool.release(c2);
        pool.close();
        assertTrue(c1.isClosed());
        assertTrue(c2.isClosed());
    }

    @Test
    public void testCloseItemOnReleaseAfterPoolClosed() {
        TestCloseable c1 = pool.get();
        pool.close();
        pool.release(c1);
        assertTrue(c1.isClosed());
    }

    @Test
    public void testEnsureMinSize() {
        pool.ensureMinSize(0);
        assertEquals(0, pool.getAvailableCount());

        pool.ensureMinSize(1);
        assertEquals(1, pool.getAvailableCount());

        pool.ensureMinSize(1);
        assertEquals(1, pool.getAvailableCount());

        pool.get();
        pool.ensureMinSize(1);
        assertEquals(0, pool.getAvailableCount());

        pool.ensureMinSize(4);
        assertEquals(3, pool.getAvailableCount());
    }

    @Test
    public void testPrune() {
        TestCloseable t1 = pool.get();
        TestCloseable t2 = pool.get();
        t1.shouldPrune = true;
        t2.shouldPrune = true;

        pool.release(t1);
        pool.release(t2);

        pool.prune();
        assertEquals(0, pool.getAvailableCount());
        assertEquals(0, pool.getInUseCount());
        assertTrue(t1.isClosed());
        assertTrue(t2.isClosed());
    }
}
