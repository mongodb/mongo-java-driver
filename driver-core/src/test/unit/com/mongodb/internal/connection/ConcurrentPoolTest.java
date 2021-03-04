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

import com.mongodb.Function;
import com.mongodb.MongoException;
import com.mongodb.MongoTimeoutException;
import org.junit.Test;

import java.io.Closeable;
import java.util.Optional;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConcurrentPoolTest {
    private ConcurrentPool<TestCloseable> pool;

    @Test
    public void testThatGetDecreasesAvailability() {
        pool = new ConcurrentPool<TestCloseable>(3, new TestItemFactory());

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
    public void testThatReleaseIncreasesAvailability() {
        pool = new ConcurrentPool<TestCloseable>(3, new TestItemFactory());

        pool.get();
        pool.get();
        pool.release(pool.get());
        assertNotNull(pool.get());
    }

    @Test
    public void testThatGetReleasesPermitIfCreateFails() {
        pool = new ConcurrentPool<TestCloseable>(1, new TestItemFactory(true));

        try {
            pool.get();
            fail();
        } catch (MongoException e) {
            // expected
        }

        assertTrue(pool.acquirePermit(-1, MILLISECONDS));
    }

    @Test
    public void testInUseCount() {
        pool = new ConcurrentPool<TestCloseable>(3, new TestItemFactory());

        assertEquals(0, pool.getInUseCount());
        TestCloseable closeable = pool.get();
        assertEquals(1, pool.getInUseCount());
        pool.release(closeable);
        assertEquals(0, pool.getInUseCount());
    }

    @Test
    public void testAvailableCount() {
        pool = new ConcurrentPool<TestCloseable>(3, new TestItemFactory());

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
        pool = new ConcurrentPool<TestCloseable>(3, new TestItemFactory());

        TestCloseable closeable = pool.get();
        pool.release(closeable, false);
        assertFalse(closeable.isClosed());
    }

    @Test
    public void testCloseItemOnReleaseWithDiscard() {
        pool = new ConcurrentPool<TestCloseable>(3, new TestItemFactory());

        TestCloseable closeable = pool.get();
        pool.release(closeable, true);
        assertTrue(closeable.isClosed());
    }

    @Test
    public void testCloseAllItemsAfterPoolClosed() {
        pool = new ConcurrentPool<TestCloseable>(3, new TestItemFactory());

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
        pool = new ConcurrentPool<TestCloseable>(3, new TestItemFactory());

        TestCloseable c1 = pool.get();
        pool.close();
        pool.release(c1);
        assertTrue(c1.isClosed());
    }

    @Test
    public void testEnsureMinSize() {
        pool = new ConcurrentPool<TestCloseable>(3, new TestItemFactory());

        pool.ensureMinSize(0, null);
        assertEquals(0, pool.getAvailableCount());

        pool.ensureMinSize(1, null);
        assertEquals(1, pool.getAvailableCount());

        pool.ensureMinSize(1, null);
        assertEquals(1, pool.getAvailableCount());

        pool.get();
        pool.ensureMinSize(1, null);
        assertEquals(0, pool.getAvailableCount());

        pool.ensureMinSize(4, null);
        assertEquals(3, pool.getAvailableCount());
    }

    @Test
    public void whenEnsuringMinSizeShouldNotInitializePooledItemIfNotRequested() {
        pool = new ConcurrentPool<TestCloseable>(3, new TestItemFactory());

        pool.ensureMinSize(1, null);
        assertFalse(pool.get().isInitialized());
    }

    @Test
    public void whenEnsuringMinSizeShouldInitializePooledItemIfRequested() {
        pool = new ConcurrentPool<TestCloseable>(3, new TestItemFactory());

        pool.ensureMinSize(1, TestCloseable.initializeAction);
        assertTrue(pool.get().isInitialized());
    }

    @Test
    public void testThatEnsuringMinSizeReleasesPermitIfCreateFails() {
        pool = new ConcurrentPool<TestCloseable>(1, new TestItemFactory(true));

        try {
            pool.ensureMinSize(1, TestCloseable.initializeAction);
            fail();
        } catch (MongoException e) {
            // expected
        }

        assertTrue(pool.acquirePermit(-1, MILLISECONDS));
    }

    @Test
    public void testPrune() {
        pool = new ConcurrentPool<TestCloseable>(5, new TestItemFactory());

        TestCloseable t1 = pool.get();
        TestCloseable t2 = pool.get();
        TestCloseable t3 = pool.get();
        TestCloseable t4 = pool.get();
        TestCloseable t5 = pool.get();
        t1.shouldPrune = ConcurrentPool.Prune.YES;
        t2.shouldPrune = ConcurrentPool.Prune.NO;
        t3.shouldPrune = ConcurrentPool.Prune.YES;
        t4.shouldPrune = ConcurrentPool.Prune.STOP;
        t5.shouldPrune = null;

        pool.release(t1);
        pool.release(t2);
        pool.release(t3);
        pool.release(t4);
        pool.release(t5);

        pool.prune();

        assertEquals(3, pool.getAvailableCount());
        assertEquals(0, pool.getInUseCount());
        assertTrue(t1.isClosed());
        assertTrue(!t2.isClosed());
        assertTrue(t3.isClosed());
        assertTrue(!t4.isClosed());
        assertTrue(!t5.isClosed());
    }

    class TestItemFactory implements ConcurrentPool.ItemFactory<TestCloseable> {
        private final boolean shouldThrowOnCreate;

        TestItemFactory() {
            this(false);
        }

        TestItemFactory(final boolean shouldThrowOnCreate) {
            this.shouldThrowOnCreate = shouldThrowOnCreate;
        }

        @Override
        public TestCloseable create() {
            if (shouldThrowOnCreate) {
                throw new MongoException("This is a journey");
            }
            return new TestCloseable();
        }

        @Override
        public void close(final TestCloseable closeable) {
            closeable.close();
        }

        @Override
        public ConcurrentPool.Prune shouldPrune(final TestCloseable testCloseable) {
            return testCloseable.shouldPrune();
        }
    }

    static class TestCloseable implements Closeable {
        private static final Function<TestCloseable, Optional<TestCloseable>> initializeAction = connection -> {
            connection.initialized = true;
            return Optional.of(connection);
        };

        private boolean closed;
        private ConcurrentPool.Prune shouldPrune;
        private boolean initialized;

        TestCloseable() {
        }

        @Override
        public void close() {
            closed = true;
        }

        boolean isClosed() {
            return closed;
        }

        public boolean isInitialized() {
            return initialized;
        }

        public ConcurrentPool.Prune shouldPrune() {
            return shouldPrune;
        }
    }
}
