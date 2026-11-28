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

import org.bson.ByteBuf;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class PowerOfTwoBufferPoolTest {
    private PowerOfTwoBufferPool pool;

    @Before
    public void setUp() {
        pool = new PowerOfTwoBufferPool(10);
    }

    @Test
    public void testNormalRequest() {

        for (int i = 0; i <= 10; i++) {
            ByteBuf buf = pool.getBuffer((int) Math.pow(2, i));
            assertEquals((int) Math.pow(2, i), buf.capacity());
            assertEquals((int) Math.pow(2, i), buf.limit());

            if (i > 1) {
                buf = pool.getBuffer((int) Math.pow(2, i) - 1);
                assertEquals((int) Math.pow(2, i), buf.capacity());
                assertEquals((int) Math.pow(2, i) - 1, buf.limit());
            }

            if (i < 10) {
                buf = pool.getBuffer((int) Math.pow(2, i) + 1);
                assertEquals((int) Math.pow(2, i + 1), buf.capacity());
                assertEquals((int) Math.pow(2, i) + 1, buf.limit());
            }
        }
    }

    @Test
    public void testReuse() {
        ByteBuf buf = pool.getBuffer((int) Math.pow(2, 10));
        ByteBuffer byteBuffer = buf.asNIO();
        buf.release();
        assertSame(byteBuffer, pool.getBuffer((int) Math.pow(2, 10)).asNIO());
    }

    @Test
    public void testHugeBufferRequest() {
        ByteBuf buf = pool.getBuffer((int) Math.pow(2, 10) + 1);
        assertEquals((int) Math.pow(2, 10) + 1, buf.capacity());
        assertEquals((int) Math.pow(2, 10) + 1, buf.limit());

        buf.release();
        assertNotSame(buf, pool.getBuffer((int) Math.pow(2, 10) + 1));
    }

    @Test
    public void testPruning() throws InterruptedException {
        PowerOfTwoBufferPool pool = new PowerOfTwoBufferPool(10, 5, TimeUnit.MILLISECONDS)
                .enablePruning();
        try {
            ByteBuf byteBuf = pool.getBuffer(256);
            ByteBuffer wrappedByteBuf = byteBuf.asNIO();
            byteBuf.release();
            // The pruner stops only after it empties the pool. Therefore a thread count of zero shows that the pruner
            // removed the buffer. A wait for a fixed period would make this test racy.
            assertTrue("the pruner must empty the pool", await(() -> pool.prunerThreadCount() == 0));
            ByteBuf newByteBuf = pool.getBuffer(256);
            assertNotSame(wrappedByteBuf, newByteBuf.asNIO());
        } finally {
            pool.disablePruning();
        }
    }

    /**
     * The pruner removes idle buffers, and an empty pool has no idle buffers. Therefore {@code enablePruning} must not
     * start a thread. A thread that runs keeps the class loader of all driver classes in memory. See JAVA-6279.
     */
    @Test
    public void testEnablePruningStartsNoThreadWhileThePoolIsEmpty() {
        PowerOfTwoBufferPool pool = new PowerOfTwoBufferPool(10, 5, TimeUnit.MILLISECONDS).enablePruning();
        try {
            assertEquals(0, pool.prunerThreadCount());
        } finally {
            pool.disablePruning();
        }
    }

    /**
     * The pruner empties the pool. Then it has no more work, and the thread must stop. The thread must not continue to
     * wake up. This behavior is the correction for JAVA-6279.
     */
    @Test
    public void testPrunerThreadTerminatesOnceThePoolIsDrained() throws InterruptedException {
        PowerOfTwoBufferPool pool = new PowerOfTwoBufferPool(10, 5, TimeUnit.MILLISECONDS).enablePruning();
        try {
            pool.getBuffer(256).release();
            assertTrue("the pruner thread should terminate once the pool is drained",
                    await(() -> pool.prunerThreadCount() == 0));
        } finally {
            pool.disablePruning();
        }
    }

    /**
     * The pruner must start again. A pool can become idle and then busy. If the pruner does not start again, the pool
     * keeps the buffers that you release after the idle period.
     */
    @Test
    public void testPruningResumesAfterTheThreadHasTerminated() throws InterruptedException {
        PowerOfTwoBufferPool pool = new PowerOfTwoBufferPool(10, 5, TimeUnit.MILLISECONDS).enablePruning();
        try {
            pool.getBuffer(256).release();
            assertTrue("precondition: the pruner thread terminates once drained",
                    await(() -> pool.prunerThreadCount() == 0));

            ByteBuf byteBuf = pool.getBuffer(256);
            ByteBuffer wrapped = byteBuf.asNIO();
            byteBuf.release();
            assertTrue("a buffer released after termination should still be pruned",
                    await(() -> pool.getBuffer(256).asNIO() != wrapped));
        } finally {
            pool.disablePruning();
        }
    }

    /** A pool without pruning must not start a pruner thread. The number of buffers does not change this behavior. */
    @Test
    public void testPruningDisabledPoolNeverStartsAThread() {
        ByteBuf byteBuf = pool.getBuffer(256);
        ByteBuffer wrapped = byteBuf.asNIO();
        byteBuf.release();
        // This assertion needs no wait. The executor creates its worker thread when it accepts a task, and not when it
        // runs that task. Therefore a pool that schedules a prune has a thread before `release` returns.
        assertEquals(0, pool.prunerThreadCount());
        assertSame("the pool must keep the buffer because it does not prune", wrapped, pool.getBuffer(256).asNIO());
    }

    private static boolean await(final BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(5);
        }
        return condition.getAsBoolean();
    }
}
