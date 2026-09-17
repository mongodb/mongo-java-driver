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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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

    // Racy test
    @Test
    public void testPruning() throws InterruptedException {
        PowerOfTwoBufferPool pool = new PowerOfTwoBufferPool(10, 5, TimeUnit.MILLISECONDS)
                .enablePruning();
        try {
            ByteBuf byteBuf = pool.getBuffer(256);
            ByteBuffer wrappedByteBuf = byteBuf.asNIO();
            byteBuf.release();
            Thread.sleep(50);
            ByteBuf newByteBuf = pool.getBuffer(256);
            assertNotSame(wrappedByteBuf, newByteBuf.asNIO());
        } finally {
            pool.disablePruning();
        }
    }

    @Test
    public void testDisablePruningStopsPruner() {
        PowerOfTwoBufferPool pool = new PowerOfTwoBufferPool(10, 1, TimeUnit.MINUTES).enablePruning();
        assertTrue(pool.isPruningEnabled());
        pool.disablePruning();
        assertFalse(pool.isPruningEnabled());
        // Idempotent
        pool.disablePruning();
        assertFalse(pool.isPruningEnabled());
    }

    @Test
    public void testEnablePruningAfterDisableRestartsPruner() {
        PowerOfTwoBufferPool pool = new PowerOfTwoBufferPool(10, 1, TimeUnit.MINUTES).enablePruning();
        pool.disablePruning();
        assertFalse(pool.isPruningEnabled());
        pool.enablePruning();
        assertTrue(pool.isPruningEnabled());
        pool.disablePruning();
    }

    @Test
    public void testRetainReleasePruningStopsWhenLastReleased() {
        PowerOfTwoBufferPool pool = new PowerOfTwoBufferPool(10, 1, TimeUnit.MINUTES);
        assertFalse(pool.isPruningEnabled());

        pool.retainPruning();
        assertTrue(pool.isPruningEnabled());
        pool.retainPruning();
        assertTrue(pool.isPruningEnabled());

        pool.releasePruning();
        assertTrue(pool.isPruningEnabled());
        pool.releasePruning();
        assertFalse(pool.isPruningEnabled());
    }

    @Test
    public void testDisablePruningTerminatesPrunerThread() throws InterruptedException {
        // Account for any pre-existing BufferPoolPruner threads (e.g. PowerOfTwoBufferPool.DEFAULT)
        int threadsBefore = countBufferPoolPrunerThreads();
        PowerOfTwoBufferPool pool = new PowerOfTwoBufferPool(10, 1, TimeUnit.MINUTES).enablePruning();
        // Force the scheduled thread to start
        pool.getBuffer(64).release();
        Thread.sleep(50);
        assertTrue(countBufferPoolPrunerThreads() > threadsBefore);

        pool.disablePruning();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (countBufferPoolPrunerThreads() > threadsBefore && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertEquals(threadsBefore, countBufferPoolPrunerThreads());
        assertFalse(pool.isPruningEnabled());
    }

    private static int countBufferPoolPrunerThreads() {
        int count = 0;
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            String name = thread.getName();
            if (name != null && name.startsWith("BufferPoolPruner-") && thread.isAlive()) {
                count++;
            }
        }
        return count;
    }
}
