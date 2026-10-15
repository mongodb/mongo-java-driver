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

import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;
import com.mongodb.internal.thread.DaemonThreadFactory;
import org.bson.ByteBuf;
import org.bson.ByteBufNIO;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class PowerOfTwoBufferPool implements BufferProvider {
    private static final Logger LOGGER = Loggers.getLogger("connection");

    /**
     * The global default pool.  Pruning is enabled on this pool. Idle buffers are pruned after one minute.
     *
     * <p>The pruner thread does not run all the time. It starts when the pool holds a buffer. It stops when the pool
     * becomes empty.</p>
     *
     * <p>The pruner thread must stop. A thread that runs forever keeps the class loader of all driver classes in
     * memory. The static data of those classes also stays in memory. Then an application server cannot unload the
     * application. See <a href="https://jira.mongodb.org/browse/JAVA-6279">JAVA-6279</a>.</p>
     */
    public static final PowerOfTwoBufferPool DEFAULT = new PowerOfTwoBufferPool().enablePruning();

    private static final class IdleTrackingByteBuffer {
        private final long lastUsedNanos;
        private final ByteBuffer buffer;

        private IdleTrackingByteBuffer(final ByteBuffer buffer) {
            this.lastUsedNanos = System.nanoTime();
            this.buffer = buffer;
        }

        public long getLastUsedNanos() {
            return lastUsedNanos;
        }

        public ByteBuffer getBuffer() {
            return buffer;
        }
    }

    private final Map<Integer, BufferPool> powerOfTwoToPoolMap = new HashMap<>();
    private final long maxIdleTimeNanos;
    private final ScheduledThreadPoolExecutor pruner;
    /**
     * True if the pruner has a scheduled prune. Two threads must not schedule a prune at the same time, and this flag
     * prevents that. The method {@link #pruneAndRescheduleIfNeeded()} also uses this flag when it stops the pruner.
     */
    private final AtomicBoolean pruningScheduled = new AtomicBoolean();
    private volatile boolean pruningEnabled;

    /**
     * Construct an instance with a highest power of two of 24.
     */
    PowerOfTwoBufferPool() {
        this(24);
    }

    /**
     * Construct an instance.
     *
     * @param highestPowerOfTwo the highest power of two buffer size that will be pooled
     */
    PowerOfTwoBufferPool(final int highestPowerOfTwo) {
        this(highestPowerOfTwo, 1, TimeUnit.MINUTES);
    }

    /**
     * Construct an instance.
     *
     * @param highestPowerOfTwo the highest power of two buffer size that will be pooled
     * @param maxIdleTime max idle time when pruning is enabled
     * @param timeUnit time unit of maxIdleTime
     */
    PowerOfTwoBufferPool(final int highestPowerOfTwo, final long maxIdleTime, final TimeUnit timeUnit) {
        int powerOfTwo = 1;
        for (int i = 0; i <= highestPowerOfTwo; i++) {
            int size = powerOfTwo;
            powerOfTwoToPoolMap.put(i, new BufferPool(size));
            powerOfTwo = powerOfTwo << 1;
        }
        maxIdleTimeNanos = timeUnit.toNanos(maxIdleTime);
        pruner = new ScheduledThreadPoolExecutor(1, new DaemonThreadFactory("BufferPoolPruner"));
        // The worker thread must stop when it has no more work. Then an idle pool holds no thread.
        //
        // These three settings are sufficient only because this class schedules one prune at a time. It schedules the
        // next prune only if the pool is not empty. Then the work queue becomes empty and the keep-alive time expires.
        // A periodic task stays in the work queue forever. Then the worker thread always has a task to wait for, and
        // the keep-alive time never expires.
        //
        // The keep-alive time applies only after the last prune. While a prune is in the work queue, the worker thread
        // waits for that prune. Because of this, a short keep-alive time does not change the interval between prunes.
        // A short keep-alive time also decreases the time that an idle pool keeps our class loader in memory.
        pruner.setKeepAliveTime(Math.max(1, maxIdleTimeNanos / 2), TimeUnit.NANOSECONDS);
        pruner.allowCoreThreadTimeOut(true);
        pruner.setRemoveOnCancelPolicy(true);
    }

    /**
     * Call this method one time only. It permits the pool to prune idle buffers.
     *
     * <p>This method does not start a thread. An empty pool has no buffers to prune. The pruner starts when you
     * {@linkplain #release(ByteBuffer) release} a buffer. The pruner stops when the pool becomes empty.</p>
     */
    PowerOfTwoBufferPool enablePruning() {
        pruningEnabled = true;
        if (!allPoolsEmpty()) {
            // The pool can hold buffers from before this call, and those buffers also need a prune. An empty pool
            // must not start a thread.
            startPruningIfNeeded();
        }
        return this;
    }

    void disablePruning() {
        pruningEnabled = false;
        pruner.shutdownNow();
    }

    /**
     * @return The number of threads that the pruner uses. This method is package-private because the tests must show
     * that no thread runs when the pool has no buffers to prune. JAVA-6279 is about that behavior.
     */
    int prunerThreadCount() {
        return pruner.getPoolSize();
    }

    @Override
    public ByteBuf getBuffer(final int size) {
        return new PooledByteBufNIO(getByteBuffer(size));
    }

    public ByteBuffer getByteBuffer(final int size) {
        BufferPool pool = powerOfTwoToPoolMap.get(log2(roundUpToNextHighestPowerOfTwo(size)));
        ByteBuffer byteBuffer = (pool == null) ? createNew(size) : pool.get().getBuffer();

        ((Buffer) byteBuffer).clear();
        ((Buffer) byteBuffer).limit(size);
        return byteBuffer;
    }

    private ByteBuffer createNew(final int size) {
        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.order(ByteOrder.LITTLE_ENDIAN);
        return buf;
    }

    public void release(final ByteBuffer buffer) {
        BufferPool pool =
                powerOfTwoToPoolMap.get(log2(roundUpToNextHighestPowerOfTwo(buffer.capacity())));
        if (pool != null) {
            pool.release(new IdleTrackingByteBuffer(buffer));
            startPruningIfNeeded();
        }
    }

    private void startPruningIfNeeded() {
        if (pruningEnabled && pruningScheduled.compareAndSet(false, true)) {
            schedulePrune();
        }
    }

    private void schedulePrune() {
        try {
            pruner.schedule(this::pruneAndRescheduleIfNeeded, maxIdleTimeNanos / 2, TimeUnit.NANOSECONDS);
        } catch (RejectedExecutionException e) {
            // Another thread called `disablePruning` and stopped the executor. A release of a buffer must not fail
            // because of this.
            pruningScheduled.set(false);
        }
    }

    /**
     * Prunes the pool. Then schedules the next prune, but only if the pool is not empty.
     *
     * <p>This method does not cancel a task to stop the pruner. It stops the pruner when it does not schedule the next
     * prune. Then the work queue becomes empty and the pruner thread stops.</p>
     *
     * <p>The steps below prevent a lost pruner. A thread that releases a buffer reads {@link #pruningScheduled}. If
     * that flag is true, the thread does not schedule a prune, because it relies on this method to schedule the next
     * prune. For this reason, this method clears the flag and then examines the pool one more time. If the pool is not
     * empty, this method takes the next prune. If it cannot take the next prune, the other thread has taken it. The
     * class {@code io.netty.util.concurrent.GlobalEventExecutor.TaskRunner} uses the same steps.</p>
     */
    private void pruneAndRescheduleIfNeeded() {
        prune();
        if (allPoolsEmpty()) {
            pruningScheduled.set(false);
            if (allPoolsEmpty()) {
                return;
            }
            if (!pruningScheduled.compareAndSet(false, true)) {
                return;
            }
        }
        schedulePrune();
    }

    private boolean allPoolsEmpty() {
        for (BufferPool pool : powerOfTwoToPoolMap.values()) {
            if (!pool.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    private void prune() {
        try {
            powerOfTwoToPoolMap.values().forEach(BufferPool::prune);
        } catch (Throwable t) {
            LOGGER.error(this + " stopped pruning idle buffer pools. You may want to recreate the MongoClient", t);
            throw t;
        }
    }

    static int log2(final int powerOfTwo) {
        return 31 - Integer.numberOfLeadingZeros(powerOfTwo);
    }

    static int roundUpToNextHighestPowerOfTwo(final int size) {
        int v = size;
        v--;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v++;
        return v;
    }

    private class PooledByteBufNIO extends ByteBufNIO {

        PooledByteBufNIO(final ByteBuffer buf) {
            super(buf);
        }

        @Override
        public void release() {
            ByteBuffer wrapped = asNIO();
            super.release();
            if (getReferenceCount() == 0) {
                PowerOfTwoBufferPool.this.release(wrapped);
            }
        }
    }

    private final class BufferPool {
        private final int bufferSize;
        private final ConcurrentLinkedDeque<IdleTrackingByteBuffer> available = new ConcurrentLinkedDeque<>();

        BufferPool(final int bufferSize) {
            this.bufferSize = bufferSize;
        }

        IdleTrackingByteBuffer get() {
            IdleTrackingByteBuffer buffer = available.pollLast();
            if (buffer != null) {
                return buffer;
            }
            return new IdleTrackingByteBuffer(createNew(bufferSize));
        }

        void release(final IdleTrackingByteBuffer t) {
            available.addLast(t);
        }

        void prune() {
            long now = System.nanoTime();
            available.removeIf(cur -> now - cur.getLastUsedNanos() >= maxIdleTimeNanos);
        }

        boolean isEmpty() {
            return available.isEmpty();
        }
    }
}
