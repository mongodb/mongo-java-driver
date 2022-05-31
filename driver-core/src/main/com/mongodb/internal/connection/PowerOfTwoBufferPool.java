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

import com.mongodb.connection.BufferProvider;
import com.mongodb.internal.thread.DaemonThreadFactory;
import org.bson.ByteBuf;
import org.bson.ByteBufNIO;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Power-of-two buffer pool implementation.
 *
 * <p>This class should not be considered a part of the public API.</p>
 */
public class PowerOfTwoBufferPool implements BufferProvider {

    /**
     * The global default pool.  Pruning is enabled on this pool. Idle buffers are pruned after one minute.
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
    private final ScheduledExecutorService pruner;

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
        pruner = Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory("BufferPoolPruner"));
    }

    /**
     * Call this method at most once to enable a background thread that prunes idle buffers from the pool
     */
    PowerOfTwoBufferPool enablePruning() {
        pruner.scheduleAtFixedRate(this::prune, maxIdleTimeNanos, maxIdleTimeNanos / 2, TimeUnit.NANOSECONDS);
        return this;
    }

    void disablePruning() {
        pruner.shutdownNow();
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
        }
    }

    private void prune() {
        powerOfTwoToPoolMap.values().forEach(BufferPool::prune);
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
    }
}
