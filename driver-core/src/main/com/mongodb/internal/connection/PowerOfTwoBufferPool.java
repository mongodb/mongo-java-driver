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
import com.mongodb.internal.connection.ConcurrentPool.Prune;
import com.mongodb.internal.thread.DaemonThreadFactory;
import org.bson.ByteBuf;
import org.bson.ByteBufNIO;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.mongodb.internal.connection.ConcurrentPool.INFINITE_SIZE;

/**
 * Power-of-two buffer pool implementation.
 *
 * <p>This class should not be considered a part of the public API.</p>
 */
public class PowerOfTwoBufferPool implements BufferProvider, AutoCloseable {

    /**
     * The global default pool.  Pruning is enabled on this pool. Idle buffers are pruned after one minute.
     */
    public static final PowerOfTwoBufferPool DEFAULT = new PowerOfTwoBufferPool();

    private static final long DEFAULT_MAX_IDLE_TIME_MINUTES = 1;

    static {
        DEFAULT.enablePruning();
    }

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

    private final class ItemFactory implements ConcurrentPool.ItemFactory<IdleTrackingByteBuffer> {
        private final int size;

        private ItemFactory(final int size) {
            this.size = size;
        }

        @Override
        public IdleTrackingByteBuffer create() {
            return new IdleTrackingByteBuffer(createNew(size));
        }

        @Override
        public void close(final IdleTrackingByteBuffer idleTrackingByteBuffer) {
        }

        @Override
        public Prune shouldPrune(final IdleTrackingByteBuffer idleTrackingByteBuffer) {
            return idleTrackingByteBuffer.getLastUsedNanos() < System.nanoTime() + maxIdleTimeNanos
                    ? Prune.YES : Prune.STOP;
        }
    }

    private final Map<Integer, ConcurrentPool<IdleTrackingByteBuffer>> powerOfTwoToPoolMap
            = new HashMap<Integer, ConcurrentPool<IdleTrackingByteBuffer>>();
    private final long maxIdleTimeNanos;
    private final ScheduledExecutorService pruner;

    /**
     * Construct an instance with a highest power of two of 24.
     */
    public PowerOfTwoBufferPool() {
        this(24);
    }

    /**
     * Construct an instance.
     *
     * @param highestPowerOfTwo the highest power of two buffer size that will be pooled
     */
    public PowerOfTwoBufferPool(final int highestPowerOfTwo) {
        this(highestPowerOfTwo, DEFAULT_MAX_IDLE_TIME_MINUTES, TimeUnit.MINUTES);
    }

    /**
     * Construct an instance.
     *
     * @param highestPowerOfTwo the highest power of two buffer size that will be pooled
     * @param maxIdleTime max idle time when pruning is enabled
     * @param timeUnit time unit of maxIdleTime
     */
    public PowerOfTwoBufferPool(final int highestPowerOfTwo, final long maxIdleTime, final TimeUnit timeUnit) {
        int powerOfTwo = 1;
        for (int i = 0; i <= highestPowerOfTwo; i++) {
            int size = powerOfTwo;
            powerOfTwoToPoolMap.put(i, new ConcurrentPool<>(INFINITE_SIZE, new ItemFactory(size)));
            powerOfTwo = powerOfTwo << 1;
        }
        maxIdleTimeNanos = timeUnit.toNanos(maxIdleTime);
        pruner = Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory("BufferPoolPruner"));
    }

    /**
     * Call this method at most once to enable a background thread that prunes idle buffers from the pool
     */
    public void enablePruning() {
        pruner.scheduleAtFixedRate(this::prune, maxIdleTimeNanos, maxIdleTimeNanos / 2, TimeUnit.NANOSECONDS);
    }

    @Override
    public void close() {
        pruner.shutdownNow();
    }

    @Override
    public ByteBuf getBuffer(final int size) {
        return new PooledByteBufNIO(getByteBuffer(size));
    }

    public ByteBuffer getByteBuffer(final int size) {
        ConcurrentPool<IdleTrackingByteBuffer> pool = powerOfTwoToPoolMap.get(log2(roundUpToNextHighestPowerOfTwo(size)));
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
        ConcurrentPool<IdleTrackingByteBuffer> pool =
                powerOfTwoToPoolMap.get(log2(roundUpToNextHighestPowerOfTwo(buffer.capacity())));
        if (pool != null) {
            pool.release(new IdleTrackingByteBuffer(buffer));
        }
    }

    private void prune() {
        powerOfTwoToPoolMap.values().forEach(ConcurrentPool::prune);
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
}
