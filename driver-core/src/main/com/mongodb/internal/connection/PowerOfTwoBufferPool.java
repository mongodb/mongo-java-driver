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

package com.mongodb.internal.connection;

import com.mongodb.connection.BufferProvider;
import com.mongodb.internal.connection.ConcurrentPool.Prune;
import org.bson.ByteBuf;
import org.bson.ByteBufNIO;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;

/**
 * Power-of-two buffer pool implementation.
 *
 * <p>This class should not be considered a part of the public API.</p>
 */
public class PowerOfTwoBufferPool implements BufferProvider {

    private final Map<Integer, ConcurrentPool<ByteBuffer>> powerOfTwoToPoolMap = new HashMap<Integer, ConcurrentPool<ByteBuffer>>();

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
        int powerOfTwo = 1;
        for (int i = 0; i <= highestPowerOfTwo; i++) {
            final int size = powerOfTwo;
            powerOfTwoToPoolMap.put(i, new ConcurrentPool<ByteBuffer>(Integer.MAX_VALUE,
                                                                         new ConcurrentPool.ItemFactory<ByteBuffer>() {
                                                                             @Override
                                                                             public ByteBuffer create(final boolean initialize) {
                                                                                 return createNew(size);
                                                                             }

                                                                             @Override
                                                                             public void close(final ByteBuffer byteBuffer) {
                                                                             }

                                                                             @Override
                                                                             public Prune shouldPrune(final ByteBuffer byteBuffer) {
                                                                                 return Prune.STOP;
                                                                             }
                                                                         }));
            powerOfTwo = powerOfTwo << 1;
        }
    }

    @Override
    public ByteBuf getBuffer(final int size) {
        ConcurrentPool<ByteBuffer> pool = powerOfTwoToPoolMap.get(log2(roundUpToNextHighestPowerOfTwo(size)));
        ByteBuffer byteBuffer = (pool == null) ? createNew(size) : pool.get();

        ((Buffer) byteBuffer).clear();
        ((Buffer) byteBuffer).limit(size);
        return new PooledByteBufNIO(byteBuffer);
    }

    private ByteBuffer createNew(final int size) {
        ByteBuffer buf = ByteBuffer.allocate(size);  // TODO: configure whether this uses allocateDirect or allocate
        buf.order(ByteOrder.LITTLE_ENDIAN);
        return buf;
    }

    private void release(final ByteBuffer buffer) {
        ConcurrentPool<ByteBuffer> pool = powerOfTwoToPoolMap.get(log2(roundUpToNextHighestPowerOfTwo(buffer.capacity())));
        if (pool != null) {
            pool.release(buffer);
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
}
