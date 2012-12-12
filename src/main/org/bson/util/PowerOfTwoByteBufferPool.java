/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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

package org.bson.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;

public class PowerOfTwoByteBufferPool extends BufferPool<ByteBuffer> {

    final Map<Integer, SimplePool<ByteBuffer>> powerOfTwoToPoolMap = new HashMap<Integer, SimplePool<ByteBuffer>>();

    public PowerOfTwoByteBufferPool(final int highestPowerOfTwo) {
        int x = 1;
        for (int i = 0; i <= highestPowerOfTwo; i++) {
            final int size = x;
            // TODO: Determine max size of each pool.
            powerOfTwoToPoolMap.put(size, new SimplePool<ByteBuffer>() {
                @Override
                protected ByteBuffer createNew() {
                    return PowerOfTwoByteBufferPool.this.createNew(size);
                }
            });
            x = x << 1;
        }
    }

    @Override
    public ByteBuffer get(final int size) {
        final SimplePool<ByteBuffer> simplePool = powerOfTwoToPoolMap.get(roundUpToNextHighestPowerOfTwo(size));
        final ByteBuffer byteBuffer = simplePool.get();
        byteBuffer.clear();
        byteBuffer.limit(size);
        return byteBuffer;
    }

    @Override
    public void done(final ByteBuffer buffer) {
        powerOfTwoToPoolMap.get(roundUpToNextHighestPowerOfTwo(buffer.capacity())).done(buffer);
    }

    @Override
    public ByteBuffer createNew(final int size) {
        final ByteBuffer buf = ByteBuffer.allocateDirect(size);
        buf.order(ByteOrder.LITTLE_ENDIAN);
        return buf;
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
}
