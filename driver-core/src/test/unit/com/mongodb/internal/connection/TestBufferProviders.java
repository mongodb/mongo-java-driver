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

import com.mongodb.internal.connection.netty.NettyByteBuf;
import com.mongodb.lang.NonNull;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.bson.ByteBuf;
import org.bson.ByteBufNIO;
import org.junit.jupiter.params.provider.Arguments;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Shared buffer providers for testing ByteBuf implementations.
 */
public final class TestBufferProviders {
    private TestBufferProviders() {
    }


    public static Stream<Arguments> bufferProviders() {
        TestBufferProviders.TrackingBufferProvider nioBufferProvider = TestBufferProviders.trackingBufferProvider(size -> new ByteBufNIO(ByteBuffer.allocate(size)));
        PowerOfTwoBufferPool bufferPool = new PowerOfTwoBufferPool(1);
        bufferPool.disablePruning();
        TestBufferProviders.TrackingBufferProvider pooledNioBufferProvider = TestBufferProviders.trackingBufferProvider(bufferPool);
        TestBufferProviders.TrackingBufferProvider nettyBufferProvider = TestBufferProviders.trackingNettyBufferProvider();
        return Stream.of(
                Arguments.of("NIO", nioBufferProvider),
                Arguments.of("pooled NIO", pooledNioBufferProvider),
                Arguments.of("Netty", nettyBufferProvider));
    }

    /**
     * Creates a TrackingBufferProvider that tracks allocated buffers for validation.
     */
    public static TrackingBufferProvider trackingNettyBufferProvider() {
        return new TrackingBufferProvider(size -> new NettyByteBuf(UnpooledByteBufAllocator.DEFAULT.buffer(size, size)));
    }

    /**
     * Creates a TrackingBufferProvider that wraps any BufferProvider.
     */
    public static TrackingBufferProvider trackingBufferProvider(final BufferProvider provider) {
        return new TrackingBufferProvider(provider);
    }

    /**
     * Creates a TrackingBufferProvider that wraps a functional provider.
     */
    public static TrackingBufferProvider trackingBufferProvider(final FunctionalBufferProvider provider) {
        return new TrackingBufferProvider(provider);
    }

    /**
     * Functional interface for creating buffers of a given size.
     */
    public interface FunctionalBufferProvider extends BufferProvider {
        @Override
        @NonNull
        ByteBuf getBuffer(int size);
    }

    /**
     * A NettyBufferProvider that validates cleanup and prevents use-after-free.
     */
    private static final class NettyBufferProvider implements BufferProvider {
        private final ByteBufAllocator allocator;

        NettyBufferProvider() {
            allocator = PooledByteBufAllocator.DEFAULT;
        }

        @Override
        public ByteBuf getBuffer(final int size) {
            io.netty.buffer.ByteBuf nettyBuffer = allocator.directBuffer(size, size);
            return new ValidatingNettyByteBuf(new NettyByteBuf(nettyBuffer));
        }

        private static final class ValidatingNettyByteBuf implements ByteBuf {
            private final NettyByteBuf delegate;
            private boolean released = false;

            ValidatingNettyByteBuf(final NettyByteBuf delegate) {
                this.delegate = delegate;
            }

            @Override
            public int capacity() {
                validateNotReleased();
                return delegate.capacity();
            }

            @Override
            public ByteBuf put(final int index, final byte value) {
                validateNotReleased();
                return delegate.put(index, value);
            }

            @Override
            public int remaining() {
                validateNotReleased();
                return delegate.remaining();
            }

            @Override
            public ByteBuf put(final byte[] src, final int offset, final int length) {
                validateNotReleased();
                return delegate.put(src, offset, length);
            }

            @Override
            public boolean hasRemaining() {
                validateNotReleased();
                return delegate.hasRemaining();
            }

            @Override
            public ByteBuf put(final byte value) {
                validateNotReleased();
                return delegate.put(value);
            }

            @Override
            public ByteBuf putInt(final int value) {
                validateNotReleased();
                return delegate.putInt(value);
            }

            @Override
            public ByteBuf putInt(final int index, final int value) {
                validateNotReleased();
                return delegate.putInt(index, value);
            }

            @Override
            public ByteBuf putDouble(final double value) {
                validateNotReleased();
                return delegate.putDouble(value);
            }

            @Override
            public ByteBuf putLong(final long value) {
                validateNotReleased();
                return delegate.putLong(value);
            }

            @Override
            public ByteBuf flip() {
                validateNotReleased();
                return delegate.flip();
            }

            @Override
            public byte[] array() {
                validateNotReleased();
                return delegate.array();
            }

            @Override
            public boolean isBackedByArray() {
                validateNotReleased();
                return delegate.isBackedByArray();
            }

            @Override
            public int arrayOffset() {
                validateNotReleased();
                return delegate.arrayOffset();
            }

            @Override
            public int limit() {
                validateNotReleased();
                return delegate.limit();
            }

            @Override
            public ByteBuf position(final int newPosition) {
                validateNotReleased();
                return delegate.position(newPosition);
            }

            @Override
            public ByteBuf clear() {
                validateNotReleased();
                return delegate.clear();
            }

            @Override
            public ByteBuf order(final java.nio.ByteOrder byteOrder) {
                validateNotReleased();
                return delegate.order(byteOrder);
            }

            @Override
            public byte get() {
                validateNotReleased();
                return delegate.get();
            }

            @Override
            public byte get(final int index) {
                validateNotReleased();
                return delegate.get(index);
            }

            @Override
            public ByteBuf get(final byte[] bytes) {
                validateNotReleased();
                return delegate.get(bytes);
            }

            @Override
            public ByteBuf get(final int index, final byte[] bytes) {
                validateNotReleased();
                return delegate.get(index, bytes);
            }

            @Override
            public ByteBuf get(final byte[] bytes, final int offset, final int length) {
                validateNotReleased();
                return delegate.get(bytes, offset, length);
            }

            @Override
            public ByteBuf get(final int index, final byte[] bytes, final int offset, final int length) {
                validateNotReleased();
                return delegate.get(index, bytes, offset, length);
            }

            @Override
            public long getLong() {
                validateNotReleased();
                return delegate.getLong();
            }

            @Override
            public long getLong(final int index) {
                validateNotReleased();
                return delegate.getLong(index);
            }

            @Override
            public double getDouble() {
                validateNotReleased();
                return delegate.getDouble();
            }

            @Override
            public double getDouble(final int index) {
                validateNotReleased();
                return delegate.getDouble(index);
            }

            @Override
            public int getInt() {
                validateNotReleased();
                return delegate.getInt();
            }

            @Override
            public int getInt(final int index) {
                validateNotReleased();
                return delegate.getInt(index);
            }

            @Override
            public int position() {
                validateNotReleased();
                return delegate.position();
            }

            @Override
            public ByteBuf limit(final int newLimit) {
                validateNotReleased();
                return delegate.limit(newLimit);
            }

            @Override
            public ByteBuf asReadOnly() {
                validateNotReleased();
                return delegate.asReadOnly();
            }

            @Override
            public ByteBuf duplicate() {
                validateNotReleased();
                return delegate.duplicate();
            }

            @Override
            public java.nio.ByteBuffer asNIO() {
                validateNotReleased();
                return delegate.asNIO();
            }

            @Override
            public int getReferenceCount() {
                validateNotReleased();
                return delegate.getReferenceCount();
            }

            @Override
            public ByteBuf retain() {
                validateNotReleased();
                return delegate.retain();
            }

            @Override
            public void release() {
                if (!released) {
                    released = true;
                    delegate.release();
                    assertEquals(0, delegate.getReferenceCount(), "Buffer should have reference count 0 after release");
                }
            }

            private void validateNotReleased() {
                if (released) {
                    throw new IllegalStateException("Buffer has been released");
                }
            }
        }
    }

    /**
     * A BufferProvider that tracks allocated buffers for validation.
     */
    public static final class TrackingBufferProvider implements BufferProvider {
        private final BufferProvider decorated;
        private final List<ByteBuf> tracked;

        public TrackingBufferProvider(final BufferProvider decorated) {
            this.decorated = decorated;
            tracked = new ArrayList<>();
        }

        @NonNull
        @Override
        public ByteBuf getBuffer(final int size) {
            ByteBuf result = decorated.getBuffer(size);
            tracked.add(result);
            return result;
        }

        /**
         * Asserts that all tracked buffers are still available (reference count > 0).
         */
        public void assertAllAvailable() {
            for (ByteBuf buffer : tracked) {
                assertTrue(buffer.getReferenceCount() > 0);
                if (buffer instanceof ByteBufNIO) {
                    assertNotNull(buffer.asNIO());
                } else if (buffer instanceof NettyByteBuf) {
                    assertTrue(((NettyByteBuf) buffer).asByteBuf().refCnt() > 0);
                }
            }
        }

        /**
         * Asserts that all tracked buffers have been released (reference count = 0).
         */
        public void assertAllUnavailable() {
            for (ByteBuf buffer : tracked) {
                assertEquals(0, buffer.getReferenceCount());
                if (buffer instanceof ByteBufNIO) {
                    assertNull(buffer.asNIO());
                }
                if (buffer instanceof NettyByteBuf) {
                    assertEquals(0, ((NettyByteBuf) buffer).asByteBuf().refCnt());
                }
            }
        }
    }
}
