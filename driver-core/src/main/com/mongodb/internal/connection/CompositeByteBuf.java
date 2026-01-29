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

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.bson.assertions.Assertions.isTrueArgument;
import static org.bson.assertions.Assertions.notNull;

/**
 * A composite {@link ByteBuf} that provides a unified view over a list of component buffers.
 *
 * <h2>ByteBuf Ownership and Reference Counting</h2>
 * <p>This class manages the lifecycle of its component buffers with the following rules:</p>
 * <ul>
 *   <li><b>Constructor:</b> Takes buffers as input but does NOT take ownership. The buffers are made
 *       read-only via {@link ByteBuf#asReadOnly()}, which creates read only duplicates.
 *       The original buffers remain owned by the caller.</li>
 *   <li><b>{@link #duplicate()}:</b> Creates a new composite with independent position/limit but calls
 *       {@link ByteBuf#retain()} on each component, incrementing their reference counts. The duplicate
 *       owns these retained references and releases them when it is released.</li>
 *   <li><b>{@link #retain()}:</b> Increments the composite's reference count AND retains all component
 *       buffers. Each retain() call must be paired with a {@link #release()}.</li>
 *   <li><b>{@link #release()}:</b> Decrements the composite's reference count AND releases all component
 *       buffers. When the count reaches 0, subsequent access will throw {@link IllegalStateException}.</li>
 * </ul>
 *
 * <p><strong>Important:</strong> The composite's reference count is independent from its components'
 * reference counts, but they are kept in sync via {@link #retain()} and {@link #release()} methods.</p>
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
final class CompositeByteBuf implements ByteBuf {
    private final List<Component> components;
    private final AtomicInteger referenceCount = new AtomicInteger(1);
    private int position;
    private int limit;

    /**
     * Creates a composite buffer from the given list of buffers.
     *
     * <p><strong>ByteBuf Ownership:</strong> This constructor does NOT take ownership of the input buffers.
     * It calls {@link ByteBuf#asReadOnly()} on each buffer, which creates a shallow read-only view that
     * shares the same underlying data and reference count as the original. The caller retains ownership
     * of the original buffers and is responsible for their lifecycle.</p>
     *
     * <p>The composite starts with a reference count of 1. When {@link #release()} is called and the
     * reference count reaches 0, it does NOT automatically release the original buffers - the caller
     * must handle that separately.</p>
     *
     * @param buffers the list of buffers to compose, must not be null or empty
     */
    CompositeByteBuf(final List<ByteBuf> buffers) {
        notNull("buffers", buffers);
        isTrueArgument("buffer list not empty", !buffers.isEmpty());
        components = new ArrayList<>(buffers.size());

        int offset = 0;
        for (ByteBuf cur : buffers) {
            Component component = new Component(cur.asReadOnly().order(ByteOrder.LITTLE_ENDIAN), offset);
            components.add(component);
            offset = component.endOffset;
        }
        limit = components.get(components.size() - 1).endOffset;
    }

    private CompositeByteBuf(final CompositeByteBuf from) {
        components = from.components.stream().map(c ->
            new Component(c.buffer.retain(), c.offset))
            .collect(Collectors.toList());
        position = from.position();
        limit = from.limit();
    }

    @Override
    public ByteBuf order(final ByteOrder byteOrder) {
        if (byteOrder == ByteOrder.BIG_ENDIAN) {
            throw new UnsupportedOperationException(format("Only %s is supported", ByteOrder.LITTLE_ENDIAN));
        }
        return this;
    }

    @Override
    public int capacity() {
        return components.get(components.size() - 1).endOffset;
    }

    @Override
    public int remaining() {
        return limit() - position();
    }

    @Override
    public boolean hasRemaining() {
        return remaining() > 0;
    }

    @Override
    public int position() {
        return position;
    }

    @Override
    public ByteBuf position(final int newPosition) {
        if (newPosition < 0 || newPosition > limit) {
            throw new IndexOutOfBoundsException(format("%d is out of bounds", newPosition));
        }
        position = newPosition;
        return this;
    }

    @Override
    public ByteBuf clear() {
        position = 0;
        limit = capacity();
        return this;
    }

    @Override
    public int limit() {
        return limit;
    }

    @Override
    public byte get() {
        checkIndex(position);
        position += 1;
        return get(position - 1);
    }

    @Override
    public byte get(final int index) {
        checkIndex(index);
        Component component = findComponent(index);
        return component.buffer.get(index - component.offset);
    }

    @Override
    public ByteBuf get(final byte[] bytes) {
        checkIndex(position, bytes.length);
        position += bytes.length;
        return get(position - bytes.length, bytes);
    }

    @Override
    public ByteBuf get(final int index, final byte[] bytes) {
        return get(index, bytes, 0, bytes.length);
    }

    @Override
    public ByteBuf get(final byte[] bytes, final int offset, final int length) {
        checkIndex(position, length);
        position += length;
        return get(position - length, bytes, offset, length);
    }

    @Override
    public ByteBuf get(final int index, final byte[] bytes, final int offset, final int length) {
        checkDstIndex(index, length, offset, bytes.length);

        int i = findComponentIndex(index);
        int curIndex = index;
        int curOffset = offset;
        int curLength = length;
        while (curLength > 0) {
            Component c = components.get(i);
            int localLength = Math.min(curLength, c.buffer.capacity() - (curIndex - c.offset));
            c.buffer.get(curIndex - c.offset, bytes, curOffset, localLength);
            curIndex += localLength;
            curOffset += localLength;
            curLength -= localLength;
            i++;
        }

        return this;
    }

    @Override
    public long getLong() {
        position += 8;
        return getLong(position - 8);
    }

    @Override
    public long getLong(final int index) {
        checkIndex(index, 8);
        Component component = findComponent(index);
        if (index + 8 <= component.endOffset) {
            return component.buffer.getLong(index - component.offset);
        } else {
            return getInt(index) & 0xFFFFFFFFL | (getInt(index + 4) & 0xFFFFFFFFL) << 32;
        }
    }

    @Override
    public double getDouble() {
        position += 8;
        return getDouble(position - 8);
    }

    @Override
    public double getDouble(final int index) {
        return Double.longBitsToDouble(getLong(index));
    }

    @Override
    public int getInt() {
        position += 4;
        return getInt(position - 4);
    }

    @Override
    public int getInt(final int index) {
        checkIndex(index, 4);
        Component component = findComponent(index);
        if (index + 4 <= component.endOffset) {
            return component.buffer.getInt(index - component.offset);
        } else {
            return getShort(index) & 0xFFFF | (getShort(index + 2) & 0xFFFF) << 16;
        }
    }

    private int getShort(final int index) {
        checkIndex(index, 2);
        return (short) (get(index) & 0xff | (get(index + 1) & 0xff) << 8);
    }

    @Override
    public byte[] array() {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @Override
    public boolean isBackedByArray() {
        return false;
    }

    @Override
    public int arrayOffset() {
        throw new UnsupportedOperationException("Not implemented yet!");
    }

    @Override
    public ByteBuf limit(final int newLimit) {
        if (newLimit < 0 || newLimit > capacity()) {
            throw new IndexOutOfBoundsException(format("%d is out of bounds", newLimit));
        }
        this.limit = newLimit;
        return this;
    }

    @Override
    public ByteBuf put(final int index, final byte b) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf put(final byte[] src, final int offset, final int length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf put(final byte b) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf putInt(final int b) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf putInt(final int index, final int b) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf putDouble(final double b) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf putLong(final long b) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf flip() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuf asReadOnly() {
        throw new UnsupportedOperationException();
    }

    /**
     * Creates a duplicate of this composite buffer with independent position and limit.
     *
     * <p><strong>ByteBuf Ownership:</strong> The duplicate calls {@link ByteBuf#retain()} on each
     * component buffer, incrementing their reference counts. The duplicate owns these retained
     * references and starts with its own reference count of 1. <strong>The caller is responsible
     * for releasing the duplicate</strong> when done, which will release the component buffers.</p>
     *
     * <p>The duplicate shares the underlying buffer data with this composite but has independent
     * reference counting and position/limit state.</p>
     *
     * @return a new composite buffer that shares data with this one but has independent state
     */
    @Override
    public ByteBuf duplicate() {
        return new CompositeByteBuf(this);
    }

    @Override
    public ByteBuffer asNIO() {
        if (components.size() == 1) {
            ByteBuffer byteBuffer = components.get(0).buffer.asNIO().duplicate();
            ((Buffer) byteBuffer).position(position).limit(limit);
            return byteBuffer;
        } else {
           byte[] bytes = new byte[remaining()];
           get(position, bytes, 0, bytes.length);
           return ByteBuffer.wrap(bytes);
        }
    }

    @Override
    public int getReferenceCount() {
        return referenceCount.get();
    }

    /**
     * Increments the reference count of this composite buffer and all component buffers.
     *
     * <p><strong>Important:</strong> This method retains both the composite's reference count and all
     * component buffers. If the reference count is already 0, an {@link IllegalStateException} is thrown.
     * Note that if an exception is thrown, the component buffers will have been retained before the
     * exception occurs.</p>
     *
     * @return this buffer
     * @throws IllegalStateException if the reference count is already 0
     */
    @Override
    public ByteBuf retain() {
        if (referenceCount.incrementAndGet() == 1) {
            referenceCount.decrementAndGet();
            throw new IllegalStateException("Attempted to increment the reference count when it is already 0");
        }
        components.forEach(c -> c.buffer.retain());
        return this;
    }

    /**
     * Decrements the reference count of this composite buffer and all component buffers.
     *
     * <p><strong>Important:</strong> This method releases both the composite's reference count and all
     * component buffers. All component buffers are released even if an exception occurs.</p>
     *
     * @throws IllegalStateException if the reference count is already 0
     */
    @Override
    public void release() {
        if (referenceCount.decrementAndGet() < 0) {
            referenceCount.incrementAndGet();
            throw new IllegalStateException("Attempted to decrement the reference count below 0");
        }
        components.forEach(c -> c.buffer.release());
    }

    private Component findComponent(final int index) {
        return components.get(findComponentIndex(index));
    }

    private int findComponentIndex(final int index) {
        for (int i = components.size() - 1; i >= 0; i--) {
            Component cur = components.get(i);
            if (index >= cur.offset) {
                return i;
            }
        }
        throw new IndexOutOfBoundsException(format("%d is out of bounds", index));
    }

    private void checkIndex(final int index) {
        ensureAccessible();
        if (index < 0 || index >= capacity()) {
            throw new IndexOutOfBoundsException(format("index: %d (expected: range(0, %d))", index, capacity()));
        }
    }

    private void checkIndex(final int index, final int fieldLength) {
        ensureAccessible();
        if (index < 0 || index > capacity() - fieldLength) {
            throw new IndexOutOfBoundsException(format("index: %d, length: %d (expected: range(0, %d))", index, fieldLength, capacity()));
        }
    }

    private void checkDstIndex(final int index, final int length, final int dstIndex, final int dstCapacity) {
        checkIndex(index, length);
        if (dstIndex < 0 || dstIndex > dstCapacity - length) {
            throw new IndexOutOfBoundsException(format("dstIndex: %d, length: %d (expected: range(0, %d))", dstIndex, length, dstCapacity));
        }
    }

    private void ensureAccessible() {
        if (referenceCount.get() == 0) {
            throw new IllegalStateException("Reference count is 0");
        }
    }

    private static final class Component {
        private final ByteBuf buffer;
        private final int length;
        private final int offset;
        private final int endOffset;

        Component(final ByteBuf buffer, final int offset) {
            this.buffer = buffer;
            length = buffer.limit() - buffer.position();
            this.offset = offset;
            this.endOffset = offset + length;
        }
    }
}
