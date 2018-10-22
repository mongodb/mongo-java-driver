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

package org.bson;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * <p>An interface wrapper around a {@code java.nio.ByteBuffer} which additionally is {@code Closeable}, so that pooled byte buffers know
 * how.</p>
 *
 * <p>This interface is not frozen yet, and methods may be added in a minor release, so beware implementing this yourself.</p>
 *
 * @since 3.0
 */
public interface ByteBuf  {

    /**
     * Returns this buffer's capacity.
     *
     * @return The capacity of this buffer
     */
    int capacity();

    /**
     * <p>Absolute <em>put</em> method <em>(optional operation)</em>.</p>
     *
     * <p>Writes the given byte into this buffer at the given index. </p>
     *
     * @param index The index at which the byte will be written
     * @param b     The byte value to be written
     * @return This buffer
     * @throws IndexOutOfBoundsException        If {@code index} is negative or not smaller than the buffer's limit
     * @throws java.nio.ReadOnlyBufferException If this buffer is read-only
     */
    ByteBuf put(int index, byte b);

    /**
     * Returns the number of elements between the current position and the limit.
     *
     * @return The number of elements remaining in this buffer
     */
    int remaining();

    /**
     * <p>Relative bulk <em>put</em> method&nbsp;&nbsp;<em>(optional operation)</em>. </p>
     *
     * <p>This method transfers bytes into this buffer from the given source array.  If there are more bytes to be copied from the array
     * than remain in this buffer, that is, if {@code length > remaining()}, then no bytes are transferred and a {@link
     * java.nio.BufferOverflowException} is thrown. </p>
     *
     * <p>Otherwise, this method copies {@code length} bytes from the given array into this buffer, starting at the given offset in the
     * array and at the current position of this buffer.  The position of this buffer is then incremented by {@code length}.</p>
     *
     * <p>In other words, an invocation of this method of the form {@code dst.put(src, off, len)} has exactly the same effect as the
     * loop</p>
     * <pre>{@code
     *     for (int i = off; i < off + len; i++) {
     *         dst.put(a[i]);
     *     }}
     * </pre>
     *
     * <p>except that it first checks that there is sufficient space in this buffer and it is potentially much more efficient. </p>
     *
     * @param src    The array from which bytes are to be read
     * @param offset The offset within the array of the first byte to be read; must be non-negative and no larger than {@code array.length}
     * @param length The number of bytes to be read from the given array; must be non-negative and no larger than {@code array.length -
     *               offset}
     * @return This buffer
     * @throws java.nio.BufferOverflowException If there is insufficient space in this buffer
     * @throws IndexOutOfBoundsException        If the preconditions on the {@code offset} and {@code length} parameters do not hold
     * @throws java.nio.ReadOnlyBufferException If this buffer is read-only
     */
    ByteBuf put(byte[] src, int offset, int length);

    /**
     * States whether there are any elements between the current position and the limit.
     *
     * @return {@code true} if, and only if, there is at least one element remaining in this buffer
     */
    boolean hasRemaining();

    /**
     * <p>Relative <em>put</em> method <em>(optional operation)</em>.</p>
     *
     * <p>Writes the given byte into this buffer at the current position, and then increments the position. </p>
     *
     * @param b The byte to be written
     * @return This buffer
     * @throws java.nio.BufferOverflowException If this buffer's current position is not smaller than its limit
     * @throws java.nio.ReadOnlyBufferException If this buffer is read-only
     */
    ByteBuf put(byte b);

    /**
     * <p>Flips this buffer.  The limit is set to the current position and then the position is set to zero.  If the mark is defined then it
     * is discarded.</p>
     *
     * <p>After a sequence of channel-read or <em>put</em> operations, invoke this method to prepare for a sequence of channel-write or
     * relative <em>get</em> operations.  For example: </p>
     * <pre>
     * buf.put(magic);    // Prepend header
     * in.read(buf);      // Read data into rest of buffer
     * buf.flip();        // Flip buffer
     * out.write(buf);    // Write header + data to channel
     * </pre>
     * <p>This method is often used in conjunction with the {@link java.nio.ByteBuffer#compact compact} method when transferring data from
     * one place to another.  </p>
     *
     * @return This buffer
     */
    ByteBuf flip();

    /**
     * <p>Returns the byte array that backs this buffer <em>(optional operation)</em>.</p>
     *
     * <p>Modifications to this buffer's content will cause the returned array's content to be modified, and vice versa.</p>
     *
     * @return The array that backs this buffer
     * @throws java.nio.ReadOnlyBufferException If this buffer is backed by an array but is read-only
     * @throws UnsupportedOperationException    If this buffer is not backed by an accessible array
     */
    byte[] array();

    /**
     * Returns this buffer's limit.
     *
     * @return The limit of this buffer
     */
    int limit();

    /**
     * Sets this buffer's position.  If the mark is defined and larger than the new position then it is discarded.
     *
     * @param newPosition The new position value; must be non-negative and no larger than the current limit
     * @return This buffer
     * @throws IllegalArgumentException If the preconditions on {@code newPosition} do not hold
     */
    ByteBuf position(int newPosition);

    /**
     * <p>Clears this buffer.  The position is set to zero, the limit is set to the capacity, and the mark is discarded.</p>
     *
     * <p>Invoke this method before using a sequence of channel-read or <em>put</em> operations to fill this buffer.  For example: </p>
     *
     * <pre>
     * buf.clear();     // Prepare buffer for reading
     * in.read(buf);    // Read data
     * </pre>
     * <p>This method does not actually erase the data in the buffer, but it is named as if it did because it will most often be used in
     * situations in which that might as well be the case. </p>
     *
     * @return This buffer
     */
    ByteBuf clear();

    /**
     * Modifies this buffer's byte order.
     *
     * @param byteOrder The new byte order, either {@link ByteOrder#BIG_ENDIAN BIG_ENDIAN} or {@link ByteOrder#LITTLE_ENDIAN LITTLE_ENDIAN}
     * @return This buffer
     */
    ByteBuf order(ByteOrder byteOrder);

    /**
     * Relative <em>get</em> method.  Reads the byte at this buffer's current position, and then increments the position.
     *
     * @return The byte at the buffer's current position
     * @throws java.nio.BufferUnderflowException If the buffer's current position is not smaller than its limit
     */
    byte get();

    /**
     * Absolute <em>get</em> method.  Reads the byte at the given index.
     *
     * @param index The index from which the byte will be read
     * @return The byte at the given index
     * @throws IndexOutOfBoundsException If {@code index} is negative or not smaller than the buffer's limit
     */
    byte get(int index);

    /**
     * <p>Relative bulk {@code get} method.</p>
     *
     * <p>This method transfers bytes from this buffer into the given destination array.  An invocation of this method of the form {@code
     * src.get(a)} behaves in exactly the same way as the invocation:</p>
     *
     * <pre>
     * src.get(a, 0, a.length)
     * </pre>
     *
     * @param bytes the destination byte array
     * @return This buffer
     * @throws java.nio.BufferUnderflowException If there are fewer than {@code length} bytes remaining in this buffer
     */
    ByteBuf get(byte[] bytes);

    /**
     * <p>Absolute bulk {@code get} method.</p>
     *
     * <p>This method transfers bytes from this buffer into the given destination array.  An invocation of this method of the form {@code
     * src.get(a)} behaves in exactly the same way as the invocation:</p>
     *
     * <pre>
     * src.get(index, a, 0, a.length)
     * </pre>
     *
     * @param index The index from which the bytes will be read
     * @param bytes the destination byte array
     * @return This buffer
     * @throws java.nio.BufferUnderflowException If there are fewer than {@code length} bytes remaining in this buffer
     */
    ByteBuf get(int index, byte[] bytes);

    /**
     * Relative bulk <i>get</i> method.
     *
     * <p> This method transfers bytes from this buffer into the given
     * destination array.  If there are fewer bytes remaining in the
     * buffer than are required to satisfy the request, that is, if
     * <code style="white-space:nowrap">length &gt; remaining()</code>, then no
     * bytes are transferred and a {@link java.nio.BufferUnderflowException} is
     * thrown.
     *
     * <p> Otherwise, this method copies {@code length} bytes from this
     * buffer into the given array, starting at the current position of this
     * buffer and at the given offset in the array.  The position of this
     * buffer is then incremented by {@code length}.
     *
     * <p> In other words, an invocation of this method of the form
     * <code style="white-space:nowrap">src.get(dst, off, len)</code>
     * has exactly the same effect as the loop
     *
     * <pre>
     * {@code
     *     for (int i = off; i < off + len; i++)
     *         dst[i] = src.get();
     * }
     * </pre>
     *
     * except that it first checks that there are sufficient bytes in
     * this buffer and it is potentially much more efficient.
     *
     * @param  bytes
     *         The array into which bytes are to be written
     *
     * @param  offset
     *         The offset within the array of the first byte to be
     *         written; must be non-negative and no larger than
     *         {@code dst.length}
     *
     * @param  length
     *         The maximum number of bytes to be written to the given
     *         array; must be non-negative and no larger than
     *         {@code dst.length - offset}
     *
     * @return  This buffer
     *
     * @throws java.nio.BufferUnderflowException
     *          If there are fewer than {@code length} bytes
     *          remaining in this buffer
     *
     * @throws  IndexOutOfBoundsException
     *          If the preconditions on the {@code offset} and {@code length}
     *          parameters do not hold
     */
    ByteBuf get(byte[] bytes, int offset, int length);

    /**
     * Absolute bulk <i>get</i> method.
     *
     * <p> This method transfers bytes from this buffer into the given destination array.  If there are fewer bytes remaining in the buffer
     * than are required to satisfy the request, that is, if {@code length &gt; remaining}, then no bytes are
     * transferred and a {@link java.nio.BufferUnderflowException} is thrown.</p>
     *
     * <p> Otherwise, this method copies {@code length} bytes from this buffer into the given array, starting at the given index buffer
     * and at the given offset in the array.</p>
     *
     * <p> In other words, an invocation of this method of the form {@code src.get(dst, off, len)} has exactly the same
     * effect as the loop </p>
     * <pre>
     * {@code
     *     for (int i = off; i < off + len; i++)
     *         dst[i] = src.get(i);
     * }
     * </pre>
     *
     * except that it first checks that there are sufficient bytes in this buffer and it is potentially much more efficient.
     *
     * @param index  The index from which the bytes will be read
     * @param bytes  The array into which bytes are to be written
     * @param offset The offset within the array of the first byte to be written; must be non-negative and no larger than
     *               {@code dst.length}
     * @param length The maximum number of bytes to be written to the given array; must be non-negative and no larger than
     *               {@code dst.length - offset}
     * @return This buffer
     * @throws java.nio.BufferUnderflowException If there are fewer than {@code length} bytes remaining in this buffer
     * @throws IndexOutOfBoundsException         If the preconditions on the {@code offset} and {@code length} parameters do not hold
     */
    ByteBuf get(int index, byte[] bytes, int offset, int length);

    /**
     * <p> Relative <em>get</em> method for reading a long value. </p> <p>Reads the next eight bytes at this buffer's current position,
     * composing them into a long value according to the current byte order, and then increments the position by eight.  </p>
     *
     * @return The long value at the buffer's current position
     * @throws java.nio.BufferUnderflowException If there are fewer than eight bytes remaining in this buffer
     */
    long getLong();

    /**
     * Absolute <i>get</i> method for reading a long value.
     *
     * <p> Reads eight bytes at the given index, composing them into a long value according to the current byte order.  </p>
     *
     * @param  index The index from which the bytes will be read
     *
     * @return The long value at the given index
     *
     * @throws IndexOutOfBoundsException If {@code index} is negative or not smaller than the buffer's limit, minus seven
     */
    long getLong(int index);

    /**
     * <p>Relative <em>get</em> method for reading a double value.</p>
     *
     * <p>Reads the next eight bytes at this buffer's current position, composing them into a double value according to the current byte
     * order, and then increments the position by eight.  </p>
     *
     * @return The double value at the buffer's current position
     * @throws java.nio.BufferUnderflowException If there are fewer than eight bytes remaining in this buffer
     */
    double getDouble();

    /**
     * Absolute <em>get</em> method for reading a double value.
     *
     * <p> Reads eight bytes at the given index, composing them into a double value
     * according to the current byte order.  </p>
     *
     * @param index The index from which the bytes will be read
     * @return The double value at the given index
     * @throws IndexOutOfBoundsException If {@code index} is negative or not smaller than the buffer's limit, minus seven
     */
    double getDouble(int index);

    /**
     * <p>Relative <em>get</em> method for reading an int value.</p>
     *
     * <p>Reads the next four bytes at this buffer's current position, composing them into an int value according to the current byte order,
     * and then increments the position by four.  </p>
     *
     * @return The int value at the buffer's current position
     * @throws java.nio.BufferUnderflowException If there are fewer than four bytes remaining in this buffer
     */
    int getInt();

    /**
     * Absolute <em>get</em> method for reading an int value. <p> Reads four bytes at the given index, composing them into a int value
     * according to the current byte order.  </p>
     *
     * @param index The index from which the bytes will be read
     * @return The int value at the given index
     * @throws IndexOutOfBoundsException If {@code index} is negative or not smaller than the buffer's limit, minus three
     */
    int getInt(int index);

    /**
     * Returns this buffer's position.
     *
     * @return The position of this buffer
     */
    int position();

    /**
     * Sets this buffer's limit.  If the position is larger than the new limit then it is set to the new limit.  If the mark is defined and
     * larger than the new limit then it is discarded.
     *
     * @param newLimit The new limit value; must be non-negative and no larger than this buffer's capacity
     * @return This buffer
     * @throws IllegalArgumentException If the preconditions on {@code newLimit} do not hold
     */
    ByteBuf limit(int newLimit);


    /**
     * <p>Creates a new, read-only byte buffer that shares this buffer's content.</p>
     *
     * <p>The content of the new buffer will be that of this buffer.  Changes to this buffer's content will be visible in the new buffer;
     * the new buffer itself, however, will be read-only and will not allow the shared content to be modified.  The two buffers' position,
     * limit, and mark values will be independent.</p>
     *
     * <p>The new buffer's capacity, limit, position, and mark values will be identical to those of this buffer.</p>
     *
     * @return The new, read-only byte buffer
     */
    ByteBuf asReadOnly();


    /**
     * <p>Creates a new byte buffer that shares this buffer's content.</p>
     *
     * <p>The content of the new buffer will be that of this buffer.  Changes to this buffer's content will be visible in the new buffer,
     * and vice versa; the two buffers' position, limit, and mark values will be independent.</p>
     *
     * <p>The new buffer's capacity, limit, position, and mark values will be identical to those of this buffer.  The new buffer will be
     * direct if, and only if, this buffer is direct, and it will be read-only if, and only if, this buffer is read-only. </p>
     *
     * @return The new byte buffer
     */
    ByteBuf duplicate();

    /**
     * Gets the underlying NIO {@code ByteBuffer}.  Changes made directly to the returned buffer will be reflected in this instance, and
     * vice versa, so be careful.  This method should really only be used so that the underlying buffer can be passed directly to a socket
     * channel.
     *
     * @return the underlying ByteBuffer
     */
    ByteBuffer asNIO();

    /**
     * Gets the current reference count, which starts at 0.
     *
     * @return the current count, which must be greater than or equal to 0
     */
    int getReferenceCount();

    /**
     * Retain an additional reference to this object.  All retained references must be released, or there will be a leak.
     *
     * @return this
     */
    ByteBuf retain();

    /**
     * Release a reference to this object.
     * @throws java.lang.IllegalStateException if the reference count is already 0
     */
    void release();
}
