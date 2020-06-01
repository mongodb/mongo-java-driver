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
 *
 * Original Work: MIT License, Copyright (c) [2015-2020] all contributors
 * https://github.com/marianobarrios/tls-channel
 */

package com.mongodb.internal.connection.tlschannel.async;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousByteChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.InterruptedByTimeoutException;
import java.nio.channels.ReadPendingException;
import java.nio.channels.ShutdownChannelGroupException;
import java.nio.channels.WritePendingException;
import java.util.concurrent.TimeUnit;

/**
 * This interface extends {@link AsynchronousByteChannel} adding optional timeouts and scattering
 * and gathering methods. These additions are analogous to the ones made by {@link
 * java.nio.channels.AsynchronousSocketChannel}.
 */
public interface ExtendedAsynchronousByteChannel extends AsynchronousByteChannel {

  /**
   * Reads a sequence of bytes from this channel into the given buffer.
   *
   * <p>This method initiates an asynchronous read operation to read a sequence of bytes from this
   * channel into the given buffer. The {@code handler} parameter is a completion handler that is
   * invoked when the read operation completes (or fails). The result passed to the completion
   * handler is the number of bytes read or {@code -1} if no bytes could be read because the channel
   * has reached end-of-stream.
   *
   * <p>If a timeout is specified and the timeout elapses before the operation completes then the
   * operation completes with the exception {@link InterruptedByTimeoutException}. Where a timeout
   * occurs, and the implementation cannot guarantee that bytes have not been read, or will not be
   * read from the channel into the given buffer, then further attempts to read from the channel
   * will cause an unspecific runtime exception to be thrown.
   *
   * <p>Otherwise this method works in the same manner as the {@link
   * AsynchronousByteChannel#read(ByteBuffer,Object,CompletionHandler)} method.
   *
   * @param   <A> The type for the object to the attached to the operation
   * @param dst The buffer into which bytes are to be transferred
   * @param timeout The maximum time for the I/O operation to complete
   * @param unit The time unit of the {@code timeout} argument
   * @param attach The object to attach to the I/O operation; can be {@code null}
   * @param handler The handler for consuming the result
   * @throws IllegalArgumentException If the buffer is read-only
   * @throws ReadPendingException If a read operation is already in progress on this channel
   * @throws ShutdownChannelGroupException If the channel group has terminated
   */
  <A> void read(
      ByteBuffer dst,
      long timeout,
      TimeUnit unit,
      A attach,
      CompletionHandler<Integer, ? super A> handler);

  /**
   * Reads a sequence of bytes from this channel into a subsequence of the given buffers. This
   * operation, sometimes called a <em>scattering read</em>, is often useful when implementing
   * network protocols that group data into segments consisting of one or more fixed-length headers
   * followed by a variable-length body. The {@code handler} parameter is a completion handler that
   * is invoked when the read operation completes (or fails). The result passed to the completion
   * handler is the number of bytes read or {@code -1} if no bytes could be read because the channel
   * has reached end-of-stream.
   *
   * <p>This method initiates a read of up to <i>r</i> bytes from this channel, where <i>r</i> is
   * the total number of bytes remaining in the specified subsequence of the given buffer array,
   * that is,
   *
   * <blockquote>
   *
   * <pre>
   * dsts[offset].remaining()
   *     + dsts[offset+1].remaining()
   *     + ... + dsts[offset+length-1].remaining()</pre>
   *
   * </blockquote>
   *
   * at the moment that the read is attempted.
   *
   * <p>Suppose that a byte sequence of length <i>n</i> is read, where <code>0</code>&nbsp;<code>
   * &lt;</code>&nbsp;<i>n</i>&nbsp;<code>&lt;=</code>&nbsp;<i>r</i>. Up to the first <code>
   * dsts[offset].remaining()</code> bytes of this sequence are transferred into buffer <code>
   * dsts[offset]</code>, up to the next <code>dsts[offset+1].remaining()</code> bytes are
   * transferred into buffer <code>dsts[offset+1]</code>, and so forth, until the entire byte
   * sequence is transferred into the given buffers. As many bytes as possible are transferred into
   * each buffer, hence the final position of each updated buffer, except the last updated buffer,
   * is guaranteed to be equal to that buffer's limit. The underlying operating system may impose a
   * limit on the number of buffers that may be used in an I/O operation. Where the number of
   * buffers (with bytes remaining), exceeds this limit, then the I/O operation is performed with
   * the maximum number of buffers allowed by the operating system.
   *
   * <p>If a timeout is specified and the timeout elapses before the operation completes then it
   * completes with the exception {@link InterruptedByTimeoutException}. Where a timeout occurs, and
   * the implementation cannot guarantee that bytes have not been read, or will not be read from the
   * channel into the given buffers, then further attempts to read from the channel will cause an
   * unspecific runtime exception to be thrown.
   *
   * @param   <A> The type for the object to the attached to the operation
   * @param dsts The buffers into which bytes are to be transferred
   * @param offset The offset within the buffer array of the first buffer into which bytes are to be
   *     transferred; must be non-negative and no larger than {@code dsts.length}
   * @param length The maximum number of buffers to be accessed; must be non-negative and no larger
   *     than {@code dsts.length - offset}
   * @param timeout The maximum time for the I/O operation to complete
   * @param unit The time unit of the {@code timeout} argument
   * @param attach The object to attach to the I/O operation; can be {@code null}
   * @param handler The handler for consuming the result
   * @throws IndexOutOfBoundsException If the pre-conditions for the {@code offset} and {@code
   *     length} parameter aren't met
   * @throws IllegalArgumentException If the buffer is read-only
   * @throws ReadPendingException If a read operation is already in progress on this channel
   * @throws ShutdownChannelGroupException If the channel group has terminated
   */
  <A> void read(
      ByteBuffer[] dsts,
      int offset,
      int length,
      long timeout,
      TimeUnit unit,
      A attach,
      CompletionHandler<Long, ? super A> handler);

  /**
   * Writes a sequence of bytes to this channel from the given buffer.
   *
   * <p>This method initiates an asynchronous write operation to write a sequence of bytes to this
   * channel from the given buffer. The {@code handler} parameter is a completion handler that is
   * invoked when the write operation completes (or fails). The result passed to the completion
   * handler is the number of bytes written.
   *
   * <p>If a timeout is specified and the timeout elapses before the operation completes then it
   * completes with the exception {@link InterruptedByTimeoutException}. Where a timeout occurs, and
   * the implementation cannot guarantee that bytes have not been written, or will not be written to
   * the channel from the given buffer, then further attempts to write to the channel will cause an
   * unspecific runtime exception to be thrown.
   *
   * <p>Otherwise this method works in the same manner as the {@link
   * AsynchronousByteChannel#write(ByteBuffer,Object,CompletionHandler)} method.
   *
   * @param   <A> The type for the object to the attached to the operation
   * @param src The buffer from which bytes are to be retrieved
   * @param timeout The maximum time for the I/O operation to complete
   * @param unit The time unit of the {@code timeout} argument
   * @param attach The object to attach to the I/O operation; can be {@code null}
   * @param handler The handler for consuming the result
   * @throws WritePendingException If a write operation is already in progress on this channel
   * @throws ShutdownChannelGroupException If the channel group has terminated
   */
  <A> void write(
      ByteBuffer src,
      long timeout,
      TimeUnit unit,
      A attach,
      CompletionHandler<Integer, ? super A> handler);

  /**
   * Writes a sequence of bytes to this channel from a subsequence of the given buffers. This
   * operation, sometimes called a <em>gathering write</em>, is often useful when implementing
   * network protocols that group data into segments consisting of one or more fixed-length headers
   * followed by a variable-length body. The {@code handler} parameter is a completion handler that
   * is invoked when the write operation completes (or fails). The result passed to the completion
   * handler is the number of bytes written.
   *
   * <p>This method initiates a write of up to <i>r</i> bytes to this channel, where <i>r</i> is the
   * total number of bytes remaining in the specified subsequence of the given buffer array, that
   * is,
   *
   * <blockquote>
   *
   * <pre>
   * srcs[offset].remaining()
   *     + srcs[offset+1].remaining()
   *     + ... + srcs[offset+length-1].remaining()</pre>
   *
   * </blockquote>
   *
   * at the moment that the write is attempted.
   *
   * <p>Suppose that a byte sequence of length <i>n</i> is written, where <code>0</code>&nbsp;<code>
   * &lt;</code>&nbsp;<i>n</i>&nbsp;<code>&lt;=</code>&nbsp;<i>r</i>. Up to the first <code>
   * srcs[offset].remaining()</code> bytes of this sequence are written from buffer <code>
   * srcs[offset]</code>, up to the next <code>srcs[offset+1].remaining()</code> bytes are written
   * from buffer <code>srcs[offset+1]</code>, and so forth, until the entire byte sequence is
   * written. As many bytes as possible are written from each buffer, hence the final position of
   * each updated buffer, except the last updated buffer, is guaranteed to be equal to that buffer's
   * limit. The underlying operating system may impose a limit on the number of buffers that may be
   * used in an I/O operation. Where the number of buffers (with bytes remaining), exceeds this
   * limit, then the I/O operation is performed with the maximum number of buffers allowed by the
   * operating system.
   *
   * <p>If a timeout is specified and the timeout elapses before the operation completes then it
   * completes with the exception {@link InterruptedByTimeoutException}. Where a timeout occurs, and
   * the implementation cannot guarantee that bytes have not been written, or will not be written to
   * the channel from the given buffers, then further attempts to write to the channel will cause an
   * unspecific runtime exception to be thrown.
   *
   * @param   <A> The type for the object to the attached to the operation
   * @param srcs The buffers from which bytes are to be retrieved
   * @param offset The offset within the buffer array of the first buffer from which bytes are to be
   *     retrieved; must be non-negative and no larger than {@code srcs.length}
   * @param length The maximum number of buffers to be accessed; must be non-negative and no larger
   *     than {@code srcs.length - offset}
   * @param timeout The maximum time for the I/O operation to complete
   * @param unit The time unit of the {@code timeout} argument
   * @param attach The object to attach to the I/O operation; can be {@code null}
   * @param handler The handler for consuming the result
   * @throws IndexOutOfBoundsException If the pre-conditions for the {@code offset} and {@code
   *     length} parameter aren't met
   * @throws WritePendingException If a write operation is already in progress on this channel
   * @throws ShutdownChannelGroupException If the channel group has terminated
   */
  <A> void write(
      ByteBuffer[] srcs,
      int offset,
      int length,
      long timeout,
      TimeUnit unit,
      A attach,
      CompletionHandler<Long, ? super A> handler);
}
