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

package com.mongodb.connection;

import java.nio.ByteBuffer;

/**
 * An asynchronous channel that can write bytes.
 *
 * @since 3.0
 */
interface AsyncWritableByteChannel {
    /**
     * Writes a sequence of bytes to this channel from the given buffer.
     *
     * <p> This method initiates an asynchronous write operation to write a
     * sequence of bytes to this channel from the given buffer. The {@code
     * handler} parameter is a completion handler that is invoked when the write
     * operation completes (or fails). The result passed to the completion
     * handler is the number of bytes written.
     *
     * <p> The write operation may write up to <i>r</i> bytes to the channel,
     * where <i>r</i> is the number of bytes remaining in the buffer, that is,
     * {@code src.remaining()} at the time that the write is attempted. Where
     * <i>r</i> is 0, the write operation completes immediately with a result of
     * {@code 0} without initiating an I/O operation.
     *
     * <p> Suppose that a byte sequence of length <i>n</i> is written, where
     * <tt>0</tt>&nbsp;<tt>&lt;</tt>&nbsp;<i>n</i>&nbsp;<tt>&lt;=</tt>&nbsp;<i>r</i>.
     * This byte sequence will be transferred from the buffer starting at index
     * <i>p</i>, where <i>p</i> is the buffer's position at the moment the
     * write is performed; the index of the last byte written will be
     * <i>p</i>&nbsp;<tt>+</tt>&nbsp;<i>n</i>&nbsp;<tt>-</tt>&nbsp;<tt>1</tt>.
     * Upon completion the buffer's position will be equal to
     * <i>p</i>&nbsp;<tt>+</tt>&nbsp;<i>n</i>; its limit will not have changed.
     *
     * <p> Buffers are not safe for use by multiple concurrent threads so care
     * should be taken to not access the buffer until the operation has
     * completed.
     *
     * <p> This method may be invoked at any time. Some channel types may not
     * allow more than one write to be outstanding at any given time. If a thread
     * initiates a write operation before a previous write operation has
     * completed then a {@link java.nio.channels.WritePendingException} will be thrown.
     *
     * @param   src
     *          The buffer from which bytes are to be retrieved
     * @param   handler
     *          The completion handler object
     *
     * @throws java.nio.channels.WritePendingException
     *          If the channel does not allow more than one write to be outstanding
     *          and a previous write has not completed
     * @throws java.nio.channels.ShutdownChannelGroupException
     *          If the channel is associated with a {@link java.nio.channels.AsynchronousChannelGroup
     *          group} that has terminated
     */
    void write(ByteBuffer src, AsyncCompletionHandler<Void> handler);
}
