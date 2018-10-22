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

import com.mongodb.connection.AsyncCompletionHandler;

import java.nio.ByteBuffer;

// An asynchronous channel that can write bytes.
interface AsyncWritableByteChannel {
     // Writes a sequence of bytes to this channel from the given buffer.
     //
     // This method initiates an asynchronous write operation to write a
     // sequence of bytes to this channel from the given buffer. The
     // handler parameter is a completion handler that is invoked when the write
     // operation completes (or fails). The result passed to the completion
     // handler is the number of bytes written.
     //
     // The write operation may write up to r bytes to the channel,
     // where r is the number of bytes remaining in the buffer, that is,
     // src.remaining() at the time that the write is attempted. Where
     // r is 0, the write operation completes immediately with a result of
     // 0 without initiating an I/O operation.
     //
     // Suppose that a byte sequence of length n is written, where 0 < n <= r.
     // This byte sequence will be transferred from the buffer starting at index
     // p, where p is the buffer's position at the moment the write is performed;
     // the index of the last byte written will be p + n - 1}.
     // Upon completion the buffer's position will be equal to p + n;
     // its limit will not have changed.
     //
     // Buffers are not safe for use by multiple concurrent threads so care
     // should be taken to not access the buffer until the operation has
     // completed.
     //
     // This method may be invoked at any time. Some channel types may not
     // allow more than one write to be outstanding at any given time. If a thread
     // initiates a write operation before a previous write operation has
     // completed then a java.nio.channels.WritePendingException will be thrown.
    void write(ByteBuffer src, AsyncCompletionHandler<Void> handler);
}
