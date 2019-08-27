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

package com.mongodb.internal.async.client.gridfs;

import com.mongodb.internal.async.SingleResultCallback;

import java.nio.ByteBuffer;

/**
 * The Async Input Stream interface represents some asynchronous input stream of bytes.
 *
 * <p>See the {@link com.mongodb.internal.async.client.gridfs.helpers} package for adapters that create an {@code AsyncInputStream}</p>
 */
public interface AsyncInputStream {

    /**
     * Reads a sequence of bytes from this stream into the given buffer.
     *
     * @param dst      the destination buffer
     * @param callback the callback returning the total number of bytes read into the buffer, or
     *                 {@code -1} if there is no more data because the end of the stream has been reached.
     */
    void read(ByteBuffer dst, SingleResultCallback<Integer> callback);

    /**
     * Skips over and discards n bytes of data from this input stream.
     * @param bytesToSkip the number of bytes to skip
     * @param callback the callback returning the actual number of bytes skipped
     *
     * @since 3.10
     */
    void skip(long bytesToSkip, SingleResultCallback<Long> callback);

    /**
     * Closes the input stream
     *
     * @param callback the callback that indicates when the stream has been closed
     */
    void close(SingleResultCallback<Void> callback);
}
