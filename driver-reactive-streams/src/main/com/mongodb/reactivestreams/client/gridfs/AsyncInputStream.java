/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.reactivestreams.client.gridfs;

import com.mongodb.reactivestreams.client.Success;
import org.reactivestreams.Publisher;

import java.nio.ByteBuffer;

/**
 * The Async Input Stream interface represents some asynchronous input stream of bytes.
 *
 * <p>See the {@link com.mongodb.async.client.gridfs.helpers} package for adapters that create an {@code AsyncInputStream}</p>
 * @since 1.3
 */
public interface AsyncInputStream {

    /**
     * Reads a sequence of bytes from this stream into the given buffer.
     *
     * @param dst      the destination buffer
     * @return a publisher with a single element, the total number of bytes read into the buffer, or
     *         {@code -1} if there is no more data because the end of the stream has been reached.
     */
    Publisher<Integer> read(ByteBuffer dst);

    /**
     * Skips over and discards n bytes of data from this input stream.
     * @param bytesToSkip the number of bytes to skip
     * @return a publisher with a single element, the actual number of bytes skipped
     *
     * @since 1.11
     */
    Publisher<Long> skip(long bytesToSkip);

    /**
     * Closes the input stream
     *
     * @return a publisher with a single element indicating when the stream has been closed
     */
    Publisher<Success> close();
}
