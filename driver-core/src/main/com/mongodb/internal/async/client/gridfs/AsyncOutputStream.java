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

import com.mongodb.async.SingleResultCallback;

import java.nio.ByteBuffer;


/**
 * The Async Output Stream interface represents some asynchronous output stream of bytes.
 *
 * <p>See the {@link com.mongodb.internal.async.client.gridfs.helpers} package for adapters that create an {@code AsyncOutputStream}</p>
 */
public interface AsyncOutputStream {

    /**
     * Writes a sequence of bytes from the given buffer into this stream.
     *
     * @param src      the source buffer containing the data to be written.
     * @param callback the callback returning the number of bytes written.
     */
    void write(ByteBuffer src, SingleResultCallback<Integer> callback);

    /**
     * Closes the output stream
     *
     * @param callback the callback that indicates when the stream has been closed
     */
    void close(SingleResultCallback<Void> callback);
}
