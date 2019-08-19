/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.reactivestreams.client.gridfs.helpers;

import com.mongodb.reactivestreams.client.gridfs.AsyncInputStream;
import com.mongodb.reactivestreams.client.gridfs.AsyncOutputStream;
import com.mongodb.reactivestreams.client.internal.GridFSAsyncStreamHelper;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;


/**
 * A general helper class that creates {@link AsyncInputStream} or {@link AsyncOutputStream} instances.
 *
 * Provides support for:
 * <ul>
 *     <li>{@code byte[]} - Converts byte arrays into Async Streams</li>
 *     <li>{@link ByteBuffer} - Converts ByteBuffers into Async Streams</li>
 *     <li>{@link InputStream} - Converts InputStreams into Async Streams (Note: InputStream implementations are blocking)</li>
 *     <li>{@link OutputStream} - Converts OutputStreams into Async Streams (Note: OutputStream implementations are blocking)</li>
 * </ul>
 *
 * @since 1.3
 */
@SuppressWarnings("deprecation")
public final class AsyncStreamHelper {

    /**
     * Converts a {@code byte[]} into a {@link AsyncInputStream}
     *
     * @param srcBytes the data source
     * @return the AsyncInputStream
     */
    public static AsyncInputStream toAsyncInputStream(final byte[] srcBytes) {
        return GridFSAsyncStreamHelper.toAsyncInputStream(com.mongodb.async.client.gridfs.helpers.AsyncStreamHelper
                .toAsyncInputStream(srcBytes));
    }

    /**
     * Converts a {@code byte[]} into a {@link AsyncOutputStream}
     *
     * @param dstBytes the data destination
     * @return the AsyncOutputStream
     */
    public static AsyncOutputStream toAsyncOutputStream(final byte[] dstBytes) {
        return GridFSAsyncStreamHelper.toAsyncOutputStream(com.mongodb.async.client.gridfs.helpers.AsyncStreamHelper
                .toAsyncOutputStream(dstBytes));
    }

    /**
     * Converts a {@link ByteBuffer} into a {@link AsyncInputStream}
     *
     * @param srcByteBuffer the data source
     * @return the AsyncInputStream
     */
    public static AsyncInputStream toAsyncInputStream(final ByteBuffer srcByteBuffer) {
        return GridFSAsyncStreamHelper.toAsyncInputStream(com.mongodb.async.client.gridfs.helpers.AsyncStreamHelper
                .toAsyncInputStream(srcByteBuffer));
    }

    /**
     * Converts a {@link ByteBuffer} into a {@link AsyncOutputStream}
     *
     * @param dstByteBuffer the data destination
     * @return the AsyncOutputStream
     */
    public static AsyncOutputStream toAsyncOutputStream(final ByteBuffer dstByteBuffer) {
        return GridFSAsyncStreamHelper.toAsyncOutputStream(com.mongodb.async.client.gridfs.helpers.AsyncStreamHelper
                .toAsyncOutputStream(dstByteBuffer));
    }

    /**
     * Converts a {@link InputStream} into a {@link AsyncInputStream}
     *
     * @param inputStream the InputStream
     * @return the AsyncInputStream
     */
    public static AsyncInputStream toAsyncInputStream(final InputStream inputStream) {
        return GridFSAsyncStreamHelper.toAsyncInputStream(com.mongodb.async.client.gridfs.helpers.AsyncStreamHelper
                .toAsyncInputStream(inputStream));
    }

    /**
     * Converts a {@link OutputStream} into a {@link AsyncOutputStream}
     *
     * @param outputStream the OutputStream
     * @return the AsyncOutputStream
     */
    public static AsyncOutputStream toAsyncOutputStream(final OutputStream outputStream) {
        return GridFSAsyncStreamHelper.toAsyncOutputStream(com.mongodb.async.client.gridfs.helpers.AsyncStreamHelper
                .toAsyncOutputStream(outputStream));
    }

    private AsyncStreamHelper() {
    }
}
