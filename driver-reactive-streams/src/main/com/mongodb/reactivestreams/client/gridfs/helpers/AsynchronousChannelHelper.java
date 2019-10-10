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
package com.mongodb.reactivestreams.client.gridfs.helpers;

import com.mongodb.reactivestreams.client.gridfs.AsyncInputStream;
import com.mongodb.reactivestreams.client.gridfs.AsyncOutputStream;
import com.mongodb.reactivestreams.client.internal.GridFSAsyncStreamHelper;

import java.nio.channels.AsynchronousByteChannel;
import java.nio.channels.AsynchronousFileChannel;

/**
 * A helper class to convert to {@link AsynchronousByteChannel} or {@link AsynchronousFileChannel} instances into {@link AsyncInputStream}
 * or {@link AsyncOutputStream} instances.
 *
 * @since 4.0
 */
public final class AsynchronousChannelHelper {

    /**
     * Converts a {@link AsynchronousByteChannel} into a {@link AsyncInputStream}
     *
     * @param asynchronousByteChannel the AsynchronousByteChannel
     * @return the AsyncInputStream
     */
    public static AsyncInputStream channelToInputStream(final AsynchronousByteChannel asynchronousByteChannel) {
        return GridFSAsyncStreamHelper.toAsyncInputStream(
                com.mongodb.internal.async.client.gridfs.helpers.AsynchronousChannelHelper.channelToInputStream(asynchronousByteChannel));
    }

    /**
     * Converts a {@link AsynchronousFileChannel} into a {@link AsyncInputStream}
     *
     * @param asynchronousFileChannel the AsynchronousFileChannel
     * @return the AsyncInputStream
     */
    public static AsyncInputStream channelToInputStream(final AsynchronousFileChannel asynchronousFileChannel) {
        return GridFSAsyncStreamHelper.toAsyncInputStream(
                com.mongodb.internal.async.client.gridfs.helpers.AsynchronousChannelHelper.channelToInputStream(asynchronousFileChannel));
    }

    /**
     * Converts a {@link AsynchronousByteChannel} into a {@link AsyncOutputStream}
     *
     * @param asynchronousByteChannel the AsynchronousByteChannel
     * @return the AsyncOutputStream
     */
    public static AsyncOutputStream channelToOutputStream(final AsynchronousByteChannel asynchronousByteChannel) {
        return GridFSAsyncStreamHelper.toAsyncOutputStream(
                com.mongodb.internal.async.client.gridfs.helpers.AsynchronousChannelHelper.channelToOutputStream(asynchronousByteChannel));
    }

    /**
     * Converts a {@link AsynchronousFileChannel} into a {@link AsyncOutputStream}
     *
     * @param asynchronousFileChannel the AsynchronousFileChannel
     * @return the AsyncOutputStream
     */
    public static AsyncOutputStream channelToOutputStream(final AsynchronousFileChannel asynchronousFileChannel) {
        return GridFSAsyncStreamHelper.toAsyncOutputStream(
                com.mongodb.internal.async.client.gridfs.helpers.AsynchronousChannelHelper.channelToOutputStream(asynchronousFileChannel));
    }

    private AsynchronousChannelHelper() {
    }
}
