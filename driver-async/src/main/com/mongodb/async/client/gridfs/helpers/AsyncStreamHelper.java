/*
 * Copyright 2015 MongoDB, Inc.
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

package com.mongodb.async.client.gridfs.helpers;

import com.mongodb.MongoGridFSException;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.gridfs.AsyncInputStream;
import com.mongodb.async.client.gridfs.AsyncOutputStream;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import static org.bson.assertions.Assertions.notNull;

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
 * @since 3.3
 */
public final class AsyncStreamHelper {

    /**
     * Converts a {@code byte[]} into a {@link AsyncInputStream}
     *
     * @param srcBytes the data source
     * @return the AsyncInputStream
     */
    public static AsyncInputStream toAsyncInputStream(final byte[] srcBytes) {
        return toAsyncInputStream(ByteBuffer.wrap(srcBytes));
    }

    /**
     * Converts a {@code byte[]} into a {@link AsyncOutputStream}
     *
     * @param dstBytes the data destination
     * @return the AsyncOutputStream
     */
    public static AsyncOutputStream toAsyncOutputStream(final byte[] dstBytes) {
        return toAsyncOutputStream(ByteBuffer.wrap(dstBytes));
    }

    /**
     * Converts a {@link ByteBuffer} into a {@link AsyncInputStream}
     *
     * @param srcByteBuffer the data source
     * @return the AsyncInputStream
     */
    public static AsyncInputStream toAsyncInputStream(final ByteBuffer srcByteBuffer) {
        notNull("srcByteBuffer", srcByteBuffer);
        return new AsyncInputStream() {
            @Override
            public void read(final ByteBuffer dstByteBuffer, final SingleResultCallback<Integer> callback) {
                transferDataFromByteBuffers(srcByteBuffer, dstByteBuffer, callback);
            }

            @Override
            public void close(final SingleResultCallback<Void> callback) {
                callback.onResult(null, null);
            }
        };
    }

    /**
     * Converts a {@link ByteBuffer} into a {@link AsyncOutputStream}
     *
     * @param dstByteBuffer the data destination
     * @return the AsyncOutputStream
     */
    public static AsyncOutputStream toAsyncOutputStream(final ByteBuffer dstByteBuffer) {
        notNull("dstByteBuffer", dstByteBuffer);
        return new AsyncOutputStream() {
            @Override
            public void write(final ByteBuffer srcByteBuffer, final SingleResultCallback<Integer> callback) {
                transferDataFromByteBuffers(srcByteBuffer, dstByteBuffer, callback);
            }

            @Override
            public void close(final SingleResultCallback<Void> callback) {
                callback.onResult(null, null);
            }
        };
    }

    /**
     * Converts a {@link InputStream} into a {@link AsyncInputStream}
     *
     * @param inputStream the InputStream
     * @return the AsyncInputStream
     */
    public static AsyncInputStream toAsyncInputStream(final InputStream inputStream) {
        notNull("inputStream", inputStream);
        return new AsyncInputStream() {
            @Override
            public void read(final ByteBuffer dstByteBuffer, final SingleResultCallback<Integer> callback) {
                notNull("dst", dstByteBuffer);
                notNull("callback", callback);
                if (!dstByteBuffer.hasRemaining()) {
                    callback.onResult(-1, null);
                    return;
                }

                int maxAmount = dstByteBuffer.remaining();
                byte[] bytes = new byte[maxAmount];
                int amountRead;
                try {
                    amountRead = inputStream.read(bytes);
                } catch (Throwable t) {
                    callback.onResult(null, new MongoGridFSException("Error reading from input stream", t));
                    return;
                }

                if (amountRead > 0) {
                    if (amountRead < maxAmount) {
                        byte[] dataRead = new byte[amountRead];
                        System.arraycopy(bytes, 0, dataRead, 0, amountRead);
                        dstByteBuffer.put(dataRead);
                    } else {
                        dstByteBuffer.put(bytes);
                    }
                }
                callback.onResult(amountRead, null);
            }

            @Override
            public void close(final SingleResultCallback<Void> callback) {
                try {
                    inputStream.close();
                } catch (Throwable t) {
                    callback.onResult(null, new MongoGridFSException("Error closing input stream", t));
                    return;
                }
                callback.onResult(null, null);
            }
        };
    }

    /**
     * Converts a {@link OutputStream} into a {@link AsyncOutputStream}
     *
     * @param outputStream the OutputStream
     * @return the AsyncOutputStream
     */
    public static AsyncOutputStream toAsyncOutputStream(final OutputStream outputStream) {
        notNull("outputStream", outputStream);
        return new AsyncOutputStream() {
            @Override
            public void write(final ByteBuffer srcByteBuffer, final SingleResultCallback<Integer> callback) {
                notNull("src", srcByteBuffer);
                notNull("callback", callback);
                if (!srcByteBuffer.hasRemaining()) {
                    callback.onResult(-1, null);
                    return;
                }

                int amount = srcByteBuffer.remaining();
                byte[] bytes = new byte[amount];
                try {
                    srcByteBuffer.get(bytes);
                    outputStream.write(bytes);
                } catch (Throwable t) {
                    callback.onResult(null, new MongoGridFSException("Error reading from input stream", t));
                    return;
                }
                callback.onResult(amount, null);
            }

            @Override
            public void close(final SingleResultCallback<Void> callback) {
                try {
                    outputStream.close();
                } catch (Throwable t) {
                    callback.onResult(null, new MongoGridFSException("Error closing from output stream", t));
                    return;
                }
                callback.onResult(null, null);
            }
        };
    }

    private static void transferDataFromByteBuffers(final ByteBuffer srcByteBuffer, final ByteBuffer dstByteBuffer,
                                             final SingleResultCallback<Integer> callback) {
        if (!srcByteBuffer.hasRemaining()) {
            callback.onResult(-1, null);
            return;
        }
        int transferAmount = Math.min(dstByteBuffer.remaining(), srcByteBuffer.remaining());
        if (transferAmount > 0) {
            ByteBuffer temp = srcByteBuffer.duplicate();
            temp.limit(temp.position() + transferAmount);
            dstByteBuffer.put(temp);
            srcByteBuffer.position(srcByteBuffer.position() + transferAmount);
        }
        callback.onResult(transferAmount, null);
    }

    private AsyncStreamHelper() {
    }
}
