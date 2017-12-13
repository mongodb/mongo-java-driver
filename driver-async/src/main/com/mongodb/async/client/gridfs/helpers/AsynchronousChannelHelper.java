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

import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.gridfs.AsyncInputStream;
import com.mongodb.async.client.gridfs.AsyncOutputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousByteChannel;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A helper class to convert to {@link AsynchronousByteChannel} or {@link AsynchronousFileChannel} instances into {@link AsyncInputStream}
 * or {@link AsyncOutputStream} instances.
 *
 * <p>Requires Java 7 or greater.</p>
 * @since 3.3
 */
public final class AsynchronousChannelHelper {

    /**
     * Converts a {@link AsynchronousByteChannel} into a {@link AsyncInputStream}
     *
     * @param asynchronousByteChannel the AsynchronousByteChannel
     * @return the AsyncInputStream
     */
    public static AsyncInputStream channelToInputStream(final AsynchronousByteChannel asynchronousByteChannel) {
        notNull("asynchronousByteChannel", asynchronousByteChannel);
        return new AsyncInputStream() {
            @Override
            public void read(final ByteBuffer dst, final SingleResultCallback<Integer> callback) {
                asynchronousByteChannel.read(dst, null, new CompletionHandler<Integer, Object>() {
                    @Override
                    public void completed(final Integer result, final Object attachment) {
                        callback.onResult(result, null);
                    }

                    @Override
                    public void failed(final Throwable exc, final Object attachment) {
                        callback.onResult(null, exc);
                    }
                });
            }

            @Override
            public void close(final SingleResultCallback<Void> callback) {
                try {
                    asynchronousByteChannel.close();
                    callback.onResult(null, null);
                } catch (Exception e) {
                    callback.onResult(null, e);
                }
            }
        };
    }


    /**
     * Converts a {@link AsynchronousFileChannel} into a {@link AsyncInputStream}
     *
     * @param asynchronousFileChannel the AsynchronousFileChannel
     * @return the AsyncInputStream
     */
    public static AsyncInputStream channelToInputStream(final AsynchronousFileChannel asynchronousFileChannel) {
        notNull("asynchronousByteChannel", asynchronousFileChannel);
        return new AsyncInputStream() {
            private int position = 0;
            @Override
            public void read(final ByteBuffer dst, final SingleResultCallback<Integer> callback) {
                asynchronousFileChannel.read(dst, position, null, new CompletionHandler<Integer, Object>() {
                    @Override
                    public void completed(final Integer result, final Object attachment) {
                        position += result;
                        callback.onResult(result, null);
                    }

                    @Override
                    public void failed(final Throwable exc, final Object attachment) {
                        callback.onResult(null, exc);
                    }
                });
            }

            @Override
            public void close(final SingleResultCallback<Void> callback) {
                try {
                    asynchronousFileChannel.close();
                    callback.onResult(null, null);
                } catch (Exception e) {
                    callback.onResult(null, e);
                }
            }
        };
    }

    /**
     * Converts a {@link AsynchronousByteChannel} into a {@link AsyncOutputStream}
     *
     * @param asynchronousByteChannel the AsynchronousByteChannel
     * @return the AsyncOutputStream
     */
    public static AsyncOutputStream channelToOutputStream(final AsynchronousByteChannel asynchronousByteChannel) {
        notNull("asynchronousByteChannel", asynchronousByteChannel);
        return new AsyncOutputStream() {
            @Override
            public void write(final ByteBuffer src, final SingleResultCallback<Integer> callback) {
                asynchronousByteChannel.write(src, null, new CompletionHandler<Integer, Object>() {
                    @Override
                    public void completed(final Integer result, final Object attachment) {
                        callback.onResult(result, null);
                    }

                    @Override
                    public void failed(final Throwable exc, final Object attachment) {
                        callback.onResult(null, exc);
                    }
                });
            }

            @Override
            public void close(final SingleResultCallback<Void> callback) {
                try {
                    asynchronousByteChannel.close();
                    callback.onResult(null, null);
                } catch (IOException e) {
                    callback.onResult(null, e);
                }
            }
        };
    }

    /**
     * Converts a {@link AsynchronousFileChannel} into a {@link AsyncOutputStream}
     *
     * @param asynchronousFileChannel the AsynchronousFileChannel
     * @return the AsyncOutputStream
     */
    public static AsyncOutputStream channelToOutputStream(final AsynchronousFileChannel asynchronousFileChannel) {
        notNull("asynchronousByteChannel", asynchronousFileChannel);
        return new AsyncOutputStream() {
            private int position = 0;
            @Override
            public void write(final ByteBuffer src, final SingleResultCallback<Integer> callback) {
                asynchronousFileChannel.write(src, position, null, new CompletionHandler<Integer, Object>() {
                    @Override
                    public void completed(final Integer result, final Object attachment) {
                        position += result;
                        callback.onResult(result, null);
                    }

                    @Override
                    public void failed(final Throwable exc, final Object attachment) {
                        callback.onResult(null, exc);
                    }
                });
            }

            @Override
            public void close(final SingleResultCallback<Void> callback) {
                try {
                    asynchronousFileChannel.close();
                    callback.onResult(null, null);
                } catch (IOException e) {
                    callback.onResult(null, e);
                }
            }
        };
    }

    private AsynchronousChannelHelper() {
    }
}
