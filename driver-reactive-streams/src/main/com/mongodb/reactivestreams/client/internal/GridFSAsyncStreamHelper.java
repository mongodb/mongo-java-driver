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

package com.mongodb.reactivestreams.client.internal;

import com.mongodb.Block;
import com.mongodb.reactivestreams.client.Success;
import com.mongodb.reactivestreams.client.gridfs.AsyncInputStream;
import com.mongodb.reactivestreams.client.gridfs.AsyncOutputStream;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.nio.ByteBuffer;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.reactivestreams.client.internal.PublisherHelper.voidToSuccessCallback;


/**
 * Internal GridFS AsyncStream Helper
 *
 * <p>This should not be considered a part of the public API.</p>
 */
@SuppressWarnings("deprecation")
public final class GridFSAsyncStreamHelper {

    /**
     * Converts the callback AsyncInputStream to a Publisher AsyncInputStream
     *
     * <p>This should not be considered a part of the public API.</p>
     * @param wrapper the callback AsyncInputStream
     * @return the Publisher AsyncInputStream
     */
    public static AsyncInputStream toAsyncInputStream(final com.mongodb.async.client.gridfs.AsyncInputStream wrapper) {
        notNull("wrapper", wrapper);
        return new AsyncInputStream() {
            @Override
            public Publisher<Integer> read(final ByteBuffer dst) {
                return new SingleResultObservableToPublisher<Integer>(
                        new Block<com.mongodb.async.SingleResultCallback<Integer>>() {
                            @Override
                            public void apply(final com.mongodb.async.SingleResultCallback<Integer> callback) {
                                wrapper.read(dst, callback);
                            }
                        });
            }

            @Override
            public Publisher<Long> skip(final long bytesToSkip) {
                return new SingleResultObservableToPublisher<Long>(
                        new Block<com.mongodb.async.SingleResultCallback<Long>>() {
                            @Override
                            public void apply(final com.mongodb.async.SingleResultCallback<Long> callback) {
                                wrapper.skip(bytesToSkip, callback);
                            }
                        });
            }

            @Override
            public Publisher<Success> close() {
                return new SingleResultObservableToPublisher<Success>(
                        new Block<com.mongodb.async.SingleResultCallback<Success>>() {
                            @Override
                            public void apply(final com.mongodb.async.SingleResultCallback<Success> callback) {
                                wrapper.close(voidToSuccessCallback(callback));
                            }
                        });
            }
        };
    }

    /**
     * Converts the callback AsyncOutputStream to a Publisher AsyncOutputStream
     *
     * <p>This should not be considered a part of the public API.</p>
     * @param wrapper the callback AsyncOutputStream
     * @return the Publisher AsyncOutputStream
     */
    public static AsyncOutputStream toAsyncOutputStream(final com.mongodb.async.client.gridfs.AsyncOutputStream wrapper) {
        notNull("wrapper", wrapper);
        return new AsyncOutputStream() {

            @Override
            public Publisher<Integer> write(final ByteBuffer src) {
                return new SingleResultObservableToPublisher<Integer>(
                        new Block<com.mongodb.async.SingleResultCallback<Integer>>() {
                            @Override
                            public void apply(final com.mongodb.async.SingleResultCallback<Integer> callback) {
                                wrapper.write(src, callback);
                            }
                        });
            }

            @Override
            public Publisher<Success> close() {
                return new SingleResultObservableToPublisher<Success>(
                        new Block<com.mongodb.async.SingleResultCallback<Success>>() {
                            @Override
                            public void apply(final com.mongodb.async.SingleResultCallback<Success> callback) {
                                wrapper.close(voidToSuccessCallback(callback));
                            }
                        });
            }
        };
    }

    static com.mongodb.async.client.gridfs.AsyncInputStream toCallbackAsyncInputStream(final AsyncInputStream wrapped) {
        notNull("wrapped", wrapped);
        return new com.mongodb.async.client.gridfs.AsyncInputStream() {

            @Override
            public void read(final ByteBuffer dst, final com.mongodb.async.SingleResultCallback<Integer> callback) {
                wrapped.read(dst).subscribe(new Subscriber<Integer>() {
                    private Integer result = null;

                    @Override
                    public void onSubscribe(final Subscription s) {
                        s.request(1);
                    }

                    @Override
                    public void onNext(final Integer integer) {
                        result = integer;
                    }

                    @Override
                    public void onError(final Throwable t) {
                        callback.onResult(null, t);
                    }

                    @Override
                    public void onComplete() {
                        callback.onResult(result, null);
                    }
                });
            }

            @Override
            public void skip(final long bytesToSkip, final com.mongodb.async.SingleResultCallback<Long> callback) {
                wrapped.skip(bytesToSkip).subscribe(new Subscriber<Long>() {
                    private Long result = null;

                    @Override
                    public void onSubscribe(final Subscription s) {
                        s.request(1);
                    }

                    @Override
                    public void onNext(final Long skipped) {
                        result = skipped;
                    }

                    @Override
                    public void onError(final Throwable t) {
                        callback.onResult(null, t);
                    }

                    @Override
                    public void onComplete() {
                        callback.onResult(result, null);
                    }
                });
            }

            @Override
            public void close(final com.mongodb.async.SingleResultCallback<Void> callback) {
                wrapped.close().subscribe(new Subscriber<Success>() {

                    @Override
                    public void onSubscribe(final Subscription s) {
                        s.request(1);
                    }

                    @Override
                    public void onNext(final Success success) {
                    }

                    @Override
                    public void onError(final Throwable t) {
                        callback.onResult(null, t);
                    }

                    @Override
                    public void onComplete() {
                        callback.onResult(null, null);
                    }
                });
            }
        };
    }

    static com.mongodb.async.client.gridfs.AsyncOutputStream toCallbackAsyncOutputStream(final AsyncOutputStream wrapped) {
        notNull("wrapped", wrapped);
        return new com.mongodb.async.client.gridfs.AsyncOutputStream() {

            @Override
            public void write(final ByteBuffer src, final com.mongodb.async.SingleResultCallback<Integer> callback) {
                wrapped.write(src).subscribe(new Subscriber<Integer>() {
                    private Integer result = null;

                    @Override
                    public void onSubscribe(final Subscription s) {
                        s.request(1);
                    }

                    @Override
                    public void onNext(final Integer integer) {
                        result = integer;
                    }

                    @Override
                    public void onError(final Throwable t) {
                        callback.onResult(null, t);
                    }

                    @Override
                    public void onComplete() {
                        callback.onResult(result, null);
                    }
                });
            }

            @Override
            public void close(final com.mongodb.async.SingleResultCallback<Void> callback) {
                wrapped.close().subscribe(new Subscriber<Success>() {

                    @Override
                    public void onSubscribe(final Subscription s) {
                        s.request(1);
                    }

                    @Override
                    public void onNext(final Success success) {
                    }

                    @Override
                    public void onError(final Throwable t) {
                        callback.onResult(null, t);
                    }

                    @Override
                    public void onComplete() {
                        callback.onResult(null, null);
                    }
                });
            }
        };
    }

    private GridFSAsyncStreamHelper() {
    }
}
