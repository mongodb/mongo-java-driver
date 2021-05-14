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

package com.mongodb.internal.operation;

import com.mongodb.MongoChangeStreamException;
import com.mongodb.MongoException;
import com.mongodb.internal.async.AsyncAggregateResponseBatchCursor;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncConnectionSource;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.operation.OperationHelper.AsyncCallableWithSource;
import com.mongodb.lang.NonNull;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.ChangeStreamBatchCursorHelper.isRetryableError;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.OperationHelper.withAsyncReadConnection;
import static java.lang.String.format;

final class AsyncChangeStreamBatchCursor<T> implements AsyncAggregateResponseBatchCursor<T> {
    private final AsyncReadBinding binding;
    private final ChangeStreamOperation<T> changeStreamOperation;
    private final int maxWireVersion;

    private volatile BsonDocument resumeToken;
    /**
     * {@linkplain ChangeStreamBatchCursorHelper#isRetryableError(Throwable, int) Retryable errors} can result in
     * {@code wrapped} containing {@code null} and {@link #isClosed} being {@code false}.
     * This represents a situation in which the wrapped object was closed by {@code this} but {@code this} remained open.
     */
    private final AtomicReference<AsyncAggregateResponseBatchCursor<RawBsonDocument>> wrapped;
    private final AtomicBoolean isClosed;

    AsyncChangeStreamBatchCursor(final ChangeStreamOperation<T> changeStreamOperation,
                                 final AsyncAggregateResponseBatchCursor<RawBsonDocument> wrapped,
                                 final AsyncReadBinding binding,
                                 final BsonDocument resumeToken,
                                 final int maxWireVersion) {
        this.changeStreamOperation = changeStreamOperation;
        this.wrapped = new AtomicReference<>(assertNotNull(wrapped));
        this.binding = binding;
        binding.retain();
        this.resumeToken = resumeToken;
        this.maxWireVersion = maxWireVersion;
        isClosed = new AtomicBoolean();
    }

    @NonNull
    AsyncAggregateResponseBatchCursor<RawBsonDocument> getWrapped() {
        return assertNotNull(wrapped.get());
    }

    @Override
    public void next(final SingleResultCallback<List<T>> callback) {
        resumeableOperation(new AsyncBlock() {
            @Override
            public void apply(final AsyncAggregateResponseBatchCursor<RawBsonDocument> cursor,
                              final SingleResultCallback<List<RawBsonDocument>> callback) {
                cursor.next(callback);
            }
        }, convertResultsCallback(callback), false);
    }

    @Override
    public void tryNext(final SingleResultCallback<List<T>> callback) {
        resumeableOperation(new AsyncBlock() {
            @Override
            public void apply(final AsyncAggregateResponseBatchCursor<RawBsonDocument> cursor,
                              final SingleResultCallback<List<RawBsonDocument>> callback) {
                cursor.tryNext(callback);
            }
        }, convertResultsCallback(callback), true);
    }

    @Override
    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            try {
                nullifyAndCloseWrapped();
            } finally {
                binding.release();
            }
        }
    }

    @Override
    public void setBatchSize(final int batchSize) {
        getWrapped().setBatchSize(batchSize);
    }

    @Override
    public int getBatchSize() {
        return getWrapped().getBatchSize();
    }

    @Override
    public boolean isClosed() {
        if (isClosed.get()) {
            return true;
        } else if (wrappedClosedItself()) {
            close();
            return true;
        } else {
            return false;
        }
    }

    private boolean wrappedClosedItself() {
        AsyncAggregateResponseBatchCursor<RawBsonDocument> observedWrapped = wrapped.get();
        return observedWrapped != null && observedWrapped.isClosed();
    }

    /**
     * {@code null} is written to {@link #wrapped} before closing the wrapped object to maintain the following guarantee:
     * if {@link #wrappedClosedItself()} observes a {@linkplain AsyncAggregateResponseBatchCursor#isClosed() closed} wrapped object,
     * then it closed itself as opposed to being closed by {@code this}.
     */
    private void nullifyAndCloseWrapped() {
        AsyncAggregateResponseBatchCursor<RawBsonDocument> observedWrapped = wrapped.getAndSet(null);
        if (observedWrapped != null) {
            observedWrapped.close();
        }
    }

    /**
     * This method guarantees that the {@code newValue} argument is closed even if
     * {@link #setWrappedOrCloseIt(AsyncAggregateResponseBatchCursor)} is called concurrently with or after (in the happens-before order)
     * the method {@link #close()}.
     */
    private void setWrappedOrCloseIt(final AsyncAggregateResponseBatchCursor<RawBsonDocument> newValue) {
        if (isClosed()) {
            assertNull(this.wrapped.get());
            newValue.close();
        } else {
            assertNull(this.wrapped.getAndSet(newValue));
            if (isClosed()) {
                nullifyAndCloseWrapped();
            }
        }
    }

    @Override
    public BsonDocument getPostBatchResumeToken() {
        return getWrapped().getPostBatchResumeToken();
    }

    @Override
    public BsonTimestamp getOperationTime() {
        return changeStreamOperation.getStartAtOperationTime();
    }

    @Override
    public boolean isFirstBatchEmpty() {
        return getWrapped().isFirstBatchEmpty();
    }

    @Override
    public int getMaxWireVersion() {
        return maxWireVersion;
    }

    private void cachePostBatchResumeToken(final AsyncAggregateResponseBatchCursor<RawBsonDocument> queryBatchCursor) {
        BsonDocument resumeToken = queryBatchCursor.getPostBatchResumeToken();
        if (resumeToken != null) {
            this.resumeToken = resumeToken;
        }
    }

    private SingleResultCallback<List<RawBsonDocument>> convertResultsCallback(final SingleResultCallback<List<T>> callback) {
        return errorHandlingCallback(new SingleResultCallback<List<RawBsonDocument>>() {
            @Override
            public void onResult(final List<RawBsonDocument> rawDocuments, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else if (rawDocuments != null) {
                    List<T> results = new ArrayList<T>();
                    for (RawBsonDocument rawDocument : rawDocuments) {
                        if (!rawDocument.containsKey("_id")) {
                            callback.onResult(null,
                                    new MongoChangeStreamException("Cannot provide resume functionality when the resume token is missing.")
                            );
                            return;
                        }
                        try {
                            results.add(rawDocument.decode(changeStreamOperation.getDecoder()));
                        } catch (Exception e) {
                            callback.onResult(null, e);
                            return;
                        }
                    }
                    resumeToken = rawDocuments.get(rawDocuments.size() - 1).getDocument("_id");
                    callback.onResult(results, null);
                } else {
                    callback.onResult(null, null);
                }
            }
        }, LOGGER);
    }

    private interface AsyncBlock {
        void apply(AsyncAggregateResponseBatchCursor<RawBsonDocument> cursor, SingleResultCallback<List<RawBsonDocument>> callback);
    }

    private void resumeableOperation(final AsyncBlock asyncBlock, final SingleResultCallback<List<RawBsonDocument>> convertResultsCallback,
                                     final boolean tryNext) {
        if (isClosed()) {
            convertResultsCallback.onResult(null, new MongoException(format("%s called after the cursor was closed.",
                    tryNext ? "tryNext()" : "next()")));
            return;
        }
        AsyncAggregateResponseBatchCursor<RawBsonDocument> wrappedCursor = getWrapped();
        asyncBlock.apply(wrappedCursor, new SingleResultCallback<List<RawBsonDocument>>() {
            @Override
            public void onResult(final List<RawBsonDocument> result, final Throwable t) {
                boolean retry = false;
                if (t == null) {
                    convertResultsCallback.onResult(result, null);
                } else if (isRetryableError(t, maxWireVersion)) {
                    retry = true;
                } else {
                    convertResultsCallback.onResult(null, t);
                }
                cachePostBatchResumeToken(wrappedCursor);
                if (retry) {
                    nullifyAndCloseWrapped();
                    retryOperation(asyncBlock, convertResultsCallback, tryNext);
                }
            }
        });
    }

    private void retryOperation(final AsyncBlock asyncBlock, final SingleResultCallback<List<RawBsonDocument>> callback,
                                final boolean tryNext) {
        withAsyncReadConnection(binding, new AsyncCallableWithSource() {
            @Override
            public void call(final AsyncConnectionSource source, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    changeStreamOperation.setChangeStreamOptionsForResume(resumeToken, source.getServerDescription().getMaxWireVersion());
                    source.release();
                    changeStreamOperation.executeAsync(binding, new SingleResultCallback<AsyncBatchCursor<T>>() {
                        @Override
                        public void onResult(final AsyncBatchCursor<T> result, final Throwable t) {
                            if (t != null) {
                                callback.onResult(null, t);
                            } else {
                                try {
                                    setWrappedOrCloseIt(((AsyncChangeStreamBatchCursor<T>) result).getWrapped());
                                } finally {
                                    try {
                                        binding.release(); // release the new change stream batch cursor's reference to the binding
                                    } finally {
                                        resumeableOperation(asyncBlock, callback, tryNext);
                                    }
                                }
                            }
                        }
                    });
                }
            }
        });
    }
}
