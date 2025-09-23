package com.mongodb.internal.operation;

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


import com.mongodb.MongoException;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.async.AsyncAggregateResponseBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.lang.NonNull;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.AsyncOperationHelper.withAsyncReadConnectionSource;
import static com.mongodb.internal.operation.ChangeStreamBatchCursor.convertAndProduceLastId;
import static com.mongodb.internal.operation.ChangeStreamBatchCursorHelper.isResumableError;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static java.lang.String.format;

final class AsyncChangeStreamBatchCursor<T> implements AsyncAggregateResponseBatchCursor<T> {
    private final AsyncReadBinding binding;
    /**
     * The initial operation context, which is used as an initial state to create new operation contexts for each operation.
     */
    private final OperationContext initialOperationContext;
    private final ChangeStreamOperation<T> changeStreamOperation;
    private final int maxWireVersion;

    private volatile BsonDocument resumeToken;
    /**
     * {@linkplain ChangeStreamBatchCursorHelper#isResumableError(Throwable, int) Retryable errors} can result in
     * {@code wrapped} containing {@code null} and {@link #isClosed} being {@code false}.
     * This represents a situation in which the wrapped object was closed by {@code this} but {@code this} remained open.
     */
    private final AtomicReference<AsyncCursor<RawBsonDocument>> wrapped;
    private final AtomicBoolean isClosed;

    AsyncChangeStreamBatchCursor(final ChangeStreamOperation<T> changeStreamOperation,
                                 final AsyncCursor<RawBsonDocument> wrapped,
                                 final AsyncReadBinding binding,
                                 final OperationContext operationContext,
                                 @Nullable final BsonDocument resumeToken,
                                 final int maxWireVersion) {
        this.changeStreamOperation = changeStreamOperation;
        this.wrapped = new AtomicReference<>(assertNotNull(wrapped));
        this.binding = binding;
        binding.retain();
        this.initialOperationContext = initialOperationContext.withOverride(TimeoutContext::withMaxTimeAsMaxAwaitTimeOverride);
        this.resumeToken = resumeToken;
        this.maxWireVersion = maxWireVersion;
        isClosed = new AtomicBoolean();
    }

    @NonNull
    AsyncCursor<RawBsonDocument> getWrapped() {
        return assertNotNull(wrapped.get());
    }

    @Override
    public void next(final SingleResultCallback<List<T>> callback) {
        OperationContext operationContext = initialOperationContext.withNewlyStartedTimeout();
        resumeableOperation(AsyncCursor::next, callback, operationContext, false);
    }

    @Override
    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            try {
                nullifyAndCloseWrapped(initialOperationContext.withNewlyStartedTimeout());
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
        AsyncCursor<RawBsonDocument> observedWrapped = wrapped.get();
        return observedWrapped != null && observedWrapped.isClosed();
    }

    /**
     * {@code null} is written to {@link #wrapped} before closing the wrapped object to maintain the following guarantee:
     * if {@link #wrappedClosedItself()} observes a {@linkplain AsyncAggregateResponseBatchCursor#isClosed() closed} wrapped object,
     * then it closed itself as opposed to being closed by {@code this}.
     */
    private void nullifyAndCloseWrapped(final OperationContext operationContext) {
        AsyncCursor<RawBsonDocument> observedWrapped = wrapped.getAndSet(null);
        if (observedWrapped != null) {
            observedWrapped.close(operationContext);
        }
    }

    /**
     * This method guarantees that the {@code newValue} argument is closed even if
     * {@code setWrappedOrCloseIt(AsyncCommandBatchCursor)} is called concurrently with or after (in the happens-before order)
     * the method {@link #close()}.
     */
    private void setWrappedOrCloseIt(final AsyncCursor<RawBsonDocument> newValue, final OperationContext operationContext) {
        if (isClosed()) {
            assertNull(wrapped.get());
            newValue.close(operationContext);
        } else {
            assertNull(wrapped.getAndSet(newValue));
            if (isClosed()) {
                nullifyAndCloseWrapped(operationContext);
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

    private void cachePostBatchResumeToken(final AsyncCursor<RawBsonDocument> cursor) {
        BsonDocument resumeToken = cursor.getPostBatchResumeToken();
        if (resumeToken != null) {
            this.resumeToken = resumeToken;
        }
    }

    private interface AsyncBlock {
        void apply(AsyncCursor<RawBsonDocument> cursor, OperationContext operationContext,
                   SingleResultCallback<List<RawBsonDocument>> callback);
    }

    private void resumeableOperation(final AsyncBlock asyncBlock,
                                     final SingleResultCallback<List<T>> callback,
                                     final OperationContext operationContext,
                                     final boolean tryNext) {
        SingleResultCallback<List<T>> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
        if (isClosed()) {
            errHandlingCallback.onResult(null, new MongoException(format("%s called after the cursor was closed.",
                    tryNext ? "tryNext()" : "next()")));
            return;
        }
        AsyncCursor<RawBsonDocument> wrappedCursor = getWrapped();
        asyncBlock.apply(wrappedCursor, operationContext, (result, t) -> {
            if (t == null) {
                try {
                    List<T> convertedResults;
                    try {
                        convertedResults = convertAndProduceLastId(assertNotNull(result), changeStreamOperation.getDecoder(),
                                lastId -> resumeToken = lastId);
                    } finally {
                        cachePostBatchResumeToken(wrappedCursor);
                    }
                    errHandlingCallback.onResult(convertedResults, null);
                } catch (Exception e) {
                    errHandlingCallback.onResult(null, e);
                }
            } else {
                cachePostBatchResumeToken(wrappedCursor);
                if (isResumableError(t, maxWireVersion)) {
                    nullifyAndCloseWrapped(operationContext);
                    retryOperation(asyncBlock, errHandlingCallback, operationContext, tryNext);
                } else {
                    errHandlingCallback.onResult(null, t);
                }
            }
        });
    }

    private void retryOperation(final AsyncBlock asyncBlock,
                                final SingleResultCallback<List<T>> callback,
                                final OperationContext operationContext,
                                final boolean tryNext) {
        withAsyncReadConnectionSource(binding, operationContext, (source, t) -> {
            if (t != null) {
                callback.onResult(null, t);
            } else {
                changeStreamOperation.setChangeStreamOptionsForResume(resumeToken,
                        assertNotNull(source).getServerDescription().getMaxWireVersion());
                source.release();
                changeStreamOperation.executeAsync(binding, operationContext, (asyncBatchCursor, t1) -> {
                    if (t1 != null) {
                        callback.onResult(null, t1);
                    } else {
                        try {
                            setWrappedOrCloseIt(assertNotNull((AsyncChangeStreamBatchCursor<T>) asyncBatchCursor).getWrapped(),
                                    operationContext);
                        } finally {
                            try {
                                binding.release(); // release the new change stream batch cursor's reference to the binding
                            } finally {
                                resumeableOperation(asyncBlock, callback, operationContext, tryNext);
                            }
                        }
                    }
                });
            }
        });
    }
}

