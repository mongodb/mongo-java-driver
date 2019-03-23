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
import com.mongodb.async.AsyncAggregateResponseBatchCursor;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncConnectionSource;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.internal.operation.OperationHelper.AsyncCallableWithSource;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.ChangeStreamBatchCursorHelper.isRetryableError;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.OperationHelper.withAsyncReadConnection;

final class AsyncChangeStreamBatchCursor<T> implements AsyncAggregateResponseBatchCursor<T> {
    private final AsyncReadBinding binding;
    private final ChangeStreamOperation<T> changeStreamOperation;

    private volatile BsonDocument resumeToken;
    private volatile AsyncAggregateResponseBatchCursor<RawBsonDocument> wrapped;

    AsyncChangeStreamBatchCursor(final ChangeStreamOperation<T> changeStreamOperation,
                                 final AsyncAggregateResponseBatchCursor<RawBsonDocument> wrapped,
                                 final AsyncReadBinding binding,
                                 final BsonDocument resumeToken) {
        this.changeStreamOperation = changeStreamOperation;
        this.wrapped = wrapped;
        this.binding = binding;
        binding.retain();
        this.resumeToken = resumeToken;
    }

    AsyncAggregateResponseBatchCursor<RawBsonDocument> getWrapped() {
        return wrapped;
    }

    @Override
    public void next(final SingleResultCallback<List<T>> callback) {
        resumeableOperation(new AsyncBlock() {
            @Override
            public void apply(final AsyncAggregateResponseBatchCursor<RawBsonDocument> cursor,
                              final SingleResultCallback<List<RawBsonDocument>> callback) {
                cursor.next(callback);
                cachePostBatchResumeToken(cursor);
            }
        }, convertResultsCallback(callback));
    }

    @Override
    public void tryNext(final SingleResultCallback<List<T>> callback) {
        resumeableOperation(new AsyncBlock() {
            @Override
            public void apply(final AsyncAggregateResponseBatchCursor<RawBsonDocument> cursor,
                              final SingleResultCallback<List<RawBsonDocument>> callback) {
                cursor.tryNext(callback);
                cachePostBatchResumeToken(cursor);
            }
        }, convertResultsCallback(callback));
    }

    @Override
    public void close() {
        if (!isClosed()) {
            wrapped.close();
            binding.release();
        }
    }

    @Override
    public void setBatchSize(final int batchSize) {
        wrapped.setBatchSize(batchSize);
    }

    @Override
    public int getBatchSize() {
        return wrapped.getBatchSize();
    }

    @Override
    public boolean isClosed() {
        return wrapped.isClosed();
    }

    @Override
    public BsonDocument getPostBatchResumeToken() {
        return wrapped.getPostBatchResumeToken();
    }

    @Override
    public BsonTimestamp getOperationTime() {
        return changeStreamOperation.getStartAtOperationTime();
    }

    @Override
    public boolean isFirstBatchEmpty() {
        return wrapped.isFirstBatchEmpty();
    }

    private void cachePostBatchResumeToken(final AsyncAggregateResponseBatchCursor<RawBsonDocument> queryBatchCursor) {
        if (queryBatchCursor.getPostBatchResumeToken() != null) {
            resumeToken = queryBatchCursor.getPostBatchResumeToken();
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
                        results.add(rawDocument.decode(changeStreamOperation.getDecoder()));
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

    private void resumeableOperation(final AsyncBlock asyncBlock, final SingleResultCallback<List<RawBsonDocument>> callback) {
        asyncBlock.apply(wrapped, new SingleResultCallback<List<RawBsonDocument>>() {
            @Override
            public void onResult(final List<RawBsonDocument> result, final Throwable t) {
                if (t == null) {
                    callback.onResult(result, null);
                } else if (isRetryableError(t)) {
                    wrapped.close();
                    retryOperation(asyncBlock, callback);
                } else {
                    callback.onResult(null, t);
                }
            }
        });
    }

    private void retryOperation(final AsyncBlock asyncBlock, final SingleResultCallback<List<RawBsonDocument>> callback) {
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
                                wrapped = ((AsyncChangeStreamBatchCursor<T>) result).getWrapped();
                                binding.release(); // release the new change stream batch cursor's reference to the binding
                                resumeableOperation(asyncBlock, callback);
                            }
                        }
                    });
                }
            }
        });
    }
}
