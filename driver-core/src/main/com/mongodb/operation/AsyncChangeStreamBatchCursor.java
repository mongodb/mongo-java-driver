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

package com.mongodb.operation;

import com.mongodb.MongoChangeStreamException;
import com.mongodb.MongoCursorNotFoundException;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoSocketException;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncReadBinding;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.operation.OperationHelper.LOGGER;

final class AsyncChangeStreamBatchCursor<T> implements AsyncBatchCursor<T> {
    private final AsyncReadBinding binding;
    private final ChangeStreamOperation<T> changeStreamOperation;

    private volatile BsonDocument resumeToken;
    private volatile AsyncBatchCursor<RawBsonDocument> wrapped;

    AsyncChangeStreamBatchCursor(final ChangeStreamOperation<T> changeStreamOperation,
                                 final AsyncBatchCursor<RawBsonDocument> wrapped,
                                 final AsyncReadBinding binding) {
        this.changeStreamOperation = changeStreamOperation;
        this.resumeToken = changeStreamOperation.getResumeToken();
        this.wrapped = wrapped;
        this.binding = binding;
        binding.retain();
    }

    AsyncBatchCursor<RawBsonDocument> getWrapped() {
        return wrapped;
    }

    @Override
    public void next(final SingleResultCallback<List<T>> callback) {
        resumeableOperation(new AsyncBlock() {
            @Override
            public void apply(final AsyncBatchCursor<RawBsonDocument> cursor, final SingleResultCallback<List<RawBsonDocument>> callback) {
                cursor.next(callback);
            }
        }, convertResultsCallback(callback));
    }

    @Override
    public void tryNext(final SingleResultCallback<List<T>> callback) {
        resumeableOperation(new AsyncBlock() {
            @Override
            public void apply(final AsyncBatchCursor<RawBsonDocument> cursor, final SingleResultCallback<List<RawBsonDocument>> callback) {
                cursor.tryNext(callback);
            }
        }, convertResultsCallback(callback));
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
    public void close() {
        wrapped.close();
        binding.release();
    }

    private interface AsyncBlock {
        void apply(AsyncBatchCursor<RawBsonDocument> cursor, SingleResultCallback<List<RawBsonDocument>> callback);
    }

    private void resumeableOperation(final AsyncBlock asyncBlock, final SingleResultCallback<List<RawBsonDocument>> callback) {
        asyncBlock.apply(wrapped, new SingleResultCallback<List<RawBsonDocument>>() {
            @Override
            public void onResult(final List<RawBsonDocument> result, final Throwable t) {
                if (t == null) {
                    callback.onResult(result, null);
                } else if (t instanceof MongoNotPrimaryException
                        || t instanceof MongoCursorNotFoundException
                        || t instanceof MongoSocketException) {
                    wrapped.close();
                    retryOperation(asyncBlock, callback);
                } else {
                    callback.onResult(null, t);
                }
            }
        });
    }

    private void retryOperation(final AsyncBlock asyncBlock, final SingleResultCallback<List<RawBsonDocument>> callback) {
        changeStreamOperation.resumeAfter(resumeToken).executeAsync(binding, new SingleResultCallback<AsyncBatchCursor<T>>() {
            @Override
            public void onResult(final AsyncBatchCursor<T> result, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    wrapped = ((AsyncChangeStreamBatchCursor<T>) result).getWrapped();
                    binding.release(); // release the new change stream batch cursor's reference to the binding
                    asyncBlock.apply(wrapped, callback);
                }
            }
        });
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
                        resumeToken = rawDocument.getDocument("_id");
                        results.add(rawDocument.decode(changeStreamOperation.getDecoder()));
                    }
                    callback.onResult(results, null);
                } else {
                    callback.onResult(null, null);
                }
            }
        }, LOGGER);
    }
}
