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

package com.mongodb.internal.async.client;

import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.operation.AsyncReadOperation;
import com.mongodb.internal.operation.AsyncWriteOperation;

class WriteOperationThenCursorReadOperation<T> implements AsyncReadOperation<AsyncBatchCursor<T>> {
    private final AsyncWriteOperation<Void> aggregateToCollectionOperation;
    private final AsyncReadOperation<AsyncBatchCursor<T>> readOperation;

    WriteOperationThenCursorReadOperation(final AsyncWriteOperation<Void> aggregateToCollectionOperation,
                                          final AsyncReadOperation<AsyncBatchCursor<T>> readOperation) {
        this.aggregateToCollectionOperation = aggregateToCollectionOperation;
        this.readOperation = readOperation;
    }

    public AsyncWriteOperation<Void> getAggregateToCollectionOperation() {
        return aggregateToCollectionOperation;
    }

    public AsyncReadOperation<AsyncBatchCursor<T>> getReadOperation() {
        return readOperation;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        aggregateToCollectionOperation.executeAsync((AsyncWriteBinding) binding, new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    readOperation.executeAsync(binding, callback);
                }
            }
        });
    }
}
