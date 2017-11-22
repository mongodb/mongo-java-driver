/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.async.client;

import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.AsyncWriteBinding;
import com.mongodb.operation.AsyncReadOperation;
import com.mongodb.operation.AsyncWriteOperation;
import com.mongodb.operation.FindOperation;

class AggregateToCollectionThenFindOperation<T> implements AsyncReadOperation<AsyncBatchCursor<T>> {
    private final AsyncWriteOperation<Void> aggregateToCollectionOperation;
    private final FindOperation<T> findOperation;

    AggregateToCollectionThenFindOperation(final AsyncWriteOperation<Void> aggregateToCollectionOperation,
                                           final FindOperation<T> findOperation) {
        this.aggregateToCollectionOperation = aggregateToCollectionOperation;
        this.findOperation = findOperation;
    }

    public AsyncWriteOperation<Void> getAggregateToCollectionOperation() {
        return aggregateToCollectionOperation;
    }

    public FindOperation<T> getFindOperation() {
        return findOperation;
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
                    findOperation.executeAsync(binding, callback);
                }
            }
        });
    }
}
