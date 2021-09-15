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

package com.mongodb.reactivestreams.client.internal;

import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.operation.AsyncReadOperation;
import com.mongodb.internal.operation.AsyncWriteOperation;

class VoidWriteOperationThenCursorReadOperation<T> implements AsyncReadOperation<AsyncBatchCursor<T>> {
    private final AsyncWriteOperation<Void> writeOperation;
    private final AsyncReadOperation<AsyncBatchCursor<T>> cursorReadOperation;

    VoidWriteOperationThenCursorReadOperation(final AsyncWriteOperation<Void> writeOperation,
                                              final AsyncReadOperation<AsyncBatchCursor<T>> cursorReadOperation) {
        this.writeOperation = writeOperation;
        this.cursorReadOperation = cursorReadOperation;
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        writeOperation.executeAsync((AsyncWriteBinding) binding, (result, t) -> {
            if (t != null) {
                callback.onResult(null, t);
            } else {
                cursorReadOperation.executeAsync(binding, callback);
            }
        });
    }
}
