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

import com.mongodb.MongoNamespace;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncReadWriteBinding;
import com.mongodb.internal.binding.ReadWriteBinding;
import com.mongodb.internal.connection.OperationContext;

/**
 * A {@link WriteThenReadOperationCursor} that performs a {@link WriteOperation} returning
 * {@code Void} followed by a {@link ReadOperationCursor}, using a single read-write
 * binding for both phases.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class VoidWriteOperationThenCursorReadOperation<T> implements WriteThenReadOperationCursor<T> {
    private final WriteOperation<Void> writeOperation;
    private final ReadOperationCursor<T> cursorReadOperation;

    public VoidWriteOperationThenCursorReadOperation(final WriteOperation<Void> writeOperation,
                                                     final ReadOperationCursor<T> cursorReadOperation) {
        this.writeOperation = writeOperation;
        this.cursorReadOperation = cursorReadOperation;
    }

    public WriteOperation<Void> getWriteOperation() {
        return writeOperation;
    }

    public ReadOperationCursor<T> getCursorReadOperation() {
        return cursorReadOperation;
    }

    @Override
    public String getCommandName() {
        return writeOperation.getCommandName();
    }

    @Override
    public MongoNamespace getNamespace() {
        return writeOperation.getNamespace();
    }

    @Override
    public BatchCursor<T> execute(final ReadWriteBinding binding, final OperationContext operationContext) {
        writeOperation.execute(binding, operationContext);
        return cursorReadOperation.execute(binding, operationContext);
    }

    @Override
    public void executeAsync(final AsyncReadWriteBinding binding, final OperationContext operationContext,
                             final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        writeOperation.executeAsync(binding, operationContext, (result, t) -> {
            if (t != null) {
                callback.onResult(null, t);
            } else {
                cursorReadOperation.executeAsync(binding, operationContext, callback);
            }
        });
    }
}
