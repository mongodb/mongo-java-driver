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
import com.mongodb.internal.connection.OperationContext;

/**
 * An async-only operation that performs a write followed by a read that returns a cursor.
 *
 * <p>Unlike {@link ReadOperationCursor}, this operation requires an {@link AsyncReadWriteBinding}
 * so that both the write and the read portions can be executed without narrowing casts.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public interface AsyncWriteThenReadOperationCursor<T> {

    /**
     * @return the command name of the write phase of this operation (e.g. "mapReduce", "aggregate")
     */
    String getCommandName();

    /**
     * @return the namespace the write phase targets
     */
    MongoNamespace getNamespace();

    /**
     * Executes the write phase followed by the read phase, yielding an {@link AsyncBatchCursor}
     * over the results of the read.
     *
     * @param binding the read-write binding used by both phases
     * @param operationContext the operation context to use
     * @param callback receives the {@link AsyncBatchCursor} on success, or the failure of either phase
     */
    void executeAsync(AsyncReadWriteBinding binding, OperationContext operationContext,
                      SingleResultCallback<AsyncBatchCursor<T>> callback);
}
