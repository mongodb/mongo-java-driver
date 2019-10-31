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

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.operation.AsyncReadOperation;
import com.mongodb.internal.operation.AsyncWriteOperation;
import com.mongodb.lang.Nullable;

/**
 * An interface describing the execution of a read or a write operation.
 */
@SuppressWarnings("overloads")
interface OperationExecutor {
    /**
     * Execute the read operation with the given read preference.
     *
     * @param operation the read operation.
     * @param readPreference the read preference.
     * @param readConcern the read concern
     * @param callback the callback to be called when the operation has been executed
     * @param <T> the operations result type.
     */
    <T> void execute(AsyncReadOperation<T> operation, ReadPreference readPreference, ReadConcern readConcern,
                     SingleResultCallback<T> callback);

    /**
     * Execute the read operation with the given read preference.
     *
     * @param operation the read operation.
     * @param readPreference the read preference.
     * @param readConcern the read concern
     * @param session the session to associate this operation with
     * @param callback the callback to be called when the operation has been executed
     * @param <T> the operations result type.
     */
    <T> void execute(AsyncReadOperation<T> operation, ReadPreference readPreference, ReadConcern readConcern,
                     @Nullable AsyncClientSession session, SingleResultCallback<T> callback);

    /**
     * Execute the write operation.
     *
     * @param operation the write operation.
     * @param readConcern the read concern
     * @param callback the callback to be called when the operation has been executed
     * @param <T> the operations result type.
     */
    <T> void execute(AsyncWriteOperation<T> operation, ReadConcern readConcern, SingleResultCallback<T> callback);

    /**
     * Execute the write operation.
     *
     * @param operation the write operation.
     * @param session the session to associate this operation with
     * @param readConcern the read concern
     * @param callback the callback to be called when the operation has been executed
     * @param <T> the operations result type.
     */
    <T> void execute(AsyncWriteOperation<T> operation, ReadConcern readConcern, @Nullable AsyncClientSession session,
                     SingleResultCallback<T> callback);
}
