/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import com.mongodb.session.ClientSession;
import com.mongodb.ReadPreference;
import com.mongodb.async.SingleResultCallback;

/**
 * An interface describing the execution of a read or a write operation.
 *
 * @since 3.0
 */
public interface AsyncOperationExecutor {
    /**
     * Execute the read operation with the given read preference.
     *
     * @param operation the read operation.
     * @param readPreference the read preference.
     * @param callback the callback to be called when the operation has been executed
     * @param <T> the operations result type.
     */
    <T> void execute(AsyncReadOperation<T> operation, ReadPreference readPreference, SingleResultCallback<T> callback);

    /**
     * Execute the read operation with the given read preference.
     *
     * @param operation the read operation.
     * @param readPreference the read preference.
     * @param session the session to associate this operation with
     * @param callback the callback to be called when the operation has been executed
     * @param <T> the operations result type.
     * @since 3.6
     */
    <T> void execute(AsyncReadOperation<T> operation, ReadPreference readPreference, ClientSession session,
                     SingleResultCallback<T> callback);

    /**
     * Execute the write operation.
     *
     * @param operation the write operation.
     * @param callback the callback to be called when the operation has been executed
     * @param <T> the operations result type.
     */
    <T> void execute(AsyncWriteOperation<T> operation, SingleResultCallback<T> callback);

    /**
     * Execute the write operation.
     *
     * @param operation the write operation.
     * @param session the session to associate this operation with
     * @param callback the callback to be called when the operation has been executed
     * @param <T> the operations result type.
     * @since 3.6
     */
    <T> void execute(AsyncWriteOperation<T> operation, ClientSession session, SingleResultCallback<T> callback);
}
