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

package com.mongodb.client.internal;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.ClientSession;
import com.mongodb.internal.operation.ReadOperation;
import com.mongodb.internal.operation.WriteOperation;
import com.mongodb.lang.Nullable;

/**
 * An interface describing the execution of a read or a write operation.
 *
 * This class is not part of the public API and may be removed or changed at any time.
 */
@SuppressWarnings("overloads")
public interface OperationExecutor {
    /**
     * Execute the read operation with the given read preference.
     *
     * @param <T> the operations result type.
     * @param operation the read operation.
     * @param readPreference the read preference.
     * @param readConcern the read concern
     * @return the result of executing the operation.
     */
    <T> T execute(ReadOperation<T> operation, ReadPreference readPreference, ReadConcern readConcern);

    /**
     * Execute the write operation.
     *
     * @param operation the write operation.
     * @param readConcern the read concern
     * @param <T> the operations result type.
     * @return the result of executing the operation.
     */
    <T> T execute(WriteOperation<T> operation, ReadConcern readConcern);

    /**
     * Execute the read operation with the given read preference.
     *
     * @param <T> the operations result type.
     * @param operation the read operation.
     * @param readPreference the read preference.
     * @param readConcern the read concern
     * @param session the session to associate this operation with
     * @return the result of executing the operation.
     */
    <T> T execute(ReadOperation<T> operation, ReadPreference readPreference, ReadConcern readConcern, @Nullable ClientSession session);

    /**
     * Execute the write operation.
     *
     * @param operation the write operation.
     * @param readConcern the read concern
     * @param session the session to associate this operation with
     * @param <T> the operations result type.
     * @return the result of executing the operation.
     */
    <T> T execute(WriteOperation<T> operation, ReadConcern readConcern, @Nullable ClientSession session);
}
