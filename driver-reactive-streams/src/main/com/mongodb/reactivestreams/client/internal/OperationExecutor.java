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

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.operation.ReadOperation;
import com.mongodb.internal.operation.WriteOperation;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ClientSession;
import reactor.core.publisher.Mono;

/**
 * An interface describing the execution of a read or a write operation.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public interface OperationExecutor {

    /**
     * Execute the read operation with the given read preference.
     *
     * @param operation the read operation.
     * @param readPreference the read preference.
     * @param readConcern the read concern
     * @param session the session to associate this operation with
     * @param <T> the operations result type.
     */
    <T> Mono<T> execute(ReadOperation<?, T> operation, ReadPreference readPreference, ReadConcern readConcern,
            @Nullable ClientSession session);

    /**
     * Execute the write operation.
     *
     * @param operation the write operation.
     * @param session the session to associate this operation with
     * @param readConcern the read concern
     * @param <T> the operations result type.
     */
    <T> Mono<T> execute(WriteOperation<T> operation, ReadConcern readConcern, @Nullable ClientSession session);

    /**
     * Create a new OperationExecutor with a specific timeout settings
     *
     * @param timeoutSettings the TimeoutContext to use for the operations
     * @return the new operation executor with the set timeout context
     * @since 5.2
     */
    OperationExecutor withTimeoutSettings(TimeoutSettings timeoutSettings);

    /**
     * Returns the current timeout settings
     *
     * @return the timeout settings
     * @since 5.2
     */
    TimeoutSettings getTimeoutSettings();
}
