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

package com.mongodb.connection;

import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.lang.Nullable;

/**
 * Completion handler for asynchronous I/O.
 *
 * @since 3.0
 * @param <T> the type of a successful completion
 */
public interface AsyncCompletionHandler<T> {
    /**
     * Invoked when an operation has completed.
     *
     * @param t the result of the completed operation
     */
    void completed(@Nullable T t);

    /**
     * Invoked when an operation fails.
     *
     * @param t the exception that describes the failure
     */
    void failed(Throwable t);

    /**
     * @return this handler as a callback
     */
    default SingleResultCallback<T> asCallback() {
        return (r, t) -> {
            if (t != null) {
                failed(t);
            } else {
                completed(r);
            }
        };
    }
}
