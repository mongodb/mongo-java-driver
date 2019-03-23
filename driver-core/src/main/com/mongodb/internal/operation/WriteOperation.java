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

import com.mongodb.binding.WriteBinding;

/**
 * An operation which writes to a MongoDB server.
 *
 * @param <T> the operations result type.
 *
 * @since 3.0
 */
@Deprecated
public interface WriteOperation<T> {
    /**
     * General execute which can return anything of type T
     *
     * @param binding the binding to execute in the context of
     * @return T, the result of the execution
     */
    T execute(WriteBinding binding);
}
