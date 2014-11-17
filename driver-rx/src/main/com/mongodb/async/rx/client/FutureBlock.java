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

package com.mongodb.async.rx.client;

import com.mongodb.async.MongoFuture;

/**
 * Apply a function yielding a future for an appropriate result object.
 *
 * @param <R> the type of result objects from the {@code apply} operation.
 */
interface FutureBlock<R> {
    /**
     * Yield an appropriate result object
     *
     * @return the function result
     */
    MongoFuture<R> apply();
}
