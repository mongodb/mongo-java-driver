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

package com.mongodb.async;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A future that also supports registration of callbacks that are executed on completion of the future.
 *
 * @param <T> the type of the future
 * @since 3.0
 */
public interface MongoFuture<T> extends Future<T> {

    @Override
    T get();

    @Override
    T get(long timeout, TimeUnit unit) throws TimeoutException;

    /**
     * Register a callback to be executed when the future completes.
     *
     * @param newCallback the callback
     */
    void register(SingleResultCallback<T> newCallback);
}
