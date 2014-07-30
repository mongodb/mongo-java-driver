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

package com.mongodb.connection;

import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.operation.SingleResultFuture;

class FutureAsyncCompletionHandler<T> implements AsyncCompletionHandler<T> {
    private final SingleResultFuture<T> future;   // TODO: Move Future classes to org.mongodb

    public FutureAsyncCompletionHandler(final SingleResultFuture<T> future) {
        this.future = future;
    }

    @Override
    public void completed(final T t) {
        future.init(t, null);
    }

    @Override
    public void failed(final Throwable t) {
        if (t instanceof MongoException) {
            future.init(null, (MongoException) t);
        } else {
            future.init(null, new MongoInternalException("Unexpected exception", t));
        }
    }
}
