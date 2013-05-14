/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.async;

import org.mongodb.MongoException;
import org.mongodb.impl.AsyncConnection;

class ConnectionClosingSingleResultCallback<T> implements SingleResultCallback<T> {
    private final AsyncConnection connection;
    private final SingleResultFuture<T> retVal;

    public ConnectionClosingSingleResultCallback(final AsyncConnection connection, final SingleResultFuture<T> retVal) {
        this.connection = connection;
        this.retVal = retVal;
    }

    @Override
    public void onResult(final T result, final MongoException e) {
        connection.close();
        retVal.init(result, e);
    }
}
