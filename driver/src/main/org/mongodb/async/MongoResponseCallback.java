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
import org.mongodb.MongoInternalException;
import org.mongodb.impl.MongoAsyncConnection;
import org.mongodb.io.ResponseBuffers;

public abstract class MongoResponseCallback implements SingleResultCallback<ResponseBuffers> {
    private volatile boolean closed;
    private MongoAsyncConnection connection;

    public MongoResponseCallback(final MongoAsyncConnection connection) {
        this.connection = connection;
        connection.startOperation();
    }

    protected MongoAsyncConnection getConnection() {
        return connection;
    }

    @Override
    public void onResult(final ResponseBuffers responseBuffers, final MongoException e) {
        if (closed) {
            throw new MongoInternalException("Callback should not be invoked more than once", null);
        }
        closed = true;
        connection.operationCompleted();
        if (responseBuffers != null) {
            callCallback(responseBuffers, e);
        }
        else {
            callCallback(null, e);
        }
    }

    protected abstract void callCallback(ResponseBuffers responseBuffers, MongoException e);
}
