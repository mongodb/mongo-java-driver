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

package org.mongodb.protocol;

import org.mongodb.MongoException;
import org.mongodb.MongoInternalException;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.SingleResultCallback;

abstract class ResponseCallback implements SingleResultCallback<ResponseBuffers> {
    private volatile boolean closed;
    private final Connection connection;
    private final long requestId;
    private final boolean closeConnection;

    public ResponseCallback(final long requestId, final Connection connection, final boolean closeConnection) {
        this.connection = connection;
        this.requestId = requestId;
        this.closeConnection = closeConnection;
    }

    protected Connection getConnection() {
        return connection;
    }

    protected long getRequestId() {
        return requestId;
    }

    @Override
    public void onResult(final ResponseBuffers responseBuffers, final MongoException e) {
        if (closed) {
            throw new MongoInternalException("Callback should not be invoked more than once", null);
        }
        closed = true;
        boolean done;
        if (responseBuffers != null) {
            done = callCallback(responseBuffers, e);
        }
        else {
            done = callCallback(null, e);
        }
        if (done && closeConnection) {
            connection.close();
        }
    }

    protected abstract boolean callCallback(ResponseBuffers responseBuffers, MongoException e);
}
