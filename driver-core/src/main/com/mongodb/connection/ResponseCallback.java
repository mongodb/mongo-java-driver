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

import com.mongodb.MongoInternalException;
import com.mongodb.ServerAddress;
import com.mongodb.async.SingleResultCallback;

abstract class ResponseCallback implements SingleResultCallback<ResponseBuffers> {
    private volatile boolean closed;
    private final ServerAddress serverAddress;
    private final long requestId;

    ResponseCallback(final long requestId, final ServerAddress serverAddress) {
        this.serverAddress = serverAddress;
        this.requestId = requestId;
    }

    protected ServerAddress getServerAddress() {
        return serverAddress;
    }

    protected long getRequestId() {
        return requestId;
    }

    @Override
    public void onResult(final ResponseBuffers responseBuffers, final Throwable t) {
        if (closed) {
            throw new MongoInternalException("Callback should not be invoked more than once");
        }
        closed = true;
        if (responseBuffers != null) {
            callCallback(responseBuffers, t);
        } else {
            callCallback(null, t);
        }
    }

    protected abstract void callCallback(ResponseBuffers responseBuffers, Throwable t);
}
