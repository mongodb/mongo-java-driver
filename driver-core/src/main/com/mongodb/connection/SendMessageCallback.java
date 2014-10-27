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
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.SingleResultFuture;
import org.bson.io.OutputBuffer;

class SendMessageCallback<T> implements SingleResultCallback<Void> {
    private final OutputBuffer buffer;
    private final InternalConnection connection;
    private final SingleResultCallback<ResponseBuffers> receiveMessageCallback;
    private final int requestId;
    private final SingleResultFuture<T> future;

    SendMessageCallback(final InternalConnection connection, final OutputBuffer buffer,
                        final int requestId, final SingleResultFuture<T> future,
                        final SingleResultCallback<ResponseBuffers> receiveMessageCallback) {
        this.buffer = buffer;
        this.connection = connection;
        this.future = future;
        this.receiveMessageCallback = receiveMessageCallback;
        this.requestId = requestId;
    }

    @Override
    public void onResult(final Void result, final MongoException e) {
        buffer.close();
        if (e != null) {
            future.init(null, e);
        } else {
            connection.receiveMessageAsync(requestId, receiveMessageCallback);
        }
    }
}
