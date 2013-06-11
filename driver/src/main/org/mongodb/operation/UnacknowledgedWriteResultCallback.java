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

package org.mongodb.operation;

import org.bson.io.OutputBuffer;
import org.mongodb.MongoException;
import org.mongodb.MongoNamespace;
import org.mongodb.connection.AsyncServerConnection;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.operation.protocol.RequestMessage;

class UnacknowledgedWriteResultCallback implements SingleResultCallback<ResponseBuffers> {
    private final SingleResultCallback<CommandResult> callback;
    private final BaseWrite writeOperation;
    private final MongoNamespace namespace;
    private final RequestMessage nextMessage;
    private final AsyncServerConnection connection;
    private final OutputBuffer writtenBuffer;
    private final BufferProvider bufferProvider;

    UnacknowledgedWriteResultCallback(final SingleResultCallback<CommandResult> callback, final BaseWrite writeOperation,
                                      final MongoNamespace namespace, final RequestMessage nextMessage,
                                      final AsyncServerConnection connection, final OutputBuffer writtenBuffer,
                                      final BufferProvider bufferProvider) {
        this.callback = callback;
        this.writeOperation = writeOperation;
        this.namespace = namespace;
        this.nextMessage = nextMessage;
        this.connection = connection;
        this.writtenBuffer = writtenBuffer;
        this.bufferProvider = bufferProvider;
    }

    @Override
    public void onResult(final ResponseBuffers result, final MongoException e) {
        writtenBuffer.close();
        if (e != null) {
            callback.onResult(null, e);
        }
        else if (nextMessage != null) {
            new GenericAsyncWriteOperation(namespace, writeOperation, nextMessage, bufferProvider).execute(connection, callback);
        }
        else {
            callback.onResult(null, null);
        }
    }
}
