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
import org.mongodb.CommandResult;
import org.mongodb.MongoException;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.connection.AsyncServerConnection;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.operation.protocol.RequestMessage;

class UnacknowledgedWriteResultCallback implements SingleResultCallback<Void> {
    private final SingleResultFuture<CommandResult> future;
    private final BaseWrite writeOperation;
    private final MongoNamespace namespace;
    private final RequestMessage nextMessage;
    private final AsyncServerConnection connection;
    private final OutputBuffer writtenBuffer;
    private final BufferProvider bufferProvider;

    UnacknowledgedWriteResultCallback(final SingleResultFuture<CommandResult> future, final BaseWrite writeOperation,
                                      final MongoNamespace namespace, final RequestMessage nextMessage,
                                      final AsyncServerConnection connection, final OutputBuffer writtenBuffer,
                                      final BufferProvider bufferProvider) {
        this.future = future;
        this.writeOperation = writeOperation;
        this.namespace = namespace;
        this.nextMessage = nextMessage;
        this.connection = connection;
        this.writtenBuffer = writtenBuffer;
        this.bufferProvider = bufferProvider;
    }

    @Override
    public void onResult(final Void result, final MongoException e) {
        writtenBuffer.close();
        if (e != null) {
            future.init(null, e);
        }
        else if (nextMessage != null) {
            MongoFuture<CommandResult> newFuture = new GenericAsyncWriteOperation(namespace, writeOperation, nextMessage,
                    bufferProvider).execute(connection);
            newFuture.register(new SingleResultFutureCallback<CommandResult>(future));
        }
        else {
            future.init(null, null);
        }
    }
}
