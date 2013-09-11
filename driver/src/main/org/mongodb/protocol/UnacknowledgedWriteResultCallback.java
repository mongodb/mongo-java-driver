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

import org.bson.io.OutputBuffer;
import org.mongodb.MongoException;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.WriteConcern;
import org.mongodb.WriteResult;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ServerDescription;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.operation.SingleResultFuture;
import org.mongodb.operation.SingleResultFutureCallback;
import org.mongodb.protocol.message.RequestMessage;

class UnacknowledgedWriteResultCallback implements SingleResultCallback<Void> {
    private final SingleResultFuture<WriteResult> future;
    private final MongoNamespace namespace;
    private final RequestMessage nextMessage;
    private final OutputBuffer writtenBuffer;
    private final BufferProvider bufferProvider;
    private final ServerDescription serverDescription;
    private final Connection connection;
    private final boolean closeConnection;

    UnacknowledgedWriteResultCallback(final SingleResultFuture<WriteResult> future,
                                      final MongoNamespace namespace, final RequestMessage nextMessage,
                                      final OutputBuffer writtenBuffer, final BufferProvider bufferProvider,
                                      final ServerDescription serverDescription, final Connection connection,
                                      final boolean closeConnection) {
        this.future = future;
        this.namespace = namespace;
        this.nextMessage = nextMessage;
        this.serverDescription = serverDescription;
        this.connection = connection;
        this.writtenBuffer = writtenBuffer;
        this.bufferProvider = bufferProvider;
        this.closeConnection = closeConnection;
    }

    @Override
    public void onResult(final Void result, final MongoException e) {
        writtenBuffer.close();
        if (e != null) {
            future.init(null, e);
        }
        else if (nextMessage != null) {
            MongoFuture<WriteResult> newFuture = new GenericWriteProtocol(namespace, bufferProvider, nextMessage,
                    WriteConcern.UNACKNOWLEDGED, serverDescription, connection, closeConnection).executeAsync();
            newFuture.register(new SingleResultFutureCallback<WriteResult>(future));
        }
        else {
            future.init(null, null);
        }
    }
}
