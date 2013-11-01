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

package org.mongodb.protocol;

import org.mongodb.CommandResult;
import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.MongoException;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.WriteConcern;
import org.mongodb.WriteResult;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ServerDescription;
import org.mongodb.operation.SingleResultFuture;
import org.mongodb.operation.SingleResultFutureCallback;
import org.mongodb.protocol.message.RequestMessage;

class WriteResultCallback extends CommandResultBaseCallback {
    private final SingleResultFuture<WriteResult> future;
    private final MongoNamespace namespace;
    private final RequestMessage nextMessage; // only used for batch inserts that need to be split into multiple messages
    private final BufferProvider bufferProvider;
    private boolean ordered;
    private final WriteConcern writeConcern;
    private final ServerDescription serverDescription;
    private final Connection connection;
    private final boolean closeConnection;

    // CHECKSTYLE:OFF
    public WriteResultCallback(final SingleResultFuture<WriteResult> future, final Decoder<Document> decoder,
                               final MongoNamespace namespace, final RequestMessage nextMessage,
                               final boolean ordered, final WriteConcern writeConcern, final long requestId,
                               final BufferProvider bufferProvider, final ServerDescription serverDescription,
                               final Connection connection, final boolean closeConnection) {
        // CHECKSTYLE:ON
        super(decoder, requestId, connection, closeConnection);
        this.future = future;
        this.namespace = namespace;
        this.nextMessage = nextMessage;
        this.ordered = ordered;
        this.writeConcern = writeConcern;
        this.bufferProvider = bufferProvider;
        this.serverDescription = serverDescription;
        this.connection = connection;
        this.closeConnection = closeConnection;
    }

    @Override
    protected boolean callCallback(final CommandResult commandResult, final MongoException e) {
        boolean done = true;
        if (e != null) {
            future.init(null, e);
        } else {
            try {
                WriteResult writeResult = ProtocolHelper.getWriteResult(commandResult);
                if (nextMessage != null) {
                    MongoFuture<WriteResult> newFuture = new GenericWriteProtocol(namespace,
                                                                                  bufferProvider,
                                                                                  nextMessage,
                                                                                  ordered,
                                                                                  writeConcern,
                                                                                  serverDescription,
                                                                                  connection,
                                                                                  closeConnection).executeAsync();
                    newFuture.register(new SingleResultFutureCallback<WriteResult>(future));
                    done = false;
                } else {
                    future.init(writeResult, null);
                }
            } catch (MongoException writeException) {
                future.init(null, writeException);
            }
        }
        return done;
    }
}
