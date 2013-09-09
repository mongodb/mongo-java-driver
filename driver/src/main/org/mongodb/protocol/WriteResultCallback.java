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

import org.mongodb.CommandResult;
import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.MongoCommandFailureException;
import org.mongodb.MongoException;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.WriteConcern;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ServerDescription;
import org.mongodb.operation.SingleResultFuture;
import org.mongodb.operation.SingleResultFutureCallback;
import org.mongodb.protocol.message.RequestMessage;

import static org.mongodb.command.GetLastError.getCommandException;

class WriteResultCallback extends CommandResultBaseCallback {
    private final SingleResultFuture<CommandResult> future;
    private final MongoNamespace namespace;
    private final RequestMessage nextMessage; // only used for batch inserts that need to be split into multiple messages
    private final BufferProvider bufferProvider;
    private final WriteConcern writeConcern;
    private final ServerDescription serverDescription;
    private final Connection connection;
    private boolean closeConnection;

    public WriteResultCallback(final SingleResultFuture<CommandResult> future,
                               final Decoder<Document> decoder, final MongoNamespace namespace,
                               final RequestMessage nextMessage,
                               final WriteConcern writeConcern, final long requestId, final BufferProvider bufferProvider,
                               final ServerDescription serverDescription, final Connection connection, final boolean closeConnection) {
        super(decoder, requestId, connection, closeConnection);
        this.future = future;
        this.namespace = namespace;
        this.nextMessage = nextMessage;
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
        }
        else if (!commandResult.isOk()) {
            future.init(null, new MongoCommandFailureException(commandResult));
        }
        else {
            MongoCommandFailureException commandException = getCommandException(commandResult);

            if (commandException != null) {
                future.init(null, commandException);
            }
            else if (nextMessage != null) {
                MongoFuture<CommandResult> newFuture = new GenericWriteProtocol(namespace, bufferProvider, nextMessage,
                        writeConcern, serverDescription, connection, closeConnection).executeAsync();
                newFuture.register(new SingleResultFutureCallback<CommandResult>(future));
                done = false;
            }
            else {
                future.init(commandResult, null);
            }
        }
        return done;
    }
}
