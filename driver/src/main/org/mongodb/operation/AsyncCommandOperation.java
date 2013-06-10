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

import org.mongodb.Codec;
import org.mongodb.Document;
import org.mongodb.MongoException;
import org.mongodb.MongoNamespace;
import org.mongodb.command.Command;
import org.mongodb.connection.AsyncServerConnection;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.ClusterDescription;
import org.mongodb.connection.PooledByteBufferOutputBuffer;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.operation.protocol.CommandMessage;
import org.mongodb.session.AsyncServerSelectingSession;

import static org.mongodb.operation.OperationHelpers.encodeMessageToBuffer;
import static org.mongodb.operation.OperationHelpers.getMessageSettings;
import static org.mongodb.operation.OperationHelpers.getResponseSettings;

public class AsyncCommandOperation {

    private final Command command;
    private final Codec<Document> codec;
    private final MongoNamespace namespace;
    private final BufferProvider bufferProvider;

    public AsyncCommandOperation(final String database, final Command command, final Codec<Document> codec,
                                 final ClusterDescription clusterDescription, final BufferProvider bufferProvider) {
        this.namespace = new MongoNamespace(database, MongoNamespace.COMMAND_COLLECTION_NAME);
        this.bufferProvider = bufferProvider;
        command.readPreference(CommandReadPreferenceHelper.getCommandReadPreference(command, clusterDescription));
        this.command = command;
        this.codec = codec;
    }

    public Command getCommand() {
        return command;
    }

    public MongoFuture<CommandResult> execute(final AsyncServerSelectingSession session) {
        final SingleResultFuture<CommandResult> retVal = new SingleResultFuture<CommandResult>();

        session.getConnection(new ReadPreferenceServerSelector(command.getReadPreference()))
                .register(new SingleResultCallback<AsyncServerConnection>() {
                    @Override
                    public void onResult(final AsyncServerConnection connection, final MongoException e) {
                        if (e != null) {
                            retVal.init(null, e);
                        }
                        else {
                            MongoFuture<CommandResult> wrapped = execute(connection);
                            wrapped.register(new ConnectionClosingSingleResultCallback<CommandResult>(connection, retVal));
                        }
                    }
                });

        return retVal;
    }

    public MongoFuture<CommandResult> execute(final AsyncServerConnection connection) {
        final SingleResultFuture<CommandResult> retVal = new SingleResultFuture<CommandResult>();

        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(bufferProvider);
        final CommandMessage message = new CommandMessage(namespace.getFullName(), command, codec,
                getMessageSettings(connection.getDescription()));
        encodeMessageToBuffer(message, buffer);
        connection.sendAndReceiveMessage(buffer.getByteBuffers(), getResponseSettings(connection.getDescription(), message.getId()),
                new CommandResultCallback(
                        new SingleResultFutureCallback<CommandResult>(retVal), command, codec, connection, buffer, message.getId()));

        return retVal;
    }
}
