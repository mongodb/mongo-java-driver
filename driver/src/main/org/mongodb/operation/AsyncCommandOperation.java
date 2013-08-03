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

import org.mongodb.AsyncServerSelectingOperation;
import org.mongodb.Codec;
import org.mongodb.CommandResult;
import org.mongodb.Document;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.command.Command;
import org.mongodb.connection.AsyncServerConnection;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.ClusterDescription;
import org.mongodb.connection.PooledByteBufferOutputBuffer;
import org.mongodb.connection.ServerSelector;
import org.mongodb.operation.protocol.CommandMessage;

import static org.mongodb.operation.OperationHelpers.encodeMessageToBuffer;
import static org.mongodb.operation.OperationHelpers.getMessageSettings;

public class AsyncCommandOperation implements AsyncServerSelectingOperation<CommandResult> {

    private final Command command;
    private final Codec<Document> codec;
    private final MongoNamespace namespace;
    private final BufferProvider bufferProvider;
    private final ClusterDescription clusterDescription;

    public AsyncCommandOperation(final String database, final Command command, final Codec<Document> codec,
                                 final ClusterDescription clusterDescription, final BufferProvider bufferProvider) {
        this.clusterDescription = clusterDescription;
        this.namespace = new MongoNamespace(database, MongoNamespace.COMMAND_COLLECTION_NAME);
        this.bufferProvider = bufferProvider;
        this.command = command;
        this.codec = codec;
    }

    public Command getCommand() {
        return command;
    }

    @Override
    public MongoFuture<CommandResult> execute(final AsyncServerConnection connection) {
        final SingleResultFuture<CommandResult> retVal = new SingleResultFuture<CommandResult>();

        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(bufferProvider);
        final CommandMessage message = new CommandMessage(namespace.getFullName(), command, codec,
                getMessageSettings(connection.getDescription()));
        encodeMessageToBuffer(message, buffer);
        connection.sendMessage(buffer.getByteBuffers(),
                new SendMessageCallback<CommandResult>(connection, buffer, message.getId(), retVal,
                        new CommandResultCallback(new SingleResultFutureCallback<CommandResult>(retVal), command, codec, connection,
                        message.getId())));
        return retVal;
    }

    @Override
    public ServerSelector getServerSelector() {
        return new ReadPreferenceServerSelector(CommandReadPreferenceHelper.getCommandReadPreference(command, clusterDescription));
    }

    @Override
    public boolean isQuery() {
        return CommandReadPreferenceHelper.isQuery(command);
    }
}
