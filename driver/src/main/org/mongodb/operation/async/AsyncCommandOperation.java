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

package org.mongodb.operation.async;

import org.mongodb.Codec;
import org.mongodb.Document;
import org.mongodb.MongoException;
import org.mongodb.MongoNamespace;
import org.mongodb.command.MongoCommand;
import org.mongodb.connection.AsyncServerConnection;
import org.mongodb.connection.BufferPool;
import org.mongodb.connection.PooledByteBufferOutputBuffer;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.operation.CommandResult;
import org.mongodb.operation.MongoFuture;
import org.mongodb.operation.ReadPreferenceServerSelector;
import org.mongodb.operation.protocol.MongoCommandMessage;
import org.mongodb.session.AsyncServerSelectingSession;

import java.nio.ByteBuffer;

public class AsyncCommandOperation extends AsyncOperation {

    private final MongoCommand commandOperation;
    private final Codec<Document> codec;

    public AsyncCommandOperation(final String database, final MongoCommand commandOperation, final Codec<Document> codec,
                                 final BufferPool<ByteBuffer> bufferPool) {
        super(new MongoNamespace(database, MongoNamespace.COMMAND_COLLECTION_NAME), bufferPool);
        this.commandOperation = commandOperation;
        this.codec = codec;
    }


    public MongoFuture<CommandResult> execute(final AsyncServerSelectingSession session) {
        final SingleResultFuture<CommandResult> retVal = new SingleResultFuture<CommandResult>();

        session.getConnection(new ReadPreferenceServerSelector(commandOperation.getReadPreference()))
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

        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(getBufferPool());
        final MongoCommandMessage message = new MongoCommandMessage(getNamespace().getFullName(), commandOperation, codec);
        encodeMessageToBuffer(message, buffer, connection.getDescription());
        connection.sendAndReceiveMessage(buffer, new MongoCommandResultCallback(
                new SingleResultFutureCallback<CommandResult>(retVal), commandOperation, codec, connection, message.getId()));

        return retVal;
    }
}
