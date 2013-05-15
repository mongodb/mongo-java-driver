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

package org.mongodb.async;

import org.mongodb.Codec;
import org.mongodb.Document;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.command.MongoCommand;
import org.mongodb.impl.AsyncConnection;
import org.mongodb.io.BufferPool;
import org.mongodb.io.PooledByteBufferOutputBuffer;
import org.mongodb.protocol.MongoCommandMessage;
import org.mongodb.result.CommandResult;

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


    public MongoFuture<CommandResult> execute(final AsyncSession session) {
        AsyncConnection connection = session.getConnection(commandOperation.getReadPreference());

        MongoFuture<CommandResult> wrapped = execute(connection);
        SingleResultFuture<CommandResult> retVal = new SingleResultFuture<CommandResult>();
        wrapped.register(new ConnectionClosingSingleResultCallback<CommandResult>(connection, retVal));
        return retVal;
    }

    public MongoFuture<CommandResult> execute(final AsyncConnection connection) {
        final SingleResultFuture<CommandResult> retVal = new SingleResultFuture<CommandResult>();

        final PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(getBufferPool());
        final MongoCommandMessage message = new MongoCommandMessage(getNamespace().getFullName(), commandOperation, codec);
        encodeMessageToBuffer(message, buffer);
        connection.sendAndReceiveMessage(buffer, new MongoCommandResultCallback(
                new SingleResultFutureCallback<CommandResult>(retVal), commandOperation, codec, connection, message.getId()));

        return retVal;
    }
}
