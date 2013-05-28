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

import org.mongodb.MongoException;
import org.mongodb.MongoNamespace;
import org.mongodb.WriteConcern;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.command.GetLastError;
import org.mongodb.connection.AsyncServerConnection;
import org.mongodb.connection.BufferPool;
import org.mongodb.connection.PooledByteBufferOutputBuffer;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.operation.MongoFuture;
import org.mongodb.operation.MongoWrite;
import org.mongodb.operation.WriteResult;
import org.mongodb.operation.protocol.MessageSettings;
import org.mongodb.operation.protocol.MongoCommandMessage;
import org.mongodb.operation.protocol.MongoRequestMessage;
import org.mongodb.session.AsyncSession;

import java.nio.ByteBuffer;

public abstract class AsyncWriteOperation extends AsyncOperation {
    public AsyncWriteOperation(final MongoNamespace namespace, final BufferPool<ByteBuffer> bufferPool) {
        super(namespace, bufferPool);
    }

    public MongoFuture<WriteResult> execute(final AsyncSession session) {
        final SingleResultFuture<WriteResult> retVal = new SingleResultFuture<WriteResult>();

        session.getConnection().register(new SingleResultCallback<AsyncServerConnection>() {
            @Override
            public void onResult(final AsyncServerConnection connection, final MongoException e) {
                if (e != null) {
                    retVal.init(null, e);
                }
                else {
                    MongoFuture<WriteResult> wrapped = execute(connection);
                    wrapped.register(new ConnectionClosingSingleResultCallback<WriteResult>(connection, retVal));
                }
            }
        });

        return retVal;
    }

    public MongoFuture<WriteResult> execute(final AsyncServerConnection connection) {
        SingleResultFuture<WriteResult> retVal = new SingleResultFuture<WriteResult>();
        execute(connection, new SingleResultFutureCallback<WriteResult>(retVal));
        return retVal;
    }


    public void execute(final AsyncServerConnection connection, final SingleResultCallback<WriteResult> callback) {
        PooledByteBufferOutputBuffer buffer = new PooledByteBufferOutputBuffer(getBufferPool());
        MongoRequestMessage nextMessage = encodeMessageToBuffer(createRequestMessage(getMessageSettings(connection.getDescription())),
                buffer);
        if (getWriteConcern().callGetLastError()) {
            final GetLastError getLastError = new GetLastError(getWriteConcern());
            MongoCommandMessage getLastErrorMessage =
                    new MongoCommandMessage(new MongoNamespace(getNamespace().getDatabaseName(), MongoNamespace.COMMAND_COLLECTION_NAME)
                            .getFullName(), getLastError, new DocumentCodec(), getMessageSettings(connection.getDescription()));
            encodeMessageToBuffer(getLastErrorMessage, buffer);
            connection.sendAndReceiveMessage(buffer, new MongoWriteResultCallback(callback, getWrite(), getLastError,
                    new DocumentCodec(), getNamespace(), nextMessage, connection, getBufferPool(), getLastErrorMessage.getId()));
        }
        else {
            connection.sendMessage(buffer, new MongoWriteResultCallback(callback, getWrite(), null, new DocumentCodec(),
                    getNamespace(), nextMessage, connection, getBufferPool()));
        }
    }

    protected abstract MongoRequestMessage createRequestMessage(final MessageSettings settings);

    public abstract MongoWrite getWrite();

    public abstract WriteConcern getWriteConcern();
}
