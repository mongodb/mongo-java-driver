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
import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.MongoException;
import org.mongodb.MongoNamespace;
import org.mongodb.command.GetLastError;
import org.mongodb.command.MongoCommandFailureException;
import org.mongodb.connection.AsyncServerConnection;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.operation.protocol.RequestMessage;

import static org.mongodb.command.GetLastError.getCommandException;

class WriteResultCallback extends CommandResultBaseCallback {
    private final SingleResultCallback<CommandResult> callback;
    private final BaseWrite writeOperation;
    private final GetLastError getLastError;
    private final MongoNamespace namespace;
    private final RequestMessage nextMessage; // only used for batch inserts that need to be split into multiple messages
    private final BufferProvider bufferProvider;

    public WriteResultCallback(final SingleResultCallback<CommandResult> callback, final BaseWrite writeOperation,
                               final GetLastError getLastError, final Decoder<Document> decoder, final MongoNamespace namespace,
                               final RequestMessage nextMessage, final AsyncServerConnection connection,
                               final OutputBuffer writtenBuffer,
                               final BufferProvider bufferProvider) {
        this(callback, writeOperation, getLastError, decoder, namespace, nextMessage, connection, writtenBuffer, bufferProvider, 0);
    }

    public WriteResultCallback(final SingleResultCallback<CommandResult> callback, final BaseWrite writeOperation,
                               final GetLastError getLastError, final Decoder<Document> decoder, final MongoNamespace namespace,
                               final RequestMessage nextMessage, final AsyncServerConnection connection,
                               final OutputBuffer writtenBuffer, final BufferProvider bufferProvider, final long requestId) {
        super(getLastError, decoder, connection, writtenBuffer, requestId);
        this.callback = callback;
        this.writeOperation = writeOperation;
        this.getLastError = getLastError;
        this.namespace = namespace;
        this.nextMessage = nextMessage;
        this.bufferProvider = bufferProvider;
    }

    @Override
    protected void callCallback(final CommandResult commandResult, final MongoException e) {
        if (e != null) {
            callback.onResult(null, e);
        }
        else if (getLastError != null && !commandResult.isOk()) {
            callback.onResult(null, new MongoCommandFailureException(commandResult));
        }
        else {
            MongoCommandFailureException commandException = null;
            if (getLastError != null) {
                commandException = getCommandException(commandResult);
            }

            if (commandException != null) {
                callback.onResult(null, commandException);
            }
            else if (nextMessage != null) {
                new GenericAsyncWriteOperation(namespace, writeOperation, nextMessage, bufferProvider).execute(getConnection(), callback);
            }
            else {
                callback.onResult(commandResult, null);
            }
        }
    }
}
