/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.internal.operation;

import com.mongodb.internal.ClientSideOperationTimeoutFactory;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ReadBinding;
import org.bson.BsonDocument;
import org.bson.codecs.Decoder;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.operation.AsyncCommandOperationHelper.executeCommandAsync;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotZero;
import static com.mongodb.internal.operation.SyncCommandOperationHelper.executeCommand;

/**
 * An operation that executes an arbitrary command that reads from the server.
 *
 * @param <T> the operations result type.
 * @since 3.0
 */
public class CommandReadOperation<T> implements AsyncReadOperation<T>, ReadOperation<T> {
    private final ClientSideOperationTimeoutFactory clientSideOperationTimeoutFactory;
    private final String databaseName;
    private final CommandCreator commandCreator;
    private final Decoder<T> decoder;


    /**
     * Construct a new instance.
     *
     * <p>Will overwrite any existing maxTimeMS value if the ClientSideOperationTimeout contains a timeoutMS value.</p>
     *
     * @param clientSideOperationTimeoutFactory the client side operation timeout factory
     * @param databaseName the name of the database for the operation.
     * @param command the command to execute.
     * @param decoder the decoder for the result documents.
     */
    public CommandReadOperation(final ClientSideOperationTimeoutFactory clientSideOperationTimeoutFactory, final String databaseName,
                                final BsonDocument command, final Decoder<T> decoder) {
        this(clientSideOperationTimeoutFactory, databaseName,
                (clientSideOperationTimeout, serverDescription, connectionDescription) -> {
                    notNull("command", command);
                    if (clientSideOperationTimeout.hasTimeoutMS()) {
                        putIfNotZero(command, "maxTimeMS", clientSideOperationTimeout.getMaxTimeMS());
                    }
                    return command;
            }, decoder);
    }

    /**
     * Construct a new instance.
     * @param clientSideOperationTimeoutFactory the client side operation timeout factory
     * @param databaseName the name of the database for the operation.
     * @param commandCreator the command creator.
     * @param decoder the decoder for the result documents.
     */
    public CommandReadOperation(final ClientSideOperationTimeoutFactory clientSideOperationTimeoutFactory, final String databaseName,
                                final CommandCreator commandCreator, final Decoder<T> decoder) {
        this.clientSideOperationTimeoutFactory = notNull("clientSideOperationTimeoutFactory", clientSideOperationTimeoutFactory);
        this.databaseName = notNull("databaseName", databaseName);
        this.commandCreator = notNull("command", commandCreator);
        this.decoder = notNull("decoder", decoder);
    }

    @Override
    public T execute(final ReadBinding binding) {
        return executeCommand(clientSideOperationTimeoutFactory.create(), binding, databaseName, commandCreator, decoder, false);
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<T> callback) {
        executeCommandAsync(clientSideOperationTimeoutFactory.create(), binding, databaseName, commandCreator, decoder, false, callback);
    }

}
