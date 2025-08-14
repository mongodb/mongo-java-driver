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

import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ReadBinding;
import org.bson.BsonDocument;
import org.bson.codecs.Decoder;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.operation.AsyncOperationHelper.executeRetryableReadAsync;
import static com.mongodb.internal.operation.CommandOperationHelper.CommandCreator;
import static com.mongodb.internal.operation.SyncOperationHelper.executeRetryableRead;

/**
 * An operation that executes an arbitrary command that reads from the server.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class CommandReadOperation<T> implements ReadOperationSimple<T> {
    private final String commandName;
    private final String databaseName;
    private final CommandCreator commandCreator;
    private final Decoder<T> decoder;

    public CommandReadOperation(final String databaseName,  final BsonDocument command, final Decoder<T> decoder) {
        this(databaseName, command.getFirstKey(), (operationContext, serverDescription, connectionDescription) -> command, decoder);
    }

    public CommandReadOperation(final String databaseName, final String commandName, final CommandCreator commandCreator,
                                final Decoder<T> decoder) {
        this.commandName = notNull("commandName", commandName);
        this.databaseName = notNull("databaseName", databaseName);
        this.commandCreator = notNull("commandCreator", commandCreator);
        this.decoder = notNull("decoder", decoder);
    }

    @Override
    public String getCommandName() {
        return commandName;
    }

    @Override
    public T execute(final ReadBinding binding) {
        return executeRetryableRead(binding, databaseName, commandCreator, decoder,
                                    (result, source, connection) -> result, false);
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<T> callback) {
        executeRetryableReadAsync(binding, databaseName, commandCreator, decoder,
                                  (result, source, connection) -> result, false, callback);
    }
}
