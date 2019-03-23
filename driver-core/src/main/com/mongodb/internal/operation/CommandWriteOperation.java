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

import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncWriteBinding;
import com.mongodb.binding.WriteBinding;
import org.bson.BsonDocument;
import org.bson.codecs.Decoder;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.operation.CommandOperationHelper.executeCommand;
import static com.mongodb.internal.operation.CommandOperationHelper.executeCommandAsync;

/**
 * An operation that executes an arbitrary command that writes to the server.
 *
 * @param <T> the operations result type.
 * @since 3.0
 */
public class CommandWriteOperation<T> implements AsyncWriteOperation<T>, WriteOperation<T> {
    private final String databaseName;
    private final BsonDocument command;
    private final Decoder<T> decoder;

    /**
     * Construct a new instance.
     *
     * @param databaseName the name of the database for the operation.
     * @param command the command to execute.
     * @param decoder the decoder for the result documents.
     */
    public CommandWriteOperation(final String databaseName, final BsonDocument command, final Decoder<T> decoder) {
        this.databaseName = notNull("databaseName", databaseName);
        this.command = notNull("command", command);
        this.decoder = notNull("decoder", decoder);
    }

    @Override
    public T execute(final WriteBinding binding) {
        return executeCommand(binding, databaseName, command, decoder);
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<T> callback) {
        executeCommandAsync(binding, databaseName, command, decoder, callback);
    }
}
