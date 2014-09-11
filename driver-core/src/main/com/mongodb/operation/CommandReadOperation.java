/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.operation;

import com.mongodb.async.MongoFuture;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.ReadBinding;
import org.bson.BsonDocument;
import org.bson.codecs.Decoder;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;

/**
 * An operation that executes an arbitrary command that reads from the server.
 *
 * @param <T> the operations result type.
 * @since 3.0
 */
public class CommandReadOperation<T> implements AsyncReadOperation<T>, ReadOperation<T> {
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
    public CommandReadOperation(final String databaseName, final BsonDocument command, final Decoder<T> decoder) {
        this.databaseName = notNull("databaseName", databaseName);
        this.command = notNull("command", command);
        this.decoder = notNull("decoder", decoder);
    }

    @Override
    public T execute(final ReadBinding binding) {
        return executeWrappedCommandProtocol(databaseName, command, decoder, binding);
    }

    @Override
    public MongoFuture<T> executeAsync(final AsyncReadBinding binding) {
        return executeWrappedCommandProtocolAsync(databaseName, command, decoder, binding);
    }
}
