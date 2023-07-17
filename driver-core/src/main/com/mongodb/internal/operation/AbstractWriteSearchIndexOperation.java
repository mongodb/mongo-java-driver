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


import com.mongodb.MongoCommandException;
import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.codecs.BsonDocumentCodec;

import static com.mongodb.internal.operation.CommandOperationHelper.executeRetryableWrite;
import static com.mongodb.internal.operation.CommandOperationHelper.executeRetryableWriteAsync;
import static com.mongodb.internal.operation.CommandOperationHelper.writeConcernErrorTransformer;
import static com.mongodb.internal.operation.CommandOperationHelper.writeConcernErrorTransformerAsync;
import static com.mongodb.internal.operation.OperationHelper.isRetryableWrite;

/**
 * An abstract class for defining operations for managing Atlas Search indexes.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
abstract class AbstractWriteSearchIndexOperation implements AsyncWriteOperation<Void>, WriteOperation<Void> {
    private final MongoNamespace namespace;
    private final WriteConcern writeConcern;
    private final boolean retryWrites;

    AbstractWriteSearchIndexOperation(final MongoNamespace mongoNamespace,
                                      final WriteConcern writeConcern,
                                      final boolean retryWrites) {
        this.namespace = mongoNamespace;
        this.writeConcern = writeConcern;
        this.retryWrites = retryWrites;
    }

    @Override
    public Void execute(final WriteBinding binding) {
        try {
            return executeRetryableWrite(binding, namespace.getDatabaseName(), null, new NoOpFieldNameValidator(),
                    new BsonDocumentCodec(), getCommandCreator(binding.getSessionContext()),
                    writeConcernErrorTransformer(), cmd -> cmd);
        } catch (MongoCommandException mongoCommandException) {
            swallowOrThrow(mongoCommandException);
        }
        return null;
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<Void> callback) {
        executeRetryableWriteAsync(binding, namespace.getDatabaseName(), null, new NoOpFieldNameValidator(),
                new BsonDocumentCodec(), getCommandCreator(binding.getSessionContext()), writeConcernErrorTransformerAsync(),
                cmd -> cmd, (result, commandExecutionError) -> {
                    try {
                        swallowOrThrow(commandExecutionError);
                        callback.onResult(result, null);
                    } catch (Throwable mongoCommandException) {
                        callback.onResult(null, mongoCommandException);
                    }
                });
    }

    private CommandOperationHelper.CommandCreator getCommandCreator(final SessionContext sessionContext) {
        return (serverDescription, connectionDescription) -> {

            BsonDocument command = buildCommand();
            if (isRetryableWrite(retryWrites, writeConcern, connectionDescription, sessionContext)) {
                command.put("txnNumber", new BsonInt64(sessionContext.advanceTransactionNumber()));
            }

            return command;
        };
    }

    /**
     * Handles the provided execution exception by either throwing it or ignoring it. This method is meant to be overridden
     * by subclasses that need to handle exceptions differently based on their specific requirements.
     *
     * <p>
     * <strong>Note:</strong> While the method declaration allows throwing a checked exception to enhance readability, the implementation
     * of this method must not throw a checked exception.
     * </p>
     *
     * @param <E>                     The type of the execution exception.
     * @param mongoExecutionException The execution exception to handle. If not null, it may be thrown or ignored.
     * @throws E The execution exception, if it is not null (implementation-specific).
     */
    <E extends Throwable> void swallowOrThrow(@Nullable final E mongoExecutionException) throws E {
        if (mongoExecutionException != null) {
            throw mongoExecutionException;
        }
    }

    abstract BsonDocument buildCommand();

    MongoNamespace getNamespace() {
        return namespace;
    }

    WriteConcern getWriteConcern() {
        return writeConcern;
    }
}
