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
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;

import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.CommandOperationHelper.executeCommand;
import static com.mongodb.internal.operation.CommandOperationHelper.executeCommandAsync;
import static com.mongodb.internal.operation.CommandOperationHelper.writeConcernErrorTransformer;
import static com.mongodb.internal.operation.CommandOperationHelper.writeConcernErrorWriteTransformer;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.OperationHelper.releasingCallback;
import static com.mongodb.internal.operation.OperationHelper.withAsyncConnection;
import static com.mongodb.internal.operation.OperationHelper.withConnection;

/**
 * An abstract class for defining operations for managing Atlas Search indexes.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
abstract class AbstractWriteSearchIndexOperation implements AsyncWriteOperation<Void>, WriteOperation<Void> {
    private final MongoNamespace namespace;
    private final WriteConcern writeConcern;

    AbstractWriteSearchIndexOperation(final MongoNamespace mongoNamespace, @Nullable final WriteConcern writeConcern) {
        this.namespace = mongoNamespace;
        this.writeConcern = writeConcern;
    }

    @Override
    public Void execute(final WriteBinding binding) {
        return withConnection(binding, connection -> {
            try {
                executeCommand(binding, namespace.getDatabaseName(), buildCommand(), connection, writeConcernErrorTransformer());
            } catch (MongoCommandException mongoCommandException){
                handleCommandException(mongoCommandException);
            }
            return null;
        });
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<Void> callback) {
        withAsyncConnection(binding, (connection, connectionException) -> {
            SingleResultCallback<Void> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
            if (connectionException != null) {
                errHandlingCallback.onResult(null, connectionException);
            } else {
                SingleResultCallback<Void> completionCallback = releasingCallback(errHandlingCallback, connection);
                try {
                    executeCommandAsync(binding, namespace.getDatabaseName(),
                            buildCommand(), connection, writeConcernErrorWriteTransformer(),
                            getCommandExecutionCallback(completionCallback));
                } catch (Throwable commandExecutionException) {
                    completionCallback.onResult(null, commandExecutionException);
                }
            }
        });
    }

    SingleResultCallback<Void> getCommandExecutionCallback(final SingleResultCallback<Void> completionCallback) {
        return (result, commandExecutionException) -> completionCallback.onResult(null, commandExecutionException);
    }

    void handleCommandException(final MongoCommandException mongoCommandException) {
        throw mongoCommandException;
    }

    abstract BsonDocument buildCommand();

    public MongoNamespace getNamespace() {
        return namespace;
    }

    public WriteConcern getWriteConcern() {
        return writeConcern;
    }
}
