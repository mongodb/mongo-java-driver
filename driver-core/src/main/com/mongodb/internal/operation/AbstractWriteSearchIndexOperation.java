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
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;

import static com.mongodb.internal.operation.AsyncOperationHelper.executeCommandAsync;
import static com.mongodb.internal.operation.AsyncOperationHelper.withAsyncSourceAndConnection;
import static com.mongodb.internal.operation.AsyncOperationHelper.writeConcernErrorTransformerAsync;
import static com.mongodb.internal.operation.SyncOperationHelper.executeCommand;
import static com.mongodb.internal.operation.SyncOperationHelper.withConnection;
import static com.mongodb.internal.operation.SyncOperationHelper.writeConcernErrorTransformer;

/**
 * An abstract class for defining operations for managing Atlas Search indexes.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
abstract class AbstractWriteSearchIndexOperation implements AsyncWriteOperation<Void>, WriteOperation<Void> {
    private final TimeoutSettings timeoutSettings;
    private final MongoNamespace namespace;
    private final WriteConcern writeConcern;

    AbstractWriteSearchIndexOperation(final TimeoutSettings timeoutSettings, final MongoNamespace namespace,
                                      final WriteConcern writeConcern) {
        this.timeoutSettings = timeoutSettings;
        this.namespace = namespace;
        this.writeConcern = writeConcern;
    }

    @Override
    public TimeoutSettings getTimeoutSettings() {
        return timeoutSettings;
    }

    @Override
    public Void execute(final WriteBinding binding) {
        return withConnection(binding, connection -> {
            try {
                executeCommand(binding, namespace.getDatabaseName(), buildCommand(), connection, writeConcernErrorTransformer());
            } catch (MongoCommandException mongoCommandException) {
                swallowOrThrow(mongoCommandException);
            }
            return null;
        });
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<Void> callback) {
        withAsyncSourceAndConnection(binding::getWriteConnectionSource, false, callback,
                (connectionSource, connection, cb) ->
                        executeCommandAsync(binding, namespace.getDatabaseName(), buildCommand(), connection,
                                writeConcernErrorTransformerAsync(), (result, commandExecutionError) -> {
                                    try {
                                        swallowOrThrow(commandExecutionError);
                                        callback.onResult(result, null);
                                    } catch (Throwable mongoCommandException) {
                                        callback.onResult(null, mongoCommandException);
                                    }
                                }
                        )
        );
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
