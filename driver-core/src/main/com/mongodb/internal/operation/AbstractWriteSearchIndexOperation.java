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
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.async.function.AsyncCallbackSupplier;
import com.mongodb.internal.async.function.RetryControl;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;

import java.util.function.Supplier;

import static com.mongodb.internal.operation.AsyncOperationHelper.decorateWithRetriesAsync;
import static com.mongodb.internal.operation.AsyncOperationHelper.executeCommandAsync;
import static com.mongodb.internal.operation.AsyncOperationHelper.withAsyncSourceAndConnection;
import static com.mongodb.internal.operation.AsyncOperationHelper.writeConcernErrorTransformerAsync;
import static com.mongodb.internal.operation.CommandOperationHelper.createSpecRetryControl;
import static com.mongodb.internal.operation.SpecRetryPolicy.IndividualPolicies.overloadForWrite;
import static com.mongodb.internal.operation.SyncOperationHelper.decorateWithRetries;
import static com.mongodb.internal.operation.SyncOperationHelper.executeCommand;
import static com.mongodb.internal.operation.SyncOperationHelper.withConnection;
import static com.mongodb.internal.operation.SyncOperationHelper.writeConcernErrorTransformer;

/**
 * An abstract class for defining operations for managing Atlas Search indexes.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
abstract class AbstractWriteSearchIndexOperation implements WriteOperation<Void> {
    private final MongoNamespace namespace;
    private final boolean retryWrites;
    @Nullable
    private final Integer maxAdaptiveRetriesSetting;

    AbstractWriteSearchIndexOperation(final MongoNamespace namespace) {
        this(namespace, false, null);
    }

    AbstractWriteSearchIndexOperation(final MongoNamespace namespace, final boolean retryWrites,
            @Nullable final Integer maxAdaptiveRetriesSetting) {
        this.namespace = namespace;
        this.retryWrites = retryWrites;
        this.maxAdaptiveRetriesSetting = maxAdaptiveRetriesSetting;
    }

    @Override
    public Void execute(final WriteBinding binding, final OperationContext operationContext) {
        RetryControl<SpecRetryPolicy> retryControl = createSpecRetryControl(
                overloadForWrite(retryWrites, maxAdaptiveRetriesSetting),
                operationContext);
        Supplier<Void> retryingCommandExecutor = decorateWithRetries(retryControl, operationContext, () -> {
            retryControl.getPolicy().onCommand(this::getCommandName);
            return withConnection(binding, operationContext, (connection, operationContextWithMinRtt) -> {
                try {
                    executeCommand(binding, operationContextWithMinRtt, namespace.getDatabaseName(), buildCommand(),
                            connection,
                            writeConcernErrorTransformer(operationContextWithMinRtt.getTimeoutContext()));
                } catch (MongoCommandException mongoCommandException) {
                    swallowOrThrow(mongoCommandException);
                }
                return null;
            });
        });
        return retryingCommandExecutor.get();
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final OperationContext operationContext, final SingleResultCallback<Void> callback) {
        RetryControl<SpecRetryPolicy> retryControl = createSpecRetryControl(
                overloadForWrite(retryWrites, maxAdaptiveRetriesSetting),
                operationContext);
        AsyncCallbackSupplier<Void> retryingCommandExecutor = decorateWithRetriesAsync(retryControl, operationContext, supplierCallback -> {
            retryControl.getPolicy().onCommand(this::getCommandName);
            withAsyncSourceAndConnection(binding::getWriteConnectionSource, false, operationContext, supplierCallback,
                    (connectionSource, connection, operationContextWithMinRtt, cb) ->
                            executeCommandAsync(binding, operationContextWithMinRtt, namespace.getDatabaseName(), buildCommand(),
                                    connection, writeConcernErrorTransformerAsync(operationContextWithMinRtt.getTimeoutContext()),
                                    (result, commandExecutionError) -> {
                                        try {
                                            swallowOrThrow(commandExecutionError);
                                            cb.onResult(result, null);
                                        } catch (Throwable mongoCommandException) {
                                            cb.onResult(null, mongoCommandException);
                                        }
                                    }
                            ));
        });
        retryingCommandExecutor.get(callback);
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


    @Override
    public MongoNamespace getNamespace() {
        return namespace;
    }

}
