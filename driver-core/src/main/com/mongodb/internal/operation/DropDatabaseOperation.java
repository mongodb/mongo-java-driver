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

import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.internal.MongoNamespaceHelper;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.async.function.AsyncCallbackSupplier;
import com.mongodb.internal.async.function.RetryControl;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonInt32;

import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.AsyncOperationHelper.decorateWithRetriesAsync;
import static com.mongodb.internal.operation.AsyncOperationHelper.executeCommandAsync;
import static com.mongodb.internal.operation.AsyncOperationHelper.releasingCallback;
import static com.mongodb.internal.operation.AsyncOperationHelper.withAsyncConnection;
import static com.mongodb.internal.operation.AsyncOperationHelper.writeConcernErrorTransformerAsync;
import static com.mongodb.internal.operation.CommandOperationHelper.createSpecRetryControl;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.SpecRetryPolicy.IndividualPolicies.overloadForWrite;
import static com.mongodb.internal.operation.SyncOperationHelper.decorateWithRetries;
import static com.mongodb.internal.operation.SyncOperationHelper.executeCommand;
import static com.mongodb.internal.operation.SyncOperationHelper.withConnection;
import static com.mongodb.internal.operation.SyncOperationHelper.writeConcernErrorTransformer;
import static com.mongodb.internal.operation.WriteConcernHelper.appendWriteConcernToCommand;

/**
 * Operation to drop a database in MongoDB.  The {@code execute} method throws MongoCommandFailureException if something goes wrong, but
 * it will not throw an Exception if the collection does not exist before trying to drop it.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class DropDatabaseOperation implements WriteOperation<Void> {
    private final String databaseName;
    private final WriteConcern writeConcern;
    private final boolean retryWrites;
    @Nullable
    private final Integer maxAdaptiveRetriesSetting;

    public DropDatabaseOperation(final String databaseName, @Nullable final WriteConcern writeConcern) {
        this(databaseName, writeConcern, false, null);
    }

    public DropDatabaseOperation(final String databaseName, @Nullable final WriteConcern writeConcern,
            final boolean retryWrites, @Nullable final Integer maxAdaptiveRetriesSetting) {
        this.databaseName = notNull("databaseName", databaseName);
        this.writeConcern = writeConcern;
        this.retryWrites = retryWrites;
        this.maxAdaptiveRetriesSetting = maxAdaptiveRetriesSetting;
    }

    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    @Override
    public String getCommandName() {
        return "dropDatabase";
    }

    @Override
    public MongoNamespace getNamespace() {
        return new MongoNamespace(databaseName, MongoNamespaceHelper.COMMAND_COLLECTION_NAME);
    }

    @Override
    public Void execute(final WriteBinding binding, final OperationContext operationContext) {
        RetryControl<SpecRetryPolicy> retryControl = createSpecRetryControl(
                overloadForWrite(retryWrites, maxAdaptiveRetriesSetting),
                operationContext);
        Supplier<Void> retryingCommandExecutor = decorateWithRetries(retryControl, operationContext, () -> {
            retryControl.getPolicy().onCommand(this::getCommandName);
            return withConnection(binding, operationContext, (connection, operationContextWithMinRtt) -> {
                executeCommand(binding, operationContextWithMinRtt, databaseName, getCommand(), connection,
                        writeConcernErrorTransformer(operationContextWithMinRtt.getTimeoutContext()));
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
        AsyncCallbackSupplier<Void> retryingCommandExecutor = decorateWithRetriesAsync(retryControl, operationContext, supplierCallback ->
                withAsyncConnection(binding, operationContext, (connection, operationContextWithMinRtt, t) -> {
                    SingleResultCallback<Void> errHandlingCallback = errorHandlingCallback(supplierCallback, LOGGER);
                    if (t != null) {
                        errHandlingCallback.onResult(null, t);
                    } else {
                        executeCommandAsync(binding, operationContextWithMinRtt, databaseName, getCommand(), connection,
                                writeConcernErrorTransformerAsync(operationContextWithMinRtt.getTimeoutContext()),
                                releasingCallback(errHandlingCallback, connection));
                    }
                }));
        retryingCommandExecutor.get(callback);
    }

    private BsonDocument getCommand() {
        BsonDocument commandDocument = new BsonDocument("dropDatabase", new BsonInt32(1));
        appendWriteConcernToCommand(writeConcern, commandDocument);
        return commandDocument;
    }

}
