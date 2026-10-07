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
import com.mongodb.internal.async.function.AsyncCallbackSupplier;
import com.mongodb.internal.async.function.RetryControl;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonString;

import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.operation.AsyncOperationHelper.decorateWithRetriesAsync;
import static com.mongodb.internal.operation.AsyncOperationHelper.executeCommandAsync;
import static com.mongodb.internal.operation.AsyncOperationHelper.writeConcernErrorTransformerAsync;
import static com.mongodb.internal.operation.CommandOperationHelper.createSpecRetryControl;
import static com.mongodb.internal.operation.CommandOperationHelper.isNamespaceError;
import static com.mongodb.internal.operation.CommandOperationHelper.rethrowIfNotNamespaceError;
import static com.mongodb.internal.operation.SpecRetryPolicy.IndividualPolicies.overloadForWrite;
import static com.mongodb.internal.operation.SyncOperationHelper.decorateWithRetries;
import static com.mongodb.internal.operation.SyncOperationHelper.executeCommand;
import static com.mongodb.internal.operation.SyncOperationHelper.writeConcernErrorTransformer;
import static com.mongodb.internal.operation.WriteConcernHelper.appendWriteConcernToCommand;

/**
 * An operation that drops an index.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class DropIndexOperation implements WriteOperation<Void> {
    private static final String COMMAND_NAME = "dropIndexes";
    private final MongoNamespace namespace;
    private final String indexName;
    private final BsonDocument indexKeys;
    private final WriteConcern writeConcern;
    private final boolean retryWrites;
    @Nullable
    private final Integer maxAdaptiveRetriesSetting;

    public DropIndexOperation(final MongoNamespace namespace, final String indexName, @Nullable final WriteConcern writeConcern) {
        this(namespace, indexName, writeConcern, false, null);
    }

    public DropIndexOperation(final MongoNamespace namespace, final BsonDocument indexKeys, @Nullable final WriteConcern writeConcern) {
        this(namespace, indexKeys, writeConcern, false, null);
    }

    public DropIndexOperation(final MongoNamespace namespace, final String indexName, @Nullable final WriteConcern writeConcern,
                              final boolean retryWrites, @Nullable final Integer maxAdaptiveRetriesSetting) {
        this.namespace = notNull("namespace", namespace);
        this.indexName = notNull("indexName", indexName);
        this.indexKeys = null;
        this.writeConcern = writeConcern;
        this.retryWrites = retryWrites;
        this.maxAdaptiveRetriesSetting = maxAdaptiveRetriesSetting;
    }

    public DropIndexOperation(final MongoNamespace namespace, final BsonDocument indexKeys, @Nullable final WriteConcern writeConcern,
                              final boolean retryWrites, @Nullable final Integer maxAdaptiveRetriesSetting) {
        this.namespace = notNull("namespace", namespace);
        this.indexKeys = notNull("indexKeys", indexKeys);
        this.indexName = null;
        this.writeConcern = writeConcern;
        this.retryWrites = retryWrites;
        this.maxAdaptiveRetriesSetting = maxAdaptiveRetriesSetting;
    }

    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    @Override
    public String getCommandName() {
        return COMMAND_NAME;
    }

    @Override
    public MongoNamespace getNamespace() {
        return namespace;
    }

    @Override
    public Void execute(final WriteBinding binding, final OperationContext operationContext) {
        RetryControl<SpecRetryPolicy> retryControl = createSpecRetryControl(
                overloadForWrite(retryWrites, maxAdaptiveRetriesSetting),
                operationContext);
        Supplier<Void> retryingCommandExecutor = decorateWithRetries(retryControl, operationContext, () -> {
            retryControl.getPolicy().onCommand(this::getCommandName);
            executeCommand(binding, operationContext, namespace.getDatabaseName(), getCommandCreator(), writeConcernErrorTransformer(
                    operationContext.getTimeoutContext()));
            return null;
        });
        try {
            retryingCommandExecutor.get();
        } catch (MongoCommandException e) {
            rethrowIfNotNamespaceError(e);
        }
        return null;
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final OperationContext operationContext,
                             final SingleResultCallback<Void> callback) {
        RetryControl<SpecRetryPolicy> retryControl = createSpecRetryControl(
                overloadForWrite(retryWrites, maxAdaptiveRetriesSetting),
                operationContext);
        AsyncCallbackSupplier<Void> retryingCommandExecutor = decorateWithRetriesAsync(retryControl, operationContext, supplierCallback -> {
            retryControl.getPolicy().onCommand(this::getCommandName);
            executeCommandAsync(binding, operationContext, namespace.getDatabaseName(), getCommandCreator(),
                    writeConcernErrorTransformerAsync(operationContext.getTimeoutContext()), supplierCallback);
        });
        retryingCommandExecutor.get((result, t) -> {
            if (t != null && !isNamespaceError(t)) {
                callback.onResult(null, t);
            } else {
                callback.onResult(null, null);
            }
        });
    }

    private CommandOperationHelper.CommandCreator getCommandCreator() {
        return (operationContext, serverDescription, connectionDescription) -> {
            BsonDocument command = new BsonDocument(getCommandName(), new BsonString(namespace.getCollectionName()));
            if (indexName != null) {
                command.put("index", new BsonString(indexName));
            } else {
                command.put("index", indexKeys);
            }
            appendWriteConcernToCommand(writeConcern, command);
            return command;
        };
    }

}
