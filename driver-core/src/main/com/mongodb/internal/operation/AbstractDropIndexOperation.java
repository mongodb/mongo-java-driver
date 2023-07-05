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

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.CommandOperationHelper.executeCommand;
import static com.mongodb.internal.operation.CommandOperationHelper.executeCommandAsync;
import static com.mongodb.internal.operation.CommandOperationHelper.isNamespaceError;
import static com.mongodb.internal.operation.CommandOperationHelper.rethrowIfNotNamespaceError;
import static com.mongodb.internal.operation.CommandOperationHelper.writeConcernErrorTransformer;
import static com.mongodb.internal.operation.CommandOperationHelper.writeConcernErrorWriteTransformer;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.OperationHelper.releasingCallback;
import static com.mongodb.internal.operation.OperationHelper.withAsyncConnection;
import static com.mongodb.internal.operation.OperationHelper.withConnection;

/**
 * An operation that drops an index.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public abstract class AbstractDropIndexOperation implements AsyncWriteOperation<Void>, WriteOperation<Void> {
    private final MongoNamespace namespace;
    private final String indexName;
    private final WriteConcern writeConcern;

    public AbstractDropIndexOperation(final MongoNamespace namespace, final String indexName, @Nullable final WriteConcern writeConcern) {
        this.namespace = notNull("namespace", namespace);
        this.indexName = notNull("indexName", indexName);
        this.writeConcern = writeConcern;
    }

    public AbstractDropIndexOperation(final MongoNamespace namespace, @Nullable final WriteConcern writeConcern) {
        this.namespace = notNull("namespace", namespace);
        this.indexName = null;
        this.writeConcern = writeConcern;
    }

    @Override
    public Void execute(final WriteBinding binding) {
        return withConnection(binding, connection -> {
            try {
                executeCommand(binding, namespace.getDatabaseName(), buildCommand(), connection,
                        writeConcernErrorTransformer());
            } catch (MongoCommandException e) {
                rethrowIfNotNamespaceError(e);
            }
            return null;
        });
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<Void> callback) {
        withAsyncConnection(binding, (connection, t) -> {
            SingleResultCallback<Void> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
            if (t != null) {
                errHandlingCallback.onResult(null, t);
            } else {
                SingleResultCallback<Void> releasingCallback = releasingCallback(errHandlingCallback, connection);
                executeCommandAsync(binding, namespace.getDatabaseName(),
                        buildCommand(), connection, writeConcernErrorWriteTransformer(),
                        (result, t1) -> {
                            if (t1 != null && !isNamespaceError(t1)) {
                                releasingCallback.onResult(null, t1);
                            } else {
                                releasingCallback.onResult(result, null);
                            }
                        });
            }
        });
    }

    abstract BsonDocument buildCommand();

    public MongoNamespace getNamespace() {
        return namespace;
    }

    public String getIndexName() {
        return indexName;
    }

    public WriteConcern getWriteConcern() {
        return writeConcern;
    }
}
