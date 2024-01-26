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
import org.bson.BsonString;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.operation.AsyncOperationHelper.executeCommandAsync;
import static com.mongodb.internal.operation.AsyncOperationHelper.writeConcernErrorTransformerAsync;
import static com.mongodb.internal.operation.CommandOperationHelper.isNamespaceError;
import static com.mongodb.internal.operation.CommandOperationHelper.rethrowIfNotNamespaceError;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotZero;
import static com.mongodb.internal.operation.SyncOperationHelper.executeCommand;
import static com.mongodb.internal.operation.SyncOperationHelper.writeConcernErrorTransformer;
import static com.mongodb.internal.operation.WriteConcernHelper.appendWriteConcernToCommand;

/**
 * An operation that drops an index.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class DropIndexOperation implements AsyncWriteOperation<Void>, WriteOperation<Void> {
    private final MongoNamespace namespace;
    private final String indexName;
    private final BsonDocument indexKeys;
    private final WriteConcern writeConcern;

    public DropIndexOperation(final MongoNamespace namespace, final String indexName, @Nullable final WriteConcern writeConcern) {
        this.namespace = notNull("namespace", namespace);
        this.indexName = notNull("indexName", indexName);
        this.indexKeys = null;
        this.writeConcern = writeConcern;
    }

    public DropIndexOperation(final MongoNamespace namespace, final BsonDocument indexKeys, @Nullable final WriteConcern writeConcern) {
        this.namespace = notNull("namespace", namespace);
        this.indexKeys = notNull("indexKeys", indexKeys);
        this.indexName = null;
        this.writeConcern = writeConcern;
    }

    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    @Override
    public Void execute(final WriteBinding binding) {
        try {
            executeCommand(binding, namespace.getDatabaseName(), getCommandCreator(), writeConcernErrorTransformer());
        } catch (MongoCommandException e) {
            rethrowIfNotNamespaceError(e);
        }
        return null;
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<Void> callback) {
        executeCommandAsync(binding, namespace.getDatabaseName(), getCommandCreator(), writeConcernErrorTransformerAsync(), (result, t) -> {
            if (t != null && !isNamespaceError(t)) {
                callback.onResult(null, t);
            } else {
                callback.onResult(null, null);
            }
        });
    }

    private CommandOperationHelper.CommandCreator getCommandCreator() {
        return (operationContext, serverDescription, connectionDescription) -> {
            BsonDocument command = new BsonDocument("dropIndexes", new BsonString(namespace.getCollectionName()));
            if (indexName != null) {
                command.put("index", new BsonString(indexName));
            } else {
                command.put("index", indexKeys);
            }
            putIfNotZero(command, "maxTimeMS", operationContext.getTimeoutContext().getMaxTimeMS());
            appendWriteConcernToCommand(writeConcern, command);
            return command;
        };
    }
}
