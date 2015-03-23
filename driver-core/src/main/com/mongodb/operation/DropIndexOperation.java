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

import com.mongodb.MongoCommandException;
import com.mongodb.MongoNamespace;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncWriteBinding;
import com.mongodb.binding.WriteBinding;
import org.bson.BsonDocument;
import org.bson.BsonString;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static com.mongodb.operation.CommandOperationHelper.isNamespaceError;
import static com.mongodb.operation.IndexHelper.generateIndexName;
import static com.mongodb.operation.OperationHelper.VoidTransformer;

/**
 * An operation that drops an index.
 *
 * @since 3.0
 * @mongodb.driver.manual reference/command/dropIndexes/ Drop indexes
 */
public class DropIndexOperation implements AsyncWriteOperation<Void>, WriteOperation<Void> {
    private final MongoNamespace namespace;
    private final String indexName;

    /**
     * Construct a new instance.
     *
     * @param namespace the database and collection namespace for the operation.
     * @param indexName the name of the index to be dropped.
     */
    public DropIndexOperation(final MongoNamespace namespace, final String indexName) {
        this.namespace = notNull("namespace", namespace);
        this.indexName = notNull("indexName", indexName);
    }

    /**
     * Construct a new instance.
     *
     * @param namespace the database and collection namespace for the operation.
     * @param keys the keys of the index to be dropped
     */
    public DropIndexOperation(final MongoNamespace namespace, final BsonDocument keys) {
        this.namespace = notNull("namespace", namespace);
        this.indexName = generateIndexName(notNull("keys", keys));
    }

    @Override
    public Void execute(final WriteBinding binding) {
        try {
            executeWrappedCommandProtocol(namespace.getDatabaseName(), getCommand(), binding);
        } catch (MongoCommandException e) {
            CommandOperationHelper.rethrowIfNotNamespaceError(e);
        }
        return null;
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<Void> callback) {
        executeWrappedCommandProtocolAsync(namespace.getDatabaseName(), getCommand(), binding, new VoidTransformer<BsonDocument>(),
                                           new SingleResultCallback<Void>() {
                                               @Override
                                               public void onResult(final Void result, final Throwable t) {
                                                   if (t != null && !isNamespaceError(t)) {
                                                       callback.onResult(null, t);
                                                   } else {
                                                       callback.onResult(result, null);
                                                   }
                                               }
                                           });
    }

    private BsonDocument getCommand() {
        return new BsonDocument("dropIndexes", new BsonString(namespace.getCollectionName())).append("index", new BsonString(indexName));
    }
}
