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

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.CommandOperationHelper.executeCommand;
import static com.mongodb.internal.operation.CommandOperationHelper.executeCommandAsync;
import static com.mongodb.internal.operation.CommandOperationHelper.isNamespaceError;
import static com.mongodb.internal.operation.CommandOperationHelper.rethrowIfNotNamespaceError;
import static com.mongodb.internal.operation.CommandOperationHelper.writeConcernErrorTransformer;
import static com.mongodb.internal.operation.CommandOperationHelper.writeConcernErrorWriteTransformer;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotZero;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.OperationHelper.releasingCallback;
import static com.mongodb.internal.operation.OperationHelper.withAsyncConnection;
import static com.mongodb.internal.operation.OperationHelper.withConnection;
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
    private long maxTimeMS;

    public DropIndexOperation(final MongoNamespace namespace, final String indexName) {
        this(namespace, indexName, null);
    }

    public DropIndexOperation(final MongoNamespace namespace, final BsonDocument keys) {
        this(namespace, keys, null);
    }

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

    public long getMaxTime(final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        return timeUnit.convert(maxTimeMS, TimeUnit.MILLISECONDS);
    }

    public DropIndexOperation maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        isTrueArgument("maxTime >= 0", maxTime >= 0);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    @Override
    public Void execute(final WriteBinding binding) {
        return withConnection(binding, connection -> {
            try {
                executeCommand(binding, namespace.getDatabaseName(), getCommand(), connection,
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
                executeCommandAsync(binding, namespace.getDatabaseName(), getCommand(),
                        connection, writeConcernErrorWriteTransformer(), (result, t1) -> {
                            if (t1 != null && !isNamespaceError(t1)) {
                                releasingCallback.onResult(null, t1);
                            } else {
                                releasingCallback.onResult(result, null);
                            }
                        });
            }
        });
    }

    private BsonDocument getCommand() {
        BsonDocument command = new BsonDocument("dropIndexes", new BsonString(namespace.getCollectionName()));
        if (indexName != null) {
            command.put("index", new BsonString(indexName));
        } else {
            command.put("index", indexKeys);
        }

        putIfNotZero(command, "maxTimeMS", maxTimeMS);
        appendWriteConcernToCommand(writeConcern, command);
        return command;
    }
}
