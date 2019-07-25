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

package com.mongodb.operation;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncWriteBinding;
import com.mongodb.binding.WriteBinding;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.operation.OperationHelper.AsyncCallableWithConnection;
import com.mongodb.operation.OperationHelper.CallableWithConnection;
import org.bson.BsonDocument;
import org.bson.BsonString;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.WriteConcernHelper.appendWriteConcernToCommand;
import static com.mongodb.operation.CommandOperationHelper.executeCommand;
import static com.mongodb.operation.CommandOperationHelper.executeCommandAsync;
import static com.mongodb.operation.CommandOperationHelper.isNamespaceError;
import static com.mongodb.operation.CommandOperationHelper.rethrowIfNotNamespaceError;
import static com.mongodb.operation.CommandOperationHelper.writeConcernErrorTransformer;
import static com.mongodb.operation.CommandOperationHelper.writeConcernErrorWriteTransformer;
import static com.mongodb.operation.DocumentHelper.putIfNotZero;
import static com.mongodb.operation.OperationHelper.LOGGER;
import static com.mongodb.operation.OperationHelper.releasingCallback;
import static com.mongodb.operation.OperationHelper.withConnection;
import static com.mongodb.operation.OperationHelper.withAsyncConnection;

/**
 * An operation that drops an index.
 *
 * @mongodb.driver.manual reference/command/dropIndexes/ Drop indexes
 * @since 3.0
 */
@Deprecated
public class DropIndexOperation implements AsyncWriteOperation<Void>, WriteOperation<Void> {
    private final MongoNamespace namespace;
    private final String indexName;
    private final BsonDocument indexKeys;
    private final WriteConcern writeConcern;
    private long maxTimeMS;

    /**
     * Construct a new instance.
     *
     * @param namespace the database and collection namespace for the operation.
     * @param indexName the name of the index to be dropped.
     * @deprecated Prefer {@link #DropIndexOperation(MongoNamespace, String, WriteConcern)}
     */
    @Deprecated
    public DropIndexOperation(final MongoNamespace namespace, final String indexName) {
        this(namespace, indexName, null);
    }

    /**
     * Construct a new instance.
     *
     * @param namespace the database and collection namespace for the operation.
     * @param keys      the keys of the index to be dropped
     * @deprecated Prefer {@link #DropIndexOperation(MongoNamespace, BsonDocument, WriteConcern)}
     */
    @Deprecated
    public DropIndexOperation(final MongoNamespace namespace, final BsonDocument keys) {
        this(namespace, keys, null);
    }

    /**
     * Construct a new instance.
     *
     * @param namespace    the database and collection namespace for the operation.
     * @param indexName    the name of the index to be dropped.
     * @param writeConcern the write concern
     * @since 3.4
     */
    public DropIndexOperation(final MongoNamespace namespace, final String indexName, final WriteConcern writeConcern) {
        this.namespace = notNull("namespace", namespace);
        this.indexName = notNull("indexName", indexName);
        this.indexKeys = null;
        this.writeConcern = writeConcern;
    }

    /**
     * Construct a new instance.
     *
     * @param namespace    the database and collection namespace for the operation.
     * @param indexKeys    the keys of the index to be dropped
     * @param writeConcern the write concern
     * @since 3.4
     */
    public DropIndexOperation(final MongoNamespace namespace, final BsonDocument indexKeys, final WriteConcern writeConcern) {
        this.namespace = notNull("namespace", namespace);
        this.indexKeys = notNull("indexKeys", indexKeys);
        this.indexName = null;
        this.writeConcern = writeConcern;
    }

    /**
     * Gets the write concern.
     *
     * @return the write concern, which may be null
     * @since 3.4
     */
    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    /**
     * Gets the maximum execution time on the server for this operation.  The default is 0, which places no limit on the execution time.
     *
     * @param timeUnit the time unit to return the result in
     * @return the maximum execution time in the given time unit
     * @since 3.6
     */
    public long getMaxTime(final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        return timeUnit.convert(maxTimeMS, TimeUnit.MILLISECONDS);
    }

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime  the max time
     * @param timeUnit the time unit, which may not be null
     * @return this
     * @since 3.6
     */
    public DropIndexOperation maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        isTrueArgument("maxTime >= 0", maxTime >= 0);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    @Override
    public Void execute(final WriteBinding binding) {
        return withConnection(binding, new CallableWithConnection<Void>() {
            @Override
            public Void call(final Connection connection) {
                try {
                    executeCommand(binding, namespace.getDatabaseName(), getCommand(connection.getDescription()), connection,
                            writeConcernErrorTransformer());
                } catch (MongoCommandException e) {
                    rethrowIfNotNamespaceError(e);
                }
                return null;
            }
        });
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<Void> callback) {
        withAsyncConnection(binding, new AsyncCallableWithConnection() {
            @Override
            public void call(final AsyncConnection connection, final Throwable t) {
                SingleResultCallback<Void> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
                if (t != null) {
                    errHandlingCallback.onResult(null, t);
                } else {
                    final SingleResultCallback<Void> releasingCallback = releasingCallback(errHandlingCallback, connection);
                    executeCommandAsync(binding, namespace.getDatabaseName(), getCommand(connection.getDescription()),
                            connection, writeConcernErrorWriteTransformer(), new SingleResultCallback<Void>() {
                                @Override
                                public void onResult(final Void result, final Throwable t) {
                                    if (t != null && !isNamespaceError(t)) {
                                        releasingCallback.onResult(null, t);
                                    } else {
                                        releasingCallback.onResult(result, null);
                                    }
                                }
                            });
                }
            }
        });
    }

    private BsonDocument getCommand(final ConnectionDescription description) {
        BsonDocument command = new BsonDocument("dropIndexes", new BsonString(namespace.getCollectionName()));
        if (indexName != null) {
            command.put("index", new BsonString(indexName));
        } else {
            command.put("index", indexKeys);
        }

        putIfNotZero(command, "maxTimeMS", maxTimeMS);
        appendWriteConcernToCommand(writeConcern, command, description);
        return command;
    }
}
