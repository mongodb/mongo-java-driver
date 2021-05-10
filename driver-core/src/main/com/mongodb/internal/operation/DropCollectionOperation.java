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
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.ClientSideOperationTimeout;
import com.mongodb.internal.ClientSideOperationTimeoutFactory;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.internal.connection.AsyncConnection;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.operation.AsyncOperationHelper.AsyncCallableWithConnection;
import com.mongodb.internal.operation.SyncOperationHelper.CallableWithConnection;
import org.bson.BsonDocument;
import org.bson.BsonString;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.AsyncCommandOperationHelper.executeCommandAsync;
import static com.mongodb.internal.operation.AsyncCommandOperationHelper.writeConcernErrorTransformerAsync;
import static com.mongodb.internal.operation.AsyncOperationHelper.releasingCallback;
import static com.mongodb.internal.operation.AsyncOperationHelper.withAsyncConnection;
import static com.mongodb.internal.operation.CommandOperationHelper.isNamespaceError;
import static com.mongodb.internal.operation.CommandOperationHelper.rethrowIfNotNamespaceError;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.SyncCommandOperationHelper.executeCommand;
import static com.mongodb.internal.operation.SyncCommandOperationHelper.writeConcernErrorTransformer;
import static com.mongodb.internal.operation.SyncOperationHelper.withConnection;
import static com.mongodb.internal.operation.WriteConcernHelper.appendWriteConcernToCommand;

/**
 * Operation to drop a Collection in MongoDB.  The {@code execute} method throws MongoCommandFailureException if something goes wrong, but
 * it will not throw an Exception if the collection does not exist before trying to drop it.
 *
 * @since 3.0
 */
public class DropCollectionOperation implements AsyncWriteOperation<Void>, WriteOperation<Void> {
    private final ClientSideOperationTimeoutFactory clientSideOperationTimeoutFactory;
    private final MongoNamespace namespace;
    private final WriteConcern writeConcern;

    /**
     * Construct a new instance.
     *
     * @param clientSideOperationTimeoutFactory the client side operation timeout factory
     * @param namespace the database and collection namespace for the operation.
     */
    public DropCollectionOperation(final ClientSideOperationTimeoutFactory clientSideOperationTimeoutFactory,
                                   final MongoNamespace namespace) {
        this(clientSideOperationTimeoutFactory, namespace, null);
    }

    /**
     * Construct a new instance.
     *
     * @param clientSideOperationTimeoutFactory the client side operation timeout factory
     * @param namespace    the database and collection namespace for the operation.
     * @param writeConcern the write concern
     * @since 3.4
     */
    public DropCollectionOperation(final ClientSideOperationTimeoutFactory clientSideOperationTimeoutFactory,
                                   final MongoNamespace namespace, final WriteConcern writeConcern) {
        this.clientSideOperationTimeoutFactory = notNull("clientSideOperationTimeoutFactory", clientSideOperationTimeoutFactory);
        this.namespace = notNull("namespace", namespace);
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

    @Override
    public Void execute(final WriteBinding binding) {
        return withConnection(clientSideOperationTimeoutFactory.create(), binding, new CallableWithConnection<Void>() {
            @Override
            public Void call(final ClientSideOperationTimeout clientSideOperationTimeout, final Connection connection) {
                try {
                    executeCommand(clientSideOperationTimeout, binding, namespace.getDatabaseName(),
                            getCommand(connection.getDescription()), connection, writeConcernErrorTransformer());
                } catch (MongoCommandException e) {
                    rethrowIfNotNamespaceError(e);
                }
                return null;
            }
        });
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<Void> callback) {
        withAsyncConnection(clientSideOperationTimeoutFactory.create(), binding, new AsyncCallableWithConnection() {
            @Override
            public void call(final ClientSideOperationTimeout clientSideOperationTimeout, final AsyncConnection connection,
                             final Throwable t) {
                SingleResultCallback<Void> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
                if (t != null) {
                    errHandlingCallback.onResult(null, t);
                } else {
                    final SingleResultCallback<Void> releasingCallback = releasingCallback(errHandlingCallback, connection);
                    executeCommandAsync(clientSideOperationTimeout, binding, namespace.getDatabaseName(),
                            getCommand(connection.getDescription()),
                            connection, writeConcernErrorTransformerAsync(), new SingleResultCallback<Void>() {
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
        BsonDocument commandDocument = new BsonDocument("drop", new BsonString(namespace.getCollectionName()));
        appendWriteConcernToCommand(writeConcern, commandDocument, description);
        return commandDocument;
    }

}
