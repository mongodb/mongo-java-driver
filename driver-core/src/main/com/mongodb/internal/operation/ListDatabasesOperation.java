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

import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.ClientSideOperationTimeout;
import com.mongodb.internal.ClientSideOperationTimeoutFactory;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncConnectionSource;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.connection.AsyncConnection;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.QueryResult;
import com.mongodb.internal.operation.AsyncCommandOperationHelper.CommandReadTransformerAsync;
import com.mongodb.internal.operation.SyncCommandOperationHelper.CommandReadTransformer;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.codecs.Decoder;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.AsyncCommandOperationHelper.executeCommandAsync;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotNull;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotZero;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.SyncCommandOperationHelper.executeCommand;


/**
 * An operation that provides a cursor allowing iteration through the metadata of all the databases for a MongoClient.
 *
 * @param <T> the document type
 * @since 3.0
 */
public class ListDatabasesOperation<T> implements AsyncReadOperation<AsyncBatchCursor<T>>, ReadOperation<BatchCursor<T>> {
    private final ClientSideOperationTimeoutFactory clientSideOperationTimeoutFactory;
    private final Decoder<T> decoder;
    private boolean retryReads;

    private BsonDocument filter;
    private Boolean nameOnly;
    private Boolean authorizedDatabasesOnly;

    /**
     * Construct a new instance.
     *
     * @param clientSideOperationTimeoutFactory the client side operation timeout factory
     * @param decoder the decoder to use for the results
     */
    public ListDatabasesOperation(final ClientSideOperationTimeoutFactory clientSideOperationTimeoutFactory, final Decoder<T> decoder) {
        this.clientSideOperationTimeoutFactory = notNull("clientSideOperationTimeoutFactory", clientSideOperationTimeoutFactory);
        this.decoder = notNull("decoder", decoder);
    }

    /**
     * Sets the query filter to apply to the returned database names.
     *
     * @param filter the filter, which may be null.
     * @return this
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    public ListDatabasesOperation<T> filter(final BsonDocument filter) {
        this.filter = filter;
        return this;
    }

    /**
     * Gets the query filter to apply to the returned database names.
     *
     * @return this
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    public BsonDocument getFilter() {
        return filter;
    }

    /**
     * Sets the nameOnly flag that indicates whether the command should return just the database names or return the database names and
     * size information.
     *
     * @param nameOnly the nameOnly flag, which may be null
     * @return this
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    public ListDatabasesOperation<T> nameOnly(final Boolean nameOnly) {
        this.nameOnly = nameOnly;
        return this;
    }

    /**
     * Sets the authorizedDatabasesOnly flag that indicates whether the command should return just the databases which the user
     * is authorized to see.
     *
     * @param authorizedDatabasesOnly the authorizedDatabasesOnly flag, which may be null
     * @return this
     */
    public ListDatabasesOperation<T> authorizedDatabasesOnly(final Boolean authorizedDatabasesOnly) {
        this.authorizedDatabasesOnly = authorizedDatabasesOnly;
        return this;
    }

    /**
     * Enables retryable reads if a read fails due to a network error.
     *
     * @param retryReads true if reads should be retried
     * @return this
     * @since 3.11
     */
    public ListDatabasesOperation<T> retryReads(final boolean retryReads) {
        this.retryReads = retryReads;
        return this;
    }

    /**
     * Gets the value for retryable reads. The default is true.
     *
     * @return the retryable reads value
     * @since 3.11
     */
    public boolean getRetryReads() {
        return retryReads;
    }

    /**
     * Gets the nameOnly flag that indicates whether the command should return just the database names or return the database names and
     * size information.
     *
     * @return this
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    public Boolean getNameOnly() {
        return nameOnly;
    }

    /**
     * Gets the authorizedDatabasesOnly flag that indicates whether the command should return just the databases which the user
     * is authorized to see.
     *
     * @return the authorized databases value
     * @since 4.1
     */
    public Boolean getAuthorizedDatabasesOnly() {
        return authorizedDatabasesOnly;
    }

    /**
     * Executing this will return a list of all the databases names in the MongoDB instance.
     *
     * @param binding the binding.
     * @return a List of Strings of the names of all the databases in the MongoDB instance.
     */
    @Override
    public BatchCursor<T> execute(final ReadBinding binding) {
        return executeCommand(clientSideOperationTimeoutFactory.create(), binding, "admin", getCommandCreator(),
                CommandResultDocumentCodec.create(decoder, "databases"), transformer(), retryReads);
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        executeCommandAsync(clientSideOperationTimeoutFactory.create(), binding, "admin", getCommandCreator(),
                CommandResultDocumentCodec.create(decoder, "databases"), asyncTransformer(),
                retryReads, errorHandlingCallback(callback, LOGGER));
    }

    private CommandReadTransformer<BsonDocument, BatchCursor<T>> transformer() {
        return new CommandReadTransformer<BsonDocument, BatchCursor<T>>() {
            @Override
            public BatchCursor<T> apply(final ClientSideOperationTimeout clientSideOperationTimeout, final ConnectionSource source,
                                        final Connection connection, final BsonDocument result) {
                return new QueryBatchCursor<T>(clientSideOperationTimeout, createQueryResult(result, connection.getDescription()), 0, 0,
                        decoder, source, connection);
            }
        };
    }

    private CommandReadTransformerAsync<BsonDocument, AsyncBatchCursor<T>> asyncTransformer() {
        return new CommandReadTransformerAsync<BsonDocument, AsyncBatchCursor<T>>() {
            @Override
            public AsyncBatchCursor<T> apply(final ClientSideOperationTimeout clientSideOperationTimeout,
                                             final AsyncConnectionSource source, final AsyncConnection connection,
                                             final BsonDocument result) {
                return new AsyncQueryBatchCursor<T>(clientSideOperationTimeout, createQueryResult(result, connection.getDescription()), 0,
                        0, decoder, source, connection);
            }
        };
    }

    @SuppressWarnings("unchecked")
    private QueryResult<T> createQueryResult(final BsonDocument result, final ConnectionDescription description) {
        return new QueryResult<T>(null, BsonDocumentWrapperHelper.<T>toList(result, "databases"), 0,
                description.getServerAddress());
    }

    private CommandCreator getCommandCreator() {
        return new CommandCreator() {
            @Override
            public BsonDocument create(final ClientSideOperationTimeout clientSideOperationTimeout,
                                       final ServerDescription serverDescription, final ConnectionDescription connectionDescription) {
                return getCommand(clientSideOperationTimeout);
            }
        };
    }

    private BsonDocument getCommand(final ClientSideOperationTimeout clientSideOperationTimeout) {
        BsonDocument command = new BsonDocument("listDatabases", new BsonInt32(1));
        putIfNotZero(command, "maxTimeMS", clientSideOperationTimeout.getMaxTimeMS());
        putIfNotNull(command, "filter", filter);
        if (nameOnly != null) {
            command.put("nameOnly", new BsonBoolean(nameOnly));
        }
        if (authorizedDatabasesOnly != null) {
            command.put("authorizedDatabases", new BsonBoolean(authorizedDatabasesOnly));
        }
        return command;
    }
}
