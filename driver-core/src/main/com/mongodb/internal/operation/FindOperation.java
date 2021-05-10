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

import com.mongodb.CursorType;
import com.mongodb.ExplainVerbosity;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoQueryException;
import com.mongodb.ReadPreference;
import com.mongodb.client.model.Collation;
import com.mongodb.connection.ConnectionDescription;
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
import com.mongodb.internal.connection.NoOpSessionContext;
import com.mongodb.internal.connection.QueryResult;
import com.mongodb.internal.operation.SyncOperationHelper.CallableWithSource;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.lang.Nullable;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.Decoder;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.ServerType.SHARD_ROUTER;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.AsyncCommandOperationHelper.CommandReadTransformerAsync;
import static com.mongodb.internal.operation.AsyncCommandOperationHelper.executeCommandAsyncWithConnection;
import static com.mongodb.internal.operation.AsyncOperationHelper.AsyncCallableWithConnectionAndSource;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotNull;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotNullOrEmpty;
import static com.mongodb.internal.operation.ExplainHelper.asExplainCommandCreator;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.OperationHelper.cursorDocumentToQueryResult;
import static com.mongodb.internal.operation.OperationHelper.validateFindOptions;
import static com.mongodb.internal.operation.OperationReadConcernHelper.appendReadConcernToCommand;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsAtLeastVersionThreeDotTwo;
import static com.mongodb.internal.operation.SyncCommandOperationHelper.CommandReadTransformer;
import static com.mongodb.internal.operation.SyncCommandOperationHelper.executeCommandWithConnection;

/**
 * An operation that queries a collection using the provided criteria.
 *
 * @param <T> the operations result type.
 * @since 3.0
 */
public class FindOperation<T> implements AsyncExplainableReadOperation<AsyncBatchCursor<T>>, ExplainableReadOperation<BatchCursor<T>> {
    private static final String FIRST_BATCH = "firstBatch";

    private final MongoNamespace namespace;
    private final Decoder<T> decoder;
    private final ClientSideOperationTimeoutFactory clientSideOperationTimeoutFactory;
    private boolean retryReads;
    private BsonDocument filter;
    private int batchSize;
    private int limit;
    private BsonDocument projection;
    private int skip;
    private BsonDocument sort;
    private CursorType cursorType = CursorType.NonTailable;
    private boolean slaveOk;
    private boolean oplogReplay;
    private boolean noCursorTimeout;
    private boolean partial;
    private Collation collation;
    private String comment;
    private BsonValue hint;
    private BsonDocument max;
    private BsonDocument min;
    private boolean returnKey;
    private boolean showRecordId;
    private Boolean allowDiskUse;

    /**
     * Construct a new instance.
     * @param clientSideOperationTimeoutFactory the client side operation timeout factory
     * @param namespace the database and collection namespace for the operation.
     * @param decoder the decoder for the result documents.
     */
    public FindOperation(final ClientSideOperationTimeoutFactory clientSideOperationTimeoutFactory, final MongoNamespace namespace,
                         final Decoder<T> decoder) {
        this.clientSideOperationTimeoutFactory = notNull("clientSideOperationTimeoutFactory", clientSideOperationTimeoutFactory);
        this.namespace = notNull("namespace", namespace);
        this.decoder = notNull("decoder", decoder);
    }

    /**
     * Gets the namespace.
     *
     * @return the namespace
     */
    public MongoNamespace getNamespace() {
        return namespace;
    }

    /**
     * Gets the decoder used to decode the result documents.
     *
     * @return the decoder
     */
    public Decoder<T> getDecoder() {
        return decoder;
    }

    /**
     * Gets the query filter.
     *
     * @return the query filter
     * @mongodb.driver.manual reference/method/db.collection.find/ Filter
     */
    public BsonDocument getFilter() {
        return filter;
    }

    /**
     * Sets the query filter to apply to the query.
     *
     * @param filter the filter, which may be null.
     * @return this
     * @mongodb.driver.manual reference/method/db.collection.find/ Filter
     */
    public FindOperation<T> filter(final BsonDocument filter) {
        this.filter = filter;
        return this;
    }

    /**
     * Gets the number of documents to return per batch.  Default to 0, which indicates that the server chooses an appropriate batch size.
     *
     * @return the batch size, which may be null
     * @mongodb.driver.manual reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Sets the number of documents to return per batch.
     *
     * @param batchSize the batch size
     * @return this
     * @mongodb.driver.manual reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    public FindOperation<T> batchSize(final int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    /**
     * Gets the limit to apply.  The default is null.
     *
     * @return the limit
     * @mongodb.driver.manual reference/method/cursor.limit/#cursor.limit Limit
     */
    public int getLimit() {
        return limit;
    }

    /**
     * Sets the limit to apply.
     *
     * @param limit the limit, which may be null
     * @return this
     * @mongodb.driver.manual reference/method/cursor.limit/#cursor.limit Limit
     */
    public FindOperation<T> limit(final int limit) {
        this.limit = limit;
        return this;
    }

    /**
     * Gets a document describing the fields to return for all matching documents.
     *
     * @return the project document, which may be null
     * @mongodb.driver.manual reference/method/db.collection.find/ Projection
     */
    public BsonDocument getProjection() {
        return projection;
    }

    /**
     * Sets a document describing the fields to return for all matching documents.
     *
     * @param projection the project document, which may be null.
     * @return this
     * @mongodb.driver.manual reference/method/db.collection.find/ Projection
     */
    public FindOperation<T> projection(final BsonDocument projection) {
        this.projection = projection;
        return this;
    }

    /**
     * Gets the number of documents to skip.  The default is 0.
     *
     * @return the number of documents to skip, which may be null
     * @mongodb.driver.manual reference/method/cursor.skip/#cursor.skip Skip
     */
    public int getSkip() {
        return skip;
    }

    /**
     * Sets the number of documents to skip.
     *
     * @param skip the number of documents to skip
     * @return this
     * @mongodb.driver.manual reference/method/cursor.skip/#cursor.skip Skip
     */
    public FindOperation<T> skip(final int skip) {
        this.skip = skip;
        return this;
    }

    /**
     * Gets the sort criteria to apply to the query. The default is null, which means that the documents will be returned in an undefined
     * order.
     *
     * @return a document describing the sort criteria
     * @mongodb.driver.manual reference/method/cursor.sort/ Sort
     */
    public BsonDocument getSort() {
        return sort;
    }

    /**
     * Sets the sort criteria to apply to the query.
     *
     * @param sort the sort criteria, which may be null.
     * @return this
     * @mongodb.driver.manual reference/method/cursor.sort/ Sort
     */
    public FindOperation<T> sort(final BsonDocument sort) {
        this.sort = sort;
        return this;
    }

    /**
     * Get the cursor type.
     *
     * @return the cursor type
     */
    public CursorType getCursorType() {
        return cursorType;
    }

    /**
     * Sets the cursor type.
     *
     * @param cursorType the cursor type
     * @return this
     */
    public FindOperation<T> cursorType(final CursorType cursorType) {
        this.cursorType = notNull("cursorType", cursorType);
        return this;
    }

    /**
     * Returns true if set to allowed to query non-primary replica set members.
     *
     * @return true if set to allowed to query non-primary replica set members.
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public boolean isSlaveOk() {
        return slaveOk;
    }

    /**
     * Sets if allowed to query non-primary replica set members.
     *
     * @param slaveOk true if allowed to query non-primary replica set members.
     * @return this
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public FindOperation<T> slaveOk(final boolean slaveOk) {
        this.slaveOk = slaveOk;
        return this;
    }

    /**
     * Internal replication use only.  Driver users should ordinarily not use this.
     *
     * @return oplogReplay
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public boolean isOplogReplay() {
        return oplogReplay;
    }

    /**
     * Internal replication use only.  Driver users should ordinarily not use this.
     *
     * @param oplogReplay the oplogReplay value
     * @return this
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public FindOperation<T> oplogReplay(final boolean oplogReplay) {
        this.oplogReplay = oplogReplay;
        return this;
    }

    /**
     * Returns true if cursor timeout has been turned off.
     *
     * <p>The server normally times out idle cursors after an inactivity period (10 minutes) to prevent excess memory use.</p>
     *
     * @return if cursor timeout has been turned off
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public boolean isNoCursorTimeout() {
        return noCursorTimeout;
    }

    /**
     * Sets if the cursor timeout should be turned off.
     *
     * @param noCursorTimeout true if the cursor timeout should be turned off.
     * @return this
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public FindOperation<T> noCursorTimeout(final boolean noCursorTimeout) {
        this.noCursorTimeout = noCursorTimeout;
        return this;
    }

    /**
     * Returns true if can get partial results from a mongos if some shards are down.
     *
     * @return if can get partial results from a mongos if some shards are down
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public boolean isPartial() {
        return partial;
    }

    /**
     * Sets if partial results from a mongos if some shards are down are allowed
     *
     * @param partial allow partial results from a mongos if some shards are down
     * @return this
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public FindOperation<T> partial(final boolean partial) {
        this.partial = partial;
        return this;
    }

    /**
     * Returns the collation options
     *
     * @return the collation options
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    public Collation getCollation() {
        return collation;
    }

    /**
     * Sets the collation options
     *
     * <p>A null value represents the server default.</p>
     * @param collation the collation options to use
     * @return this
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    public FindOperation<T> collation(final Collation collation) {
        this.collation = collation;
        return this;
    }

    /**
     * Returns the comment to send with the query. The default is not to include a comment with the query.
     *
     * @return the comment
     * @since 3.5
     */
    public String getComment() {
        return comment;
    }

    /**
     * Sets the comment to the query. A null value means no comment is set.
     *
     * @param comment the comment
     * @return this
     * @since 3.5
     */
    public FindOperation<T> comment(final String comment) {
        this.comment = comment;
        return this;
    }

    /**
     * Returns the hint for which index to use. The default is not to set a hint.
     *
     * @return the hint
     * @since 3.5
     */
    public BsonValue getHint() {
        return hint;
    }

    /**
     * Sets the hint for which index to use. A null value means no hint is set.
     *
     * @param hint the hint
     * @return this
     * @since 3.5
     */
    public FindOperation<T> hint(final BsonValue hint) {
        this.hint = hint;
        return this;
    }

    /**
     * Returns the exclusive upper bound for a specific index. By default there is no max bound.
     *
     * @return the max
     * @since 3.5
     */
    public BsonDocument getMax() {
        return max;
    }

    /**
     * Sets the exclusive upper bound for a specific index. A null value means no max is set.
     *
     * @param max the max
     * @return this
     * @since 3.5
     */
    public FindOperation<T> max(final BsonDocument max) {
        this.max = max;
        return this;
    }

    /**
     * Returns the minimum inclusive lower bound for a specific index. By default there is no min bound.
     *
     * @return the min
     * @since 3.5
     */
    public BsonDocument getMin() {
        return min;
    }

    /**
     * Sets the minimum inclusive lower bound for a specific index. A null value means no max is set.
     *
     * @param min the min
     * @return this
     * @since 3.5
     */
    public FindOperation<T> min(final BsonDocument min) {
        this.min = min;
        return this;
    }

    /**
     * Returns the returnKey. If true the find operation will return only the index keys in the resulting documents.
     *
     * Default value is false. If returnKey is true and the find command does not use an index, the returned documents will be empty.
     *
     * @return the returnKey
     * @since 3.5
     */
    public boolean isReturnKey() {
        return returnKey;
    }

    /**
     * Sets the returnKey. If true the find operation will return only the index keys in the resulting documents.
     *
     * @param returnKey the returnKey
     * @return this
     * @since 3.5
     */
    public FindOperation<T> returnKey(final boolean returnKey) {
        this.returnKey = returnKey;
        return this;
    }

    /**
     * Returns the showRecordId.
     *
     * Determines whether to return the record identifier for each document. If true, adds a field $recordId to the returned documents.
     * The default is false.
     *
     * @return the showRecordId
     * @since 3.5
     */
    public boolean isShowRecordId() {
        return showRecordId;
    }

    /**
     * Sets the showRecordId. Set to true to add a field {@code $recordId} to the returned documents.
     *
     * @param showRecordId the showRecordId
     * @return this
     * @since 3.5
     */
    public FindOperation<T> showRecordId(final boolean showRecordId) {
        this.showRecordId = showRecordId;
        return this;
    }

    /**
     * Enables retryable reads if a read fails due to a network error.
     *
     * @param retryReads true if reads should be retried
     * @return this
     * @since 3.11
     */
    public FindOperation<T> retryReads(final boolean retryReads) {
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
     * Returns the allowDiskUse value
     *
     * @return the allowDiskUse value
     */
    public Boolean isAllowDiskUse() {
        return allowDiskUse;
    }

    /**
     * Enables writing to temporary files on the server. When set to true, the server
     * can write temporary data to disk while executing the find operation.
     *
     * <p>This option is sent only if the caller explicitly sets it to true.</p>
     *
     * @param allowDiskUse the allowDiskUse
     * @return this
     */
    public FindOperation<T> allowDiskUse(@Nullable final Boolean allowDiskUse) {
        this.allowDiskUse = allowDiskUse;
        return this;
    }

    @Override
    public BatchCursor<T> execute(final ReadBinding binding) {
        return SyncOperationHelper.withReadConnectionSource(clientSideOperationTimeoutFactory.create(), binding,
                new CallableWithSource<BatchCursor<T>>() {
            @Override
            public BatchCursor<T> call(final ClientSideOperationTimeout clientSideOperationTimeout, final ConnectionSource source) {
                Connection connection = source.getConnection();
                if (serverIsAtLeastVersionThreeDotTwo(connection.getDescription())) {
                    try {
                        return executeCommandWithConnection(clientSideOperationTimeout, binding, source, namespace.getDatabaseName(),
                                getCommandCreator(binding.getSessionContext()),
                                CommandResultDocumentCodec.create(decoder, FIRST_BATCH),
                                transformer(), retryReads, connection);
                    } catch (MongoCommandException e) {
                        throw new MongoQueryException(e);
                    }
                } else {
                    try {
                        SyncOperationHelper.validateFindOptions(connection, binding.getSessionContext().getReadConcern(), collation,
                                allowDiskUse);
                        QueryResult<T> queryResult = connection.query(namespace,
                                asDocument(clientSideOperationTimeout, connection.getDescription(), binding.getReadPreference()),
                                projection,
                                skip,
                                limit,
                                batchSize,
                                isSlaveOk() || binding.getReadPreference().isSlaveOk(),
                                isTailableCursor(),
                                isAwaitData(),
                                isNoCursorTimeout(),
                                isPartial(),
                                isOplogReplay(),
                                decoder);
                        return new QueryBatchCursor<T>(clientSideOperationTimeout, queryResult, limit, batchSize,
                                decoder, source, connection);
                    } finally {
                        connection.release();
                    }
                }
            }
        });
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        AsyncOperationHelper.withAsyncReadConnection(clientSideOperationTimeoutFactory.create(), binding,
                new AsyncCallableWithConnectionAndSource() {
            @Override
            public void call(final ClientSideOperationTimeout clientSideOperationTimeout, final AsyncConnectionSource source,
                             final AsyncConnection connection, final Throwable t) {
                SingleResultCallback<AsyncBatchCursor<T>> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
                if (t != null) {
                    errHandlingCallback.onResult(null, t);
                } else {
                    if (serverIsAtLeastVersionThreeDotTwo(connection.getDescription())) {
                        final SingleResultCallback<AsyncBatchCursor<T>> wrappedCallback =
                                exceptionTransformingCallback(errHandlingCallback);
                        executeCommandAsyncWithConnection(clientSideOperationTimeout, binding, source, namespace.getDatabaseName(),
                                getCommandCreator(binding.getSessionContext()), CommandResultDocumentCodec.create(decoder, FIRST_BATCH),
                                asyncTransformer(), retryReads, connection, wrappedCallback);
                    } else {
                        final SingleResultCallback<AsyncBatchCursor<T>> wrappedCallback =
                                AsyncOperationHelper.releasingCallback(errHandlingCallback, source, connection);
                        AsyncOperationHelper.validateFindOptions(clientSideOperationTimeout, source, connection,
                                binding.getSessionContext().getReadConcern(), collation, allowDiskUse,
                                new AsyncCallableWithConnectionAndSource() {
                                    @Override
                                    public void call(final ClientSideOperationTimeout clientSideOperationTimeout,
                                                     final AsyncConnectionSource source, final AsyncConnection connection,
                                                     final Throwable t) {
                                        if (t != null) {
                                            wrappedCallback.onResult(null, t);
                                        } else {
                                            connection.queryAsync(namespace,
                                                    asDocument(clientSideOperationTimeout, connection.getDescription(),
                                                            binding.getReadPreference()), projection, skip, limit, batchSize,
                                                    isSlaveOk() || binding.getReadPreference().isSlaveOk(),
                                                    isTailableCursor(), isAwaitData(), isNoCursorTimeout(), isPartial(), isOplogReplay(),
                                                    decoder, new SingleResultCallback<QueryResult<T>>() {
                                                        @Override
                                                        public void onResult(final QueryResult<T> result, final Throwable t) {
                                                            if (t != null) {
                                                                wrappedCallback.onResult(null, t);
                                                            } else {
                                                                wrappedCallback.onResult(new AsyncQueryBatchCursor<T>(
                                                                        clientSideOperationTimeout, result, limit, batchSize, decoder,
                                                                        source, connection), null);
                                                            }
                                                        }
                                                    });
                                        }
                                    }
                        });
                    }
                }
            }
        });
    }

    private static <T> SingleResultCallback<T> exceptionTransformingCallback(final SingleResultCallback<T> callback) {
        return new SingleResultCallback<T>() {
            @Override
            public void onResult(final T result, final Throwable t) {
                if (t != null) {
                    if (t instanceof MongoCommandException) {
                        MongoCommandException commandException = (MongoCommandException) t;
                        callback.onResult(result, new MongoQueryException(commandException.getServerAddress(),
                                                                          commandException.getErrorCode(),
                                commandException.getErrorMessage()));
                    } else {
                        callback.onResult(result, t);
                    }
                } else {
                    callback.onResult(result, null);
                }
            }
        };
    }

    @Override
    public <R> ReadOperation<R> asExplainableOperation(@Nullable final ExplainVerbosity verbosity, final Decoder<R> resultDecoder) {
        return new CommandReadOperation<>(clientSideOperationTimeoutFactory, getNamespace().getDatabaseName(),
                asExplainCommandCreator(getCommandCreator(NoOpSessionContext.INSTANCE), verbosity), resultDecoder);
    }

    @Override
    public <R> AsyncReadOperation<R> asAsyncExplainableOperation(@Nullable final ExplainVerbosity verbosity,
                                                                 final Decoder<R> resultDecoder) {
        return new CommandReadOperation<>(clientSideOperationTimeoutFactory, getNamespace().getDatabaseName(),
                asExplainCommandCreator(getCommandCreator(NoOpSessionContext.INSTANCE), verbosity), resultDecoder);
    }

    private BsonDocument asDocument(final ClientSideOperationTimeout clientSideOperationTimeout,
                                    final ConnectionDescription connectionDescription, final ReadPreference readPreference) {
        BsonDocument document = new BsonDocument();

        if (sort != null) {
            document.put("$orderby", sort);
        }

        long maxTimeMS = clientSideOperationTimeout.getMaxTimeMS();
        if (maxTimeMS > 0) {
            document.put("$maxTimeMS", new BsonInt64(maxTimeMS));
        }
        if (connectionDescription.getServerType() == SHARD_ROUTER && !readPreference.equals(primary())) {
            document.put("$readPreference", readPreference.toDocument());
        }
        if (comment != null) {
            document.put("$comment", new BsonString(comment));
        }
        if (hint != null) {
            document.put("$hint", hint);
        }
        if (max != null) {
            document.put("$max", max);
        }
        if (min != null) {
            document.put("$min", min);
        }
        if (returnKey) {
            document.put("$returnKey", BsonBoolean.TRUE);
        }
        if (showRecordId) {
            document.put("$showDiskLoc", BsonBoolean.TRUE);
        }

        if (document.isEmpty()) {
            document = filter != null ? filter : new BsonDocument();
        } else if (filter != null) {
            document.put("$query", filter);
        } else if (!document.containsKey("$query")) {
            document.put("$query", new BsonDocument());
        }

        return document;
    }

    private CommandCreator getCommandCreator(final SessionContext sessionContext) {
        return (clientSideOperationTimeout, serverDescription, connectionDescription) -> {
            validateFindOptions(connectionDescription, sessionContext.getReadConcern(), collation, allowDiskUse);
            return getCommand(clientSideOperationTimeout, sessionContext);
        };
    }

    private BsonDocument getCommand(final ClientSideOperationTimeout clientSideOperationTimeout, final SessionContext sessionContext) {
        BsonDocument commandDocument = new BsonDocument("find", new BsonString(namespace.getCollectionName()));

        appendReadConcernToCommand(sessionContext, commandDocument);

        putIfNotNull(commandDocument, "filter", filter);
        putIfNotNullOrEmpty(commandDocument, "sort", sort);
        putIfNotNullOrEmpty(commandDocument, "projection", projection);
        if (skip > 0) {
            commandDocument.put("skip", new BsonInt32(skip));
        }
        if (limit != 0) {
            commandDocument.put("limit", new BsonInt32(Math.abs(limit)));
        }
        if (limit >= 0) {
            if (batchSize < 0 && Math.abs(batchSize) < limit) {
                commandDocument.put("limit", new BsonInt32(Math.abs(batchSize)));
            } else if (batchSize != 0) {
                commandDocument.put("batchSize", new BsonInt32(Math.abs(batchSize)));
            }
        }
        if (limit < 0 || batchSize < 0) {
            commandDocument.put("singleBatch", BsonBoolean.TRUE);
        }
        long maxTimeMS = clientSideOperationTimeout.getMaxTimeMS();
        if (maxTimeMS > 0) {
            commandDocument.put("maxTimeMS", new BsonInt64(maxTimeMS));
        }
        if (isTailableCursor()) {
            commandDocument.put("tailable", BsonBoolean.TRUE);
        }
        if (isAwaitData()) {
            commandDocument.put("awaitData", BsonBoolean.TRUE);
        }
        if (oplogReplay) {
            commandDocument.put("oplogReplay", BsonBoolean.TRUE);
        }
        if (noCursorTimeout) {
            commandDocument.put("noCursorTimeout", BsonBoolean.TRUE);
        }
        if (partial) {
            commandDocument.put("allowPartialResults", BsonBoolean.TRUE);
        }
        if (collation != null) {
            commandDocument.put("collation", collation.asDocument());
        }
        if (comment != null) {
            commandDocument.put("comment", new BsonString(comment));
        }
        if (hint != null) {
            commandDocument.put("hint", hint);
        }
        if (max != null) {
            commandDocument.put("max", max);
        }
        if (min != null) {
            commandDocument.put("min", min);
        }
        if (returnKey) {
            commandDocument.put("returnKey", BsonBoolean.TRUE);
        }
        if (showRecordId) {
            commandDocument.put("showRecordId", BsonBoolean.TRUE);
        }
        if (allowDiskUse != null) {
            commandDocument.put("allowDiskUse", BsonBoolean.valueOf(allowDiskUse));
        }
        return commandDocument;
    }

    private boolean isTailableCursor() {
        return cursorType.isTailable();
    }

    private boolean isAwaitData() {
        return cursorType == CursorType.TailableAwait;
    }

    private CommandReadTransformer<BsonDocument, AggregateResponseBatchCursor<T>> transformer() {
        return new CommandReadTransformer<BsonDocument, AggregateResponseBatchCursor<T>>() {
            @Override
            public AggregateResponseBatchCursor<T> apply(final ClientSideOperationTimeout clientSideOperationTimeout,
                                                         final ConnectionSource source, final Connection connection,
                                                         final BsonDocument result) {
                QueryResult<T> queryResult = cursorDocumentToQueryResult(result.getDocument("cursor"),
                        connection.getDescription().getServerAddress());
                return new QueryBatchCursor<T>(clientSideOperationTimeout, queryResult, limit, batchSize, decoder,
                        source, connection, result);
            }
        };
    }

    private CommandReadTransformerAsync<BsonDocument, AsyncBatchCursor<T>> asyncTransformer() {
        return new CommandReadTransformerAsync<BsonDocument, AsyncBatchCursor<T>>() {
            @Override
            public AsyncBatchCursor<T> apply(final ClientSideOperationTimeout clientSideOperationTimeout,
                                             final AsyncConnectionSource source, final AsyncConnection connection,
                                             final BsonDocument result) {
                QueryResult<T> queryResult = cursorDocumentToQueryResult(result.getDocument("cursor"),
                        connection.getDescription().getServerAddress());
                return new AsyncQueryBatchCursor<T>(clientSideOperationTimeout, queryResult, limit, batchSize, decoder, source,
                        connection, result);
            }
        };
    }
}
