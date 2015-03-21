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

import com.mongodb.CursorType;
import com.mongodb.ExplainVerbosity;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncConnectionSource;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.binding.ReadBinding;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.QueryResult;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.ServerType.SHARD_ROUTER;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.operation.OperationHelper.AsyncCallableWithConnectionAndSource;
import static com.mongodb.operation.OperationHelper.releasingCallback;
import static com.mongodb.operation.OperationHelper.withConnection;

/**
 * An operation that queries a collection using the provided criteria.
 *
 * @param <T> the operations result type.
 * @since 3.0
 */
public class FindOperation<T> implements AsyncReadOperation<AsyncBatchCursor<T>>, ReadOperation<BatchCursor<T>> {
    private final MongoNamespace namespace;
    private final Decoder<T> decoder;
    private BsonDocument filter;
    private int batchSize;
    private int limit;
    private BsonDocument modifiers;
    private BsonDocument projection;
    private long maxTimeMS;
    private int skip;
    private BsonDocument sort;
    private CursorType cursorType = CursorType.NonTailable;
    private boolean slaveOk;
    private boolean oplogReplay;
    private boolean noCursorTimeout;
    private boolean partial;

    /**
     * Construct a new instance.
     *
     * @param namespace the database and collection namespace for the operation.
     * @param decoder the decoder for the result documents.
     */
    public FindOperation(final MongoNamespace namespace, final Decoder<T> decoder) {
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
     * Gets the query modifiers to apply to this operation.  The default is not to apply any modifiers.
     *
     * @return the query modifiers, which may be null
     * @mongodb.driver.manual reference/operator/query-modifier/ Query Modifiers
     */
    public BsonDocument getModifiers() {
        return modifiers;
    }

    /**
     * Sets the query modifiers to apply to this operation.
     *
     * @param modifiers the query modifiers to apply, which may be null.
     * @return this
     * @mongodb.driver.manual reference/operator/query-modifier/ Query Modifiers
     */
    public FindOperation<T> modifiers(final BsonDocument modifiers) {
        this.modifiers = modifiers;
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
     * Gets the maximum execution time on the server for this operation.  The default is 0, which places no limit on the execution time.
     *
     * @param timeUnit the time unit to return the result in
     * @return the maximum execution time in the given time unit
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
     */
    public FindOperation<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
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

    @Override
    public BatchCursor<T> execute(final ReadBinding binding) {
        return withConnection(binding, new OperationHelper.CallableWithConnectionAndSource<BatchCursor<T>>() {
            @Override
            public BatchCursor<T> call(final ConnectionSource source, final Connection connection) {
                QueryResult<T> queryResult = connection.query(namespace,
                                                              asDocument(connection.getDescription(), binding.getReadPreference()),
                                                              projection,
                                                              getNumberToReturn(),
                                                              skip,
                                                              isSlaveOk() || binding.getReadPreference().isSlaveOk(),
                                                              isTailableCursor(),
                                                              isAwaitData(),
                                                              isNoCursorTimeout(),
                                                              isPartial(),
                                                              isOplogReplay(),
                                                              decoder);
                return new QueryBatchCursor<T>(queryResult, limit, batchSize, decoder, source);
            }
        });
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        withConnection(binding, new AsyncCallableWithConnectionAndSource() {
            @Override
            public void call(final AsyncConnectionSource source, final AsyncConnection connection, final Throwable t) {
                if (t != null) {
                    errorHandlingCallback(callback).onResult(null, t);
                } else {
                    final SingleResultCallback<AsyncBatchCursor<T>> wrappedCallback = releasingCallback(errorHandlingCallback(callback),
                                                                                                        source, connection);
                    connection.queryAsync(namespace, asDocument(connection.getDescription(), binding.getReadPreference()), projection,
                                          getNumberToReturn(), skip, isSlaveOk() || binding.getReadPreference().isSlaveOk(),
                                          isTailableCursor(), isAwaitData(), isNoCursorTimeout(), isPartial(), isOplogReplay(),
                                          decoder, new SingleResultCallback<QueryResult<T>>() {
                        @Override
                        public void onResult(final QueryResult<T> result, final Throwable t) {
                            if (t != null) {
                                wrappedCallback.onResult(null, t);
                            } else {
                                wrappedCallback.onResult(new AsyncQueryBatchCursor<T>(result, limit, batchSize, decoder, source), null);
                            }
                        }
                    });
                }
            }
        });
    }


    /**
     * Gets an operation whose execution explains this operation.
     *
     * @param explainVerbosity the explain verbosity
     * @return a read operation that when executed will explain this operation
     */
    public ReadOperation<BsonDocument> asExplainableOperation(final ExplainVerbosity explainVerbosity) {
        final FindOperation<BsonDocument> explainableFindOperation = createExplainableQueryOperation();
        return new ReadOperation<BsonDocument>() {
            @Override
            public BsonDocument execute(final ReadBinding binding) {
                return explainableFindOperation.execute(binding).next().iterator().next();
            }
        };
    }

    /**
     * Gets an operation whose execution explains this operation.
     *
     * @param explainVerbosity the explain verbosity
     * @return a read operation that when executed will explain this operation
     */
    public AsyncReadOperation<BsonDocument> asExplainableOperationAsync(final ExplainVerbosity explainVerbosity) {
        final FindOperation<BsonDocument> explainableFindOperation = createExplainableQueryOperation();
        return new AsyncReadOperation<BsonDocument>() {
            @Override
            public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<BsonDocument> callback) {
                explainableFindOperation.executeAsync(binding, new SingleResultCallback<AsyncBatchCursor<BsonDocument>>() {
                    @Override
                    public void onResult(final AsyncBatchCursor<BsonDocument> result, final Throwable t) {
                        if (t != null) {
                            callback.onResult(null, t);
                        } else {
                            result.next(new SingleResultCallback<List<BsonDocument>>() {
                                @Override
                                public void onResult(final List<BsonDocument> result, final Throwable t) {
                                    if (t != null) {
                                        callback.onResult(null, t);
                                    } else if (result.size() == 0) {
                                        callback.onResult(null, new MongoInternalException("Unexpected explain result size"));
                                    } else {
                                        callback.onResult(result.get(0), null);
                                    }
                                }
                            });
                        }
                    }
                });
            }
        };
    }

    private FindOperation<BsonDocument> createExplainableQueryOperation() {
        FindOperation<BsonDocument> explainFindOperation = new FindOperation<BsonDocument>(namespace, new BsonDocumentCodec());

        BsonDocument explainModifiers = new BsonDocument();
        if (modifiers != null) {
            explainModifiers.putAll(modifiers);
        }
        explainModifiers.append("$explain", BsonBoolean.TRUE);

        return explainFindOperation.filter(filter)
                             .projection(projection)
                             .sort(sort)
                             .skip(skip)
                             .limit(Math.abs(limit) * -1)
                             .modifiers(explainModifiers);

    }

    /**
     * Gets the limit of the number of documents in the first OP_REPLY response to the query.
     *
     * <p>A value of zero tells the server to use the default size. A negative value tells the server to return no more than that number
     * and immediately close the cursor.  Otherwise, the server will return no more than that number and return a cursorId to allow the
     * rest of the documents to be fetched, if it turns out there are more documents.</p>
     *
     * <p>The value returned by this method is based on the limit and the batch size, both of which can be positive, negative, or zero.</p>
     *
     * @return the value for numberToReturn in the OP_QUERY wire protocol message.
     * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    private int getNumberToReturn() {
        if (limit < 0) {
            return limit;
        } else if (limit == 0) {
            return batchSize;
        } else if (batchSize == 0) {
            return limit;
        } else if (limit < Math.abs(batchSize)) {
            return limit;
        } else {
            return batchSize;
        }
    }

    private BsonDocument asDocument(final ConnectionDescription connectionDescription, final ReadPreference readPreference) {
        BsonDocument document = modifiers != null ? modifiers : new BsonDocument();
        document.put("$query", filter != null ? filter : new BsonDocument());
        if (sort != null) {
            document.put("$orderby", sort);
        }

        if (maxTimeMS > 0) {
            document.put("$maxTimeMS", new BsonInt64(maxTimeMS));
        }

        if (connectionDescription.getServerType() == SHARD_ROUTER && !readPreference.equals(primary())) {
            document.put("$readPreference", readPreference.toDocument());
        }

        return document;
    }

    private boolean isTailableCursor() {
        return cursorType.isTailable();
    }

    private boolean isAwaitData() {
        return cursorType == CursorType.TailableAwait;
    }
}
