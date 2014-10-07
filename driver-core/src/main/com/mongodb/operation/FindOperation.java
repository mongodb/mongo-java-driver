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

import com.mongodb.Block;
import com.mongodb.ExplainVerbosity;
import com.mongodb.MongoCursor;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.async.MongoAsyncCursor;
import com.mongodb.async.MongoFuture;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.SingleResultFuture;
import com.mongodb.binding.AsyncConnectionSource;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.binding.ReadBinding;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.protocol.QueryProtocol;
import com.mongodb.protocol.QueryResult;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;

import java.util.concurrent.TimeUnit;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.ServerType.SHARD_ROUTER;
import static com.mongodb.operation.OperationHelper.withConnection;

/**
 * An operation that queries a collection using the provided criteria.
 *
 * @param <T> the operations result type.
 * @since 3.0
 */
public class FindOperation<T> implements AsyncReadOperation<MongoAsyncCursor<T>>, ReadOperation<MongoCursor<T>> {
    private final MongoNamespace namespace;
    private final Decoder<T> decoder;
    private BsonDocument criteria;
    private int batchSize;
    private int limit;
    private BsonDocument modifiers;
    private BsonDocument projection;
    private long maxTimeMS;
    private int skip;
    private BsonDocument sort;
    private boolean tailableCursor;
    private boolean slaveOk;
    private boolean oplogReplay;
    private boolean noCursorTimeout;
    private boolean awaitData;
    private boolean exhaust;
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
     * Gets the query criteria.
     *
     * @return the query criteria
     * @mongodb.driver.manual manual/reference/method/db.collection.find/ Criteria
     */
    public BsonDocument getCriteria() {
        return criteria;
    }

    /**
     * Sets the criteria to apply to the query.
     *
     * @param criteria the criteria, which may be null.
     * @return this
     * @mongodb.driver.manual manual/reference/method/db.collection.find/ Criteria
     */
    public FindOperation<T> criteria(final BsonDocument criteria) {
        this.criteria = criteria;
        return this;
    }

    /**
     * Gets the number of documents to return per batch.  Default to 0, which indicates that the server chooses an appropriate batch size.
     *
     * @return the batch size, which may be null
     * @mongodb.driver.manual manual/reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Sets the number of documents to return per batch.
     *
     * @param batchSize the batch size
     * @return this
     * @mongodb.driver.manual manual/reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    public FindOperation<T> batchSize(final int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    /**
     * Gets the limit to apply.  The default is null.
     *
     * @return the limit
     * @mongodb.driver.manual manual/reference/method/cursor.limit/#cursor.limit Limit
     */
    public int getLimit() {
        return limit;
    }

    /**
     * Sets the limit to apply.
     *
     * @param limit the limit, which may be null
     * @return this
     * @mongodb.driver.manual manual/reference/method/cursor.limit/#cursor.limit Limit
     */
    public FindOperation<T> limit(final int limit) {
        this.limit = limit;
        return this;
    }

    /**
     * Gets the query modifiers to apply to this operation.  The default is not to apply any modifiers.
     *
     * @return the query modifiers, which may be null
     * @mongodb.driver.manual manual/reference/operator/query-modifier/ Query Modifiers
     */
    public BsonDocument getModifiers() {
        return modifiers;
    }

    /**
     * Sets the query modifiers to apply to this operation.
     *
     * @param modifiers the query modifiers to apply, which may be null.
     * @return this
     * @mongodb.driver.manual manual/reference/operator/query-modifier/ Query Modifiers
     */
    public FindOperation<T> modifiers(final BsonDocument modifiers) {
        this.modifiers = modifiers;
        return this;
    }

    /**
     * Gets a document describing the fields to return for all matching documents.
     *
     * @return the project document, which may be null
     * @mongodb.driver.manual manual/reference/method/db.collection.find/ Projection
     */
    public BsonDocument getProjection() {
        return projection;
    }

    /**
     * Sets a document describing the fields to return for all matching documents.
     *
     * @param projection the project document, which may be null.
     * @return this
     * @mongodb.driver.manual manual/reference/method/db.collection.find/ Projection
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
     * @mongodb.driver.manual manual/reference/method/cursor.skip/#cursor.skip Skip
     */
    public int getSkip() {
        return skip;
    }

    /**
     * Sets the number of documents to skip.
     *
     * @param skip the number of documents to skip
     * @return this
     * @mongodb.driver.manual manual/reference/method/cursor.skip/#cursor.skip Skip
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
     * @mongodb.driver.manual manual/reference/method/cursor.sort/ Sort
     */
    public BsonDocument getSort() {
        return sort;
    }

    /**
     * Sets the sort criteria to apply to the query.
     *
     * @param sort the sort criteria, which may be null.
     * @return this
     * @mongodb.driver.manual manual/reference/method/cursor.sort/ Sort
     */
    public FindOperation<T> sort(final BsonDocument sort) {
        this.sort = sort;
        return this;
    }

    /**
     * Gets whether the cursor is configured to be a tailable cursor.
     *
     * <p>Tailable means the cursor is not closed when the last data is retrieved. Rather, the cursor marks the final object's position. You
     * can resume using the cursor later, from where it was located, if more data were received. Like any "latent cursor",
     * the cursor may become invalid at some point - for example if the final object it references were deleted.</p>
     *
     * @return true if the cursor is configured to be a tailable cursor
     * @mongodb.driver.manual meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public boolean isTailableCursor() {
        return tailableCursor;
    }

    /**
     * Sets whether the cursor should be a tailable cursor.
     *
     * <p>Tailable means the cursor is not closed when the last data is retrieved. Rather, the cursor marks the final object's position. You
     * can resume using the cursor later, from where it was located, if more data were received. Like any "latent cursor",
     * the cursor may become invalid at some point - for example if the final object it references were deleted.</p>
     *
     * @param tailableCursor whether the cursor should be a tailable cursor.
     * @return this
     * @mongodb.driver.manual meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public FindOperation<T> tailableCursor(final boolean tailableCursor) {
        this.tailableCursor = tailableCursor;
        return this;
    }

    /**
     * Returns true if set to allowed to query non-primary replica set members.
     *
     * @return true if set to allowed to query non-primary replica set members.
     * @mongodb.driver.manual meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public boolean isSlaveOk() {
        return slaveOk;
    }

    /**
     * Sets if allowed to query non-primary replica set members.
     *
     * @param slaveOk true if allowed to query non-primary replica set members.
     * @return this
     * @mongodb.driver.manual meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public FindOperation<T> slaveOk(final boolean slaveOk) {
        this.slaveOk = slaveOk;
        return this;
    }

    /**
     * Internal replication use only.  Driver users should ordinarily not use this.
     *
     * @return oplogReplay
     * @mongodb.driver.manual meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public boolean isOplogReplay() {
        return oplogReplay;
    }

    /**
     * Internal replication use only.  Driver users should ordinarily not use this.
     *
     * @param oplogReplay the oplogReplay value
     * @return this
     * @mongodb.driver.manual meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
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
     * @mongodb.driver.manual meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public boolean isNoCursorTimeout() {
        return noCursorTimeout;
    }

    /**
     * Sets if the cursor timeout should be turned off.
     *
     * @param noCursorTimeout true if the cursor timeout should be turned off.
     * @return this
     * @mongodb.driver.manual meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public FindOperation<T> noCursorTimeout(final boolean noCursorTimeout) {
        this.noCursorTimeout = noCursorTimeout;
        return this;
    }

    /**
     * Returns true if the cursor should await for data.
     *
     * <p>Use with {@link #tailableCursor}. If we are at the end of the data, block for a while rather than returning no data. After a
     * timeout period, we do return as normal.</p>
     *
     * @return if the cursor should await for data
     * @mongodb.driver.manual meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public boolean isAwaitData() {
        return awaitData;
    }

    /**
     * Sets if the cursor should await for data.
     *
     * <p>Use with {@link #tailableCursor}. If we are at the end of the data, block for a while rather than returning no data. After a
     * timeout period, we do return as normal.</p>
     *
     * @param awaitData if we should await for data
     * @return this
     * @mongodb.driver.manual meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public FindOperation<T> awaitData(final boolean awaitData) {
        this.awaitData = awaitData;
        return this;
    }

    /**
     * Gets if cursor should get all the data immediately.
     *
     * <p>Stream the data down full blast in multiple "more" packages, on the assumption that the client will fully read all data
     * queried. Faster when you are pulling a lot of data and know you want to pull it all down</p>
     *
     * @return if cursor should get all the data immediately
     * @mongodb.driver.manual meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public boolean isExhaust() {
        return exhaust;
    }

    /**
     * Should the cursor get all the data immediately.
     *
     * <p>Stream the data down full blast in multiple "more" packages, on the assumption that the client will fully read all data
     * queried. Faster when you are pulling a lot of data and know you want to pull it all down</p>
     *
     * @param exhaust should the cursor get all the data immediately.
     * @return this
     * @mongodb.driver.manual meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public FindOperation<T> exhaust(final boolean exhaust) {
        this.exhaust = exhaust;
        return this;
    }

    /**
     * Returns true if can get partial results from a mongos if some shards are down.
     *
     * @return if can get partial results from a mongos if some shards are down
     * @mongodb.driver.manual meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public boolean isPartial() {
        return partial;
    }

    /**
     * Sets if partial results from a mongos if some shards are down are allowed
     *
     * @param partial allow partial results from a mongos if some shards are down
     * @return this
     * @mongodb.driver.manual meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
     */
    public FindOperation<T> partial(final boolean partial) {
        this.partial = partial;
        return this;
    }

    @Override
    public MongoCursor<T> execute(final ReadBinding binding) {
        return withConnection(binding, new OperationHelper.CallableWithConnectionAndSource<MongoCursor<T>>() {
            @Override
            public MongoCursor<T> call(final ConnectionSource source, final Connection connection) {
                QueryResult<T> queryResult = asQueryProtocol(connection.getDescription(), binding.getReadPreference())
                                             .execute(connection);
                if (isExhaust()) {
                    return new MongoQueryCursor<T>(namespace, queryResult, limit, batchSize,
                                                   decoder, connection);
                } else {
                    return new MongoQueryCursor<T>(namespace, queryResult, limit, batchSize,
                                                   decoder, source);
                }
            }
        });
    }

    @Override
    public MongoFuture<MongoAsyncCursor<T>> executeAsync(final AsyncReadBinding binding) {
        return withConnection(binding, new OperationHelper.AsyncCallableWithConnectionAndSource<MongoAsyncCursor<T>>() {
            @Override
            public MongoFuture<MongoAsyncCursor<T>> call(final AsyncConnectionSource source, final Connection connection) {
                final SingleResultFuture<MongoAsyncCursor<T>> future = new SingleResultFuture<MongoAsyncCursor<T>>();
                asQueryProtocol(connection.getDescription(), binding.getReadPreference())
                .executeAsync(connection)
                .register(new SingleResultCallback<QueryResult<T>>() {
                              @Override
                              public void onResult(final QueryResult<T> queryResult, final MongoException e) {
                                  if (e != null) {
                                      future.init(null, e);
                                  } else {
                                      if (isExhaust()) {
                                          future.init(new MongoAsyncQueryCursor<T>(namespace, queryResult, limit,
                                                                                   batchSize, decoder,
                                                                                   connection), null);
                                      } else {
                                          future.init(new MongoAsyncQueryCursor<T>(namespace, queryResult, limit,
                                                                                   batchSize, decoder,
                                                                                   source), null);
                                      }
                                  }
                              }
                          }
                         );
                return future;
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
                return explainableFindOperation.execute(binding).next();
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
            public MongoFuture<BsonDocument> executeAsync(final AsyncReadBinding binding) {
                final SingleResultFuture<BsonDocument> retVal = new SingleResultFuture<BsonDocument>();
                explainableFindOperation.executeAsync(binding).register(new SingleResultCallback<MongoAsyncCursor<BsonDocument>>() {
                    @Override
                    public void onResult(final MongoAsyncCursor<BsonDocument> cursor, final MongoException e) {
                        if (e != null) {
                            retVal.init(null, e);
                        } else {
                            cursor.forEach(new Block<BsonDocument>() {
                                @Override
                                public void apply(final BsonDocument explainDocument) {
                                    retVal.init(explainDocument, null);
                                }
                            });
                        }
                    }
                });
                return retVal;
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

        return explainFindOperation.criteria(criteria)
                             .projection(projection)
                             .sort(sort)
                             .skip(skip)
                             .limit(limit)
                             .modifiers(explainModifiers);

    }

    private QueryProtocol<T> asQueryProtocol(final ConnectionDescription connectionDescription, final ReadPreference readPreference) {
        return new QueryProtocol<T>(namespace, skip, getNumberToReturn(), asDocument(connectionDescription, readPreference), projection,
                                    decoder).tailableCursor(isTailableCursor())
                                            .slaveOk(isSlaveOk() || readPreference.isSlaveOk())
                                            .oplogReplay(isOplogReplay())
                                            .noCursorTimeout(isNoCursorTimeout())
                                            .awaitData(isAwaitData())
                                            .exhaust(isExhaust())
                                            .partial(isPartial());
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
     * @mongodb.driver.manual meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
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

    private int getFlags(final ReadPreference readPreference) {
        int flags = 0;
        if (isTailableCursor()) {
            flags |= 1 << 1;
        }
        if (isSlaveOk() || readPreference.isSlaveOk()){
            flags |= 1 << 2;
        }
        if (isOplogReplay()){
            flags |= 1 << 3;
        }
        if (isNoCursorTimeout()){
            flags |= 1 << 4;
        }
        if (isAwaitData()){
            flags |= 1 << 5;
        }
        if (isExhaust()) {
            flags |= 1 << 6;
        }
        if (isPartial()) {
            flags |= 1 << 7;
        }
        return flags;
    }

    private BsonDocument asDocument(final ConnectionDescription connectionDescription, final ReadPreference readPreference) {
        BsonDocument document = modifiers != null ? modifiers : new BsonDocument();
        document.put("$query", criteria != null ? criteria : new BsonDocument());
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
}
