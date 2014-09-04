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

import com.mongodb.CursorFlag;
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
import com.mongodb.connection.ServerDescription;
import com.mongodb.protocol.QueryProtocol;
import com.mongodb.protocol.QueryResult;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.codecs.Decoder;

import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.ServerType.SHARD_ROUTER;
import static com.mongodb.operation.OperationHelper.withConnection;

/**
 * An operation that queries a collection using the provided criteria.
 *
 * @param <T> the document type
 * @since 3.0
 */
public class QueryOperation<T> implements AsyncReadOperation<MongoAsyncCursor<T>>, ReadOperation<MongoCursor<T>> {
    private final MongoNamespace namespace;
    private final Decoder<T> decoder;
    private BsonDocument criteria;
    private int batchSize;
    private int limit;
    private BsonDocument modifiers;
    private BsonDocument projection;
    private EnumSet<CursorFlag> cursorFlags = EnumSet.noneOf(CursorFlag.class);
    private long maxTimeMS;
    private int skip;
    private BsonDocument sort;

    /**
     * Construct a new instance.
     *
     * @param namespace the namespace to execute the query in
     * @param decoder the decoder to decode the results with
     */
    public QueryOperation(final MongoNamespace namespace, final Decoder<T> decoder) {
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
     * @mongodb.driver.manual manual/reference/method/db.collection.find/ Criteria
     */
    public void setCriteria(final BsonDocument criteria) {
        this.criteria = criteria;
    }

    /**
     * Gets the number of documents to return per batch.  Default to 0, which indicates that the server chooses an appropriate batch
     * size.
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
     * @mongodb.driver.manual manual/reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    public void setBatchSize(final int batchSize) {
        this.batchSize = batchSize;
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
     * @mongodb.driver.manual manual/reference/method/cursor.limit/#cursor.limit Limit
     */
    public void setLimit(final int limit) {
        this.limit = limit;
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
     * @mongodb.driver.manual manual/reference/operator/query-modifier/ Query Modifiers
     */
    public void setModifiers(final BsonDocument modifiers) {
        this.modifiers = modifiers;
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
     * @mongodb.driver.manual manual/reference/method/db.collection.find/ Projection
     */
    public void setProjection(final BsonDocument projection) {
        this.projection = projection;
    }

    /**
     * Gets the cursor flags.
     *
     * @return the cursor flags
     */
    public EnumSet<CursorFlag> getCursorFlags() {
        return cursorFlags;
    }

    /**
     * Sets the cursor flags.
     *
     * @param cursorFlags the cursor flags
     */
    public void setCursorFlags(final EnumSet<CursorFlag> cursorFlags) {
        this.cursorFlags = cursorFlags;
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
     */
    public void setMaxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
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
     * @mongodb.driver.manual manual/reference/method/cursor.skip/#cursor.skip Skip
     */
    public void setSkip(final int skip) {
        this.skip = skip;
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
     * @mongodb.driver.manual manual/reference/method/cursor.sort/ Sort
     */
    public void setSort(final BsonDocument sort) {
        this.sort = sort;
    }

    @Override
    public MongoCursor<T> execute(final ReadBinding binding) {
        return withConnection(binding, new OperationHelper.CallableWithConnectionAndSource<MongoCursor<T>>() {
            @Override
            public MongoCursor<T> call(final ConnectionSource source, final Connection connection) {
                QueryResult<T> queryResult = asQueryProtocol(connection.getServerDescription(), binding.getReadPreference())
                                             .execute(connection);
                if (isExhaustCursor()) {
                    return new MongoQueryCursor<T>(namespace, queryResult, limit, batchSize,
                                                   decoder, connection);
                } else {
                    return new MongoQueryCursor<T>(namespace, queryResult, limit, batchSize,
                                                   decoder, source);
                }
            }
        });
    }

    public MongoFuture<MongoAsyncCursor<T>> executeAsync(final AsyncReadBinding binding) {
        return withConnection(binding, new OperationHelper.AsyncCallableWithConnectionAndSource<MongoAsyncCursor<T>>() {
            @Override
            public MongoFuture<MongoAsyncCursor<T>> call(final AsyncConnectionSource source, final Connection connection) {
                final SingleResultFuture<MongoAsyncCursor<T>> future = new SingleResultFuture<MongoAsyncCursor<T>>();
                asQueryProtocol(connection.getServerDescription(), binding.getReadPreference())
                .executeAsync(connection)
                .register(new SingleResultCallback<QueryResult<T>>() {
                              @Override
                              public void onResult(final QueryResult<T> queryResult, final MongoException e) {
                                  if (e != null) {
                                      future.init(null, e);
                                  } else {
                                      if (isExhaustCursor()) {
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

    private QueryProtocol<T> asQueryProtocol(final ServerDescription serverDescription, final ReadPreference readPreference) {
        return new QueryProtocol<T>(namespace, getFlags(readPreference), skip,
                                    getNumberToReturn(), asDocument(serverDescription, readPreference),
                                    projection, decoder);
    }

    /**
     * Gets the limit of the number of documents in the first OP_REPLY response to the query. A value of zero tells the server to use the
     * default size. A negative value tells the server to return no more than that number and immediately close the cursor.  Otherwise, the
     * server will return no more than that number and return a cursorId to allow the rest of the documents to be fetched, if it turns out
     * there are more documents.
     * <p/>
     * The value returned by this method is based on the limit and the batch size, both of which can be positive, negative, or zero.
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

    private EnumSet<CursorFlag> getFlags(final ReadPreference readPreference) {
        if (readPreference.isSlaveOk()) {
            EnumSet<CursorFlag> retVal = EnumSet.copyOf(cursorFlags);
            retVal.add(CursorFlag.SLAVE_OK);
            return retVal;
        } else {
            return cursorFlags;
        }
    }

    private BsonDocument asDocument(final ServerDescription serverDescription, final ReadPreference readPreference) {
        BsonDocument document = modifiers != null ? modifiers : new BsonDocument();
        document.put("$query", criteria != null ? criteria : new BsonDocument());
        if (sort != null) {
            document.put("$orderby", sort);
        }

        if (maxTimeMS > 0) {
            document.put("$maxTimeMS", new BsonInt64(maxTimeMS));
        }

        if (serverDescription.getType() == SHARD_ROUTER && !readPreference.equals(primary())) {
            document.put("$readPreference", readPreference.toDocument());
        }

        return document;
    }


    private boolean isExhaustCursor() {
        return cursorFlags.contains(CursorFlag.EXHAUST);
    }
}