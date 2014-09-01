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
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.codecs.Decoder;

import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.ServerType.SHARD_ROUTER;
import static com.mongodb.operation.OperationHelper.withConnection;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * An operation that queries a collection using the provided criteria.
 *
 * @param <T> the document type
 * @since 3.0
 */
public class QueryOperation<T> implements AsyncReadOperation<MongoAsyncCursor<T>>, ReadOperation<MongoCursor<T>> {
    private final MongoNamespace namespace;
    private final Decoder<T> resultDecoder;

    private BsonDocument criteria;
    private BsonDocument projection;
    private BsonDocument sort;
    private Integer skip;
    private Integer limit;
    private EnumSet<CursorFlag> cursorFlags = EnumSet.noneOf(CursorFlag.class);
    private BsonDocument modifiers;

    private Integer batchSize;
    private Long maxTimeMS;

    public QueryOperation(final MongoNamespace namespace, final Find find, final Decoder<T> resultDecoder) {
        this(namespace, resultDecoder);
        this.criteria = find.getFilter();
        this.batchSize = find.getBatchSize();
        this.limit = find.getLimit();
        this.projection = find.getFields();
        this.maxTimeMS = find.getOptions().getMaxTimeMS();
        this.skip = find.getSkip();
        this.sort = find.getOrder();
        this.modifiers = new BsonDocument();
        addToModifiers(find);
    }

    public QueryOperation(final MongoNamespace namespace, final Decoder<T> resultDecoder) {
        this.namespace = notNull("namespace", namespace);
        this.resultDecoder = notNull("resultDecoder", resultDecoder);
    }

    public MongoNamespace getNamespace() {
        return namespace;
    }

    public Decoder<T> getResultDecoder() {
        return resultDecoder;
    }

    public BsonDocument getCriteria() {
        return criteria;
    }

    public void setCriteria(final BsonDocument criteria) {
        this.criteria = criteria;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(final Integer batchSize) {
        this.batchSize = batchSize;
    }

    public Integer getLimit() {
        return limit;
    }

    public void setLimit(final Integer limit) {
        this.limit = limit;
    }

    public BsonDocument getModifiers() {
        return modifiers;
    }

    public void setModifiers(final BsonDocument modifiers) {
        this.modifiers = modifiers;
    }

    public BsonDocument getProjection() {
        return projection;
    }

    public void setProjection(final BsonDocument projection) {
        this.projection = projection;
    }

    public Long getMaxTime(TimeUnit timeUnit) {
        if (maxTimeMS == null) {
            return null;
        }
        return timeUnit.convert(maxTimeMS, MILLISECONDS);
    }

    public void setMaxTime(final Long maxTime, final TimeUnit timeUnit) {
        if (maxTime == null) {
            this.maxTimeMS = null;
        } else {
            this.maxTimeMS = MILLISECONDS.convert(maxTime, timeUnit);
        }
    }

    public EnumSet<CursorFlag> getCursorFlags() {
        return cursorFlags;
    }

    public void setCursorFlags(final EnumSet<CursorFlag> cursorFlags) {
        this.cursorFlags = notNull("cursorFlags", cursorFlags);
    }

    public Integer getSkip() {
        return skip;
    }

    public void setSkip(final Integer skip) {
        this.skip = skip;
    }

    public BsonDocument getSort() {
        return sort;
    }

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
                    return new MongoQueryCursor<T>(namespace, queryResult, limit != null ? limit : 0, batchSize != null ? batchSize : 0,
                                                   resultDecoder, connection);
                } else {
                    return new MongoQueryCursor<T>(namespace, queryResult, limit != null ? limit : 0, batchSize != null ? batchSize : 0,
                                                   resultDecoder, source);
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
                                          future.init(new MongoAsyncQueryCursor<T>(namespace, queryResult, limit != null ? limit : 0,
                                                                                   batchSize != null ? batchSize : 0, resultDecoder,
                                                                                   connection), null);
                                      } else {
                                          future.init(new MongoAsyncQueryCursor<T>(namespace, queryResult, limit != null ? limit : 0,
                                                                                   batchSize != null ? batchSize : 0, resultDecoder,
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
        return new QueryProtocol<T>(namespace, getFlags(readPreference), skip != null ? skip : 0,
                                    getNumberToReturn(), asDocument(serverDescription, readPreference),
                                    projection, resultDecoder);
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
        int intLimit = this.limit != null ? this.limit : 0;
        int intBatchSize = this.batchSize != null ? this.batchSize : 0;
        if (intLimit < 0) {
            return intLimit;
        } else if (intLimit == 0) {
            return intBatchSize;
        } else if (intBatchSize == 0) {
            return intLimit;
        } else if (intLimit < Math.abs(intBatchSize)) {
            return intLimit;
        } else {
            return intBatchSize;
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

    private void addToModifiers(final Find find, final BsonDocument modifiers) {
        if (find.isSnapshotMode()) {
            modifiers.put("$snapshot", BsonBoolean.TRUE);
        }
        if (find.isExplain()) {
            modifiers.put("$explain", BsonBoolean.TRUE);
        }
        if (find.getHint() != null) {
            modifiers.put("$hint", find.getHint());
        }

        if (find.getOptions().getComment() != null) {
            modifiers.put("$comment", new BsonString(find.getOptions().getComment()));
        }

        if (find.getOptions().getMax() != null) {
            modifiers.put("$max", find.getOptions().getMax());
        }

        if (find.getOptions().getMin() != null) {
            modifiers.put("$min", find.getOptions().getMin());
        }

        if (find.getOptions().isReturnKey()) {
            modifiers.put("$returnKey", BsonBoolean.TRUE);
        }

        if (find.getOptions().isShowDiskLoc()) {
            modifiers.put("$showDiskLoc", BsonBoolean.TRUE);
        }

        int maxScan = find.getOptions().getMaxScan();
        if (maxScan > 0) {
            modifiers.put("$maxScan", new BsonInt32(maxScan));
        }
    }

    private BsonDocument asDocument(final ServerDescription serverDescription, final ReadPreference readPreference) {
        BsonDocument document = modifiers != null ? modifiers : new BsonDocument();
        document.put("$query", criteria == null ? new BsonDocument() : criteria);
        if (sort != null) {
            document.put("$orderby", sort);
        }

        if (maxTimeMS != null && maxTimeMS > 0) {
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