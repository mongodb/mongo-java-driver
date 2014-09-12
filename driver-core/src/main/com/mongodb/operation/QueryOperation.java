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
import com.mongodb.MongoTailableCursor;
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
public class QueryOperation<T> implements AsyncReadOperation<MongoAsyncCursor<T>>, ReadOperation<MongoTailableCursor<T>> {

    private final MongoNamespace namespace;
    private final Decoder<T> resultDecoder;
    private BsonDocument criteria;
    private Integer batchSize;
    private Integer limit;
    private BsonDocument modifiers;
    private BsonDocument projection;
    private EnumSet<CursorFlag> cursorFlags = EnumSet.noneOf(CursorFlag.class);
    private Long maxTimeMS;
    private Integer skip;
    private BsonDocument sort;

    public QueryOperation(final MongoNamespace namespace, final Find find, final Decoder<T> resultDecoder) {
        this.namespace = namespace;
        this.resultDecoder = resultDecoder;
        criteria = find.getFilter();
        batchSize = (find.getBatchSize());
        limit = (find.getLimit());
        projection = (find.getFields());
        maxTimeMS = (find.getOptions().getMaxTimeMS());
        limit = (find.getLimit());
        skip = (find.getSkip());
        sort = (find.getOrder());
        cursorFlags = (find.getFlags(primary()));
        modifiers = new BsonDocument();
        addToModifiers(find, modifiers);
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

    public void setBatchSize(final Integer batchSize) {
        this.batchSize = batchSize;
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

    public EnumSet<CursorFlag> getCursorFlags() {
        return cursorFlags;
    }

    public void setCursorFlags(final EnumSet<CursorFlag> cursorFlags) {
        this.cursorFlags = cursorFlags;
    }

    public Long getMaxTimeMS() {
        return maxTimeMS;
    }

    public void setMaxTimeMS(final Long maxTimeMS) {
        this.maxTimeMS = maxTimeMS;
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
    public MongoTailableCursor<T> execute(final ReadBinding binding) {
        return withConnection(binding, new OperationHelper.CallableWithConnectionAndSource<MongoTailableCursor<T>>() {
            @Override
            public MongoTailableCursor<T> call(final ConnectionSource source, final Connection connection) {
                QueryResult<T> queryResult = asQueryProtocol(connection.getServerDescription(), binding.getReadPreference())
                                             .execute(connection);
                if (isExhaustCursor()) {
                    return new MongoQueryCursor<T>(namespace, queryResult, getLimit(), getBatchSize(),
                                                   resultDecoder, connection);
                } else {
                    return new MongoQueryCursor<T>(namespace, queryResult, getLimit(), getBatchSize(),
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
                                          future.init(new MongoAsyncQueryCursor<T>(namespace, queryResult, getLimit(),
                                                                                   getBatchSize(), resultDecoder,
                                                                                   connection), null);
                                      } else {
                                          future.init(new MongoAsyncQueryCursor<T>(namespace, queryResult, getLimit(),
                                                                                   getBatchSize(), resultDecoder,
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
        return new QueryProtocol<T>(namespace, getFlags(readPreference), getSkip(),
                                    getNumberToReturn(), asDocument(serverDescription, readPreference),
                                    projection, resultDecoder);
    }

    private int getBatchSize() {
        return batchSize != null ? batchSize : 0;
    }

    private int getLimit() {
        return limit != null ? limit : 0;
    }

    private int getSkip() {
        return skip != null ? skip : 0;
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
        if (getLimit() < 0) {
            return getLimit();
        } else if (getLimit() == 0) {
            return getBatchSize();
        } else if (getBatchSize() == 0) {
            return getLimit();
        } else if (getLimit() < Math.abs(getBatchSize())) {
            return getLimit();
        } else {
            return getBatchSize();
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
        document.put("$query", criteria != null ? criteria : new BsonDocument());
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