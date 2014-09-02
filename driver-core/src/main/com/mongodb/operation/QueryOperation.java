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
import com.mongodb.client.model.FindModel;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ServerDescription;
import com.mongodb.protocol.QueryProtocol;
import com.mongodb.protocol.QueryResult;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.codecs.Decoder;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.EnumSet;

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
    private final FindModel model;
    private final Decoder<T> resultDecoder;
    private final CodecRegistry codecRegistry;

    public QueryOperation(final MongoNamespace namespace, final Find find, final Decoder<T> resultDecoder) {
        this.namespace = namespace;
        this.resultDecoder = resultDecoder;
        this.codecRegistry = null;
        model = new FindModel(find.getFilter());
        model.batchSize(find.getBatchSize());
        model.limit(find.getLimit());
        model.projection(find.getFields());
        model.maxTimeMS(find.getOptions().getMaxTimeMS(), MILLISECONDS);
        model.limit(find.getLimit());
        model.skip(find.getSkip());
        model.sort(find.getOrder());
        BsonDocument modifiers = new BsonDocument();
        addToModifiers(find, modifiers);
        model.modifiers(modifiers);
    }

    public QueryOperation(final MongoNamespace namespace, final FindModel model, final CodecRegistry codecRegistry,
                          final Decoder<T> resultDecoder) {
        this.model = model;
        this.codecRegistry = codecRegistry;
        this.namespace = notNull("namespace", namespace);
        this.resultDecoder = notNull("resultDecoder", resultDecoder);
    }

    public MongoNamespace getNamespace() {
        return namespace;
    }

    public FindModel getModel() {
        return model;
    }

    public Decoder<T> getResultDecoder() {
        return resultDecoder;
    }


    @Override
    public MongoCursor<T> execute(final ReadBinding binding) {
        return withConnection(binding, new OperationHelper.CallableWithConnectionAndSource<MongoCursor<T>>() {
            @Override
            public MongoCursor<T> call(final ConnectionSource source, final Connection connection) {
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

    private int getBatchSize() {
        return model.getBatchSize() != null ? model.getBatchSize() : 0;
    }

    private int getLimit() {
        return model.getLimit() != null ? model.getLimit() : 0;
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
                                    asBson(model.getProjection()), resultDecoder);
    }

    private int getSkip() {
        return model.getSkip() != null ? model.getSkip() : 0;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private BsonDocument asBson(Object document) {
        if (document == null) {
            return null;
        }
        if (document instanceof BsonDocument) {
            return (BsonDocument) document;
        } else {
            return new BsonDocumentWrapper(document, codecRegistry.get(document.getClass()));
        }
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
            EnumSet<CursorFlag> retVal = EnumSet.copyOf(model.getCursorFlags());
            retVal.add(CursorFlag.SLAVE_OK);
            return retVal;
        } else {
            return model.getCursorFlags();
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
        BsonDocument document = model.getModifiers() != null ? asBson(model.getModifiers()) : new BsonDocument();
        document.put("$query", model.getCriteria() != null ? asBson(model.getCriteria()) : new BsonDocument());
        if (model.getSort() != null) {
            document.put("$orderby", asBson(model.getSort()));
        }

        if (model.getMaxTime(MILLISECONDS) != null && model.getMaxTime(MILLISECONDS) > 0) {
            document.put("$maxTimeMS", new BsonInt64(model.getMaxTime(MILLISECONDS)));
        }

        if (serverDescription.getType() == SHARD_ROUTER && !readPreference.equals(primary())) {
            document.put("$readPreference", readPreference.toDocument());
        }

        return document;
    }


    private boolean isExhaustCursor() {
        return model.getCursorFlags().contains(CursorFlag.EXHAUST);
    }
}