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

import com.mongodb.Function;
import com.mongodb.MongoCursor;
import com.mongodb.MongoNamespace;
import com.mongodb.ServerAddress;
import com.mongodb.async.MongoAsyncCursor;
import com.mongodb.async.MongoFuture;
import com.mongodb.binding.AsyncConnectionSource;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.binding.ReadBinding;
import com.mongodb.connection.Connection;
import com.mongodb.protocol.QueryResult;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.Decoder;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static com.mongodb.operation.OperationHelper.AsyncCallableWithConnectionAndSource;
import static com.mongodb.operation.OperationHelper.CallableWithConnectionAndSource;
import static com.mongodb.operation.OperationHelper.withConnection;

/**
 * Return a list of cursors over the collection that can be used to scan it in parallel.
 *
 * <p> Note: As of MongoDB 2.6, this operation will work against a mongod, but not a mongos. </p>
 *
 * @param <T> the operations result type.
 * @mongodb.server.release 2.6
 * @since 3.0
 */
public class ParallelScanOperation<T> implements AsyncReadOperation<List<MongoAsyncCursor<T>>>, ReadOperation<List<MongoCursor<T>>> {
    private final MongoNamespace namespace;
    private final ParallelScanOptions options;
    private final Decoder<T> decoder;

    /**
     * Construct a new instance.
     *
     * @param namespace the database and collection namespace for the operation.
     * @param decoder the decoder for the result documents.
     * @param options   the options to apply.
     */
    public ParallelScanOperation(final MongoNamespace namespace, final ParallelScanOptions options, final Decoder<T> decoder) {
        this.namespace = notNull("namespace", namespace);
        this.options = notNull("options", options);
        this.decoder = notNull("decoder", decoder);
    }

    @Override
    public List<MongoCursor<T>> execute(final ReadBinding binding) {
        return withConnection(binding, new CallableWithConnectionAndSource<List<MongoCursor<T>>>() {
            @Override
            public List<MongoCursor<T>> call(final ConnectionSource source, final Connection connection) {
                return executeWrappedCommandProtocol(namespace.getDatabaseName(),
                                                     asCommandDocument(),
                                                     CommandResultDocumentCodec.create(decoder, "firstBatch"),
                                                     connection, binding.getReadPreference(),
                                                     transformer(source));
            }
        });
    }


    @Override
    public MongoFuture<List<MongoAsyncCursor<T>>> executeAsync(final AsyncReadBinding binding) {
        return withConnection(binding, new AsyncCallableWithConnectionAndSource<List<MongoAsyncCursor<T>>>() {
            @Override
            public MongoFuture<List<MongoAsyncCursor<T>>> call(final AsyncConnectionSource source, final Connection connection) {
                return executeWrappedCommandProtocolAsync(namespace.getDatabaseName(),
                                                          asCommandDocument(),
                                                          CommandResultDocumentCodec.create(decoder, "firstBatch"),
                                                          connection, binding.getReadPreference(),
                                                          asyncTransformer(source));
            }
        });
    }

    private Function<BsonDocument, List<MongoCursor<T>>> transformer(final ConnectionSource source) {
        return new Function<BsonDocument, List<MongoCursor<T>>>() {
            @Override
            public List<MongoCursor<T>> apply(final BsonDocument result) {
                List<MongoCursor<T>> cursors = new ArrayList<MongoCursor<T>>();
                for (BsonValue cursorValue : getCursorDocuments(result)) {
                    cursors.add(new MongoQueryCursor<T>(namespace, createQueryResult(getCursorDocument(cursorValue.asDocument()),
                                                                                     source.getServerDescription().getAddress()),
                                                        0, options.getBatchSize(), decoder, source));
                }
                return cursors;
            }
        };
    }

    private Function<BsonDocument, List<MongoAsyncCursor<T>>> asyncTransformer(final AsyncConnectionSource source) {
        return new Function<BsonDocument, List<MongoAsyncCursor<T>>>() {
            @Override
            public List<MongoAsyncCursor<T>> apply(final BsonDocument result) {
                List<MongoAsyncCursor<T>> cursors = new ArrayList<MongoAsyncCursor<T>>();
                for (BsonValue cursorValue : getCursorDocuments(result)) {
                    cursors.add(new MongoAsyncQueryCursor<T>(namespace, createQueryResult(getCursorDocument(cursorValue.asDocument()),
                                                                                          source.getServerDescription().getAddress()),
                                                             0, options.getBatchSize(), decoder, source
                    ));
                }
                return cursors;
            }
        };
    }

    @SuppressWarnings("unchecked")
    private BsonArray getCursorDocuments(final BsonDocument result) {
        return result.getArray("cursors");
    }

    private BsonDocument getCursorDocument(final BsonDocument cursorDocument) {
        return cursorDocument.getDocument("cursor");
    }

    @SuppressWarnings("unchecked")
    private QueryResult<T> createQueryResult(final BsonDocument cursorDocument, final ServerAddress serverAddress) {
        return new QueryResult<T>(BsonDocumentWrapperHelper.<T>toList(cursorDocument.getArray("firstBatch")),
                                  cursorDocument.getInt64("id").getValue(), serverAddress, 0);
    }

    private BsonDocument asCommandDocument() {
        return new BsonDocument("parallelCollectionScan", new BsonString(namespace.getCollectionName()))
               .append("numCursors", new BsonInt32(options.getNumCursors()));
    }
}

