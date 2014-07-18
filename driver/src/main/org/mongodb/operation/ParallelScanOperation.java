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

package org.mongodb.operation;

import com.mongodb.ServerAddress;
import com.mongodb.binding.AsyncConnectionSource;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.binding.ReadBinding;
import com.mongodb.connection.Connection;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.Decoder;
import org.mongodb.CommandResult;
import org.mongodb.Function;
import org.mongodb.MongoAsyncCursor;
import org.mongodb.MongoCursor;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.ParallelScanOptions;
import org.mongodb.protocol.QueryResult;

import java.util.ArrayList;
import java.util.List;

import static org.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static org.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static org.mongodb.operation.OperationHelper.AsyncCallableWithConnectionAndSource;
import static org.mongodb.operation.OperationHelper.CallableWithConnectionAndSource;
import static org.mongodb.operation.OperationHelper.withConnection;

/**
 * Return a list of cursors over the collection that can be used to scan it in parallel. <p> Note: As of MongoDB 2.6, this operation will
 * work against a mongod, but not a mongos. </p>
 *
 * @param <T> the document type for each cursor
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
     * @param namespace the namespace
     * @param decoder   the decoder
     * @param options   the options to apply
     */
    public ParallelScanOperation(final MongoNamespace namespace, final ParallelScanOptions options, final Decoder<T> decoder) {
        this.namespace = namespace;
        this.options = options;
        this.decoder = decoder;
    }

    @Override
    public List<MongoCursor<T>> execute(final ReadBinding binding) {
        return withConnection(binding, new CallableWithConnectionAndSource<List<MongoCursor<T>>>() {
            @Override
            public List<MongoCursor<T>> call(final ConnectionSource source, final Connection connection) {
                return executeWrappedCommandProtocol(namespace,
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
                return executeWrappedCommandProtocolAsync(namespace,
                                                          asCommandDocument(),
                                                          CommandResultDocumentCodec.create(decoder, "firstBatch"),
                                                          connection, binding.getReadPreference(),
                                                          asyncTransformer(source));
            }
        });
    }

    private Function<CommandResult, List<MongoCursor<T>>> transformer(final ConnectionSource source) {
        return new Function<CommandResult, List<MongoCursor<T>>>() {
            @Override
            public List<MongoCursor<T>> apply(final CommandResult commandResult) {
                List<MongoCursor<T>> cursors = new ArrayList<MongoCursor<T>>();
                for (BsonValue cursorValue : getCursorDocuments(commandResult)) {
                    cursors.add(new MongoQueryCursor<T>(namespace, createQueryResult(getCursorDocument(cursorValue.asDocument()),
                                                                                     commandResult.getAddress()),
                                                        0, options.getBatchSize(), decoder, source
                    ));
                }
                return cursors;
            }
        };
    }

    private Function<CommandResult, List<MongoAsyncCursor<T>>> asyncTransformer(final AsyncConnectionSource source) {
        return new Function<CommandResult, List<MongoAsyncCursor<T>>>() {
            @Override
            public List<MongoAsyncCursor<T>> apply(final CommandResult commandResult) {
                List<MongoAsyncCursor<T>> cursors = new ArrayList<MongoAsyncCursor<T>>();
                for (BsonValue cursorValue : getCursorDocuments(commandResult)) {
                    cursors.add(new MongoAsyncQueryCursor<T>(namespace, createQueryResult(getCursorDocument(cursorValue.asDocument()),
                                                                                          commandResult.getAddress()),
                                                             0, options.getBatchSize(), decoder, source
                    ));
                }
                return cursors;
            }
        };
    }

    @SuppressWarnings("unchecked")
    private BsonArray getCursorDocuments(final CommandResult commandResult) {
        return commandResult.getResponse().getArray("cursors");
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

