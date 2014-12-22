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

import com.mongodb.MongoCommandException;
import com.mongodb.Function;
import com.mongodb.MongoNamespace;
import com.mongodb.ServerAddress;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncConnectionSource;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.binding.ReadBinding;
import com.mongodb.connection.Connection;
import com.mongodb.connection.QueryResult;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.codecs.Codec;
import org.bson.codecs.Decoder;

import java.util.Collections;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static com.mongodb.operation.CommandOperationHelper.isNamespaceError;
import static com.mongodb.operation.CommandOperationHelper.rethrowIfNotNamespaceError;
import static com.mongodb.operation.OperationHelper.AsyncCallableWithConnectionAndSource;
import static com.mongodb.operation.OperationHelper.commandResultToAsyncBatchCursor;
import static com.mongodb.operation.OperationHelper.commandResultToBatchCursor;
import static com.mongodb.operation.OperationHelper.releasingCallback;
import static com.mongodb.operation.OperationHelper.serverIsAtLeastVersionTwoDotEight;
import static com.mongodb.operation.OperationHelper.withConnection;

/**
 * An operation that lists the indexes that have been created on a collection.  For flexibility, the type of each document returned is
 * generic.
 *
 * @param <T> the operations result type.
 * @since 3.0
 */
public class ListIndexesOperation<T> implements AsyncReadOperation<AsyncBatchCursor<T>>, ReadOperation<BatchCursor<T>> {
    private final MongoNamespace namespace;
    private final Decoder<T> decoder;

    /**
     * Construct a new instance.
     *
     * @param namespace the database and collection namespace for the operation.
     * @param decoder   the decoder for the result documents.
     */
    public ListIndexesOperation(final MongoNamespace namespace, final Decoder<T> decoder) {
        this.namespace = notNull("namespace", namespace);
        this.decoder = notNull("decoder", decoder);
    }

    @Override
    public BatchCursor<T> execute(final ReadBinding binding) {
        return withConnection(binding, new OperationHelper.CallableWithConnectionAndSource<BatchCursor<T>>() {
            @Override
            public BatchCursor<T> call(final ConnectionSource source, final Connection connection) {
                if (serverIsAtLeastVersionTwoDotEight(connection)) {
                    try {
                        return executeWrappedCommandProtocol(namespace.getDatabaseName(), getCommand(), createCommandDecoder(), connection,
                                                             transformer(source));
                    } catch (MongoCommandException e) {
                        return rethrowIfNotNamespaceError(e,
                                                          new QueryBatchCursor<T>(new QueryResult<T>(namespace, Collections.<T>emptyList(),
                                                                                                     0L,
                                                                                                     source.getServerDescription()
                                                                                                           .getAddress(),
                                                                                                     0),
                                                                                  0, 0, decoder, source));
                    }
                } else {
                    return new QueryBatchCursor<T>(connection.query(getIndexNamespace(), asQueryDocument(), null, 0, 0,
                                                                    binding.getReadPreference().isSlaveOk(), false,
                                                                    false, false, false, false, decoder),
                                                   0, 0, decoder, source);
                }
            }
        });

    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        withConnection(binding, new AsyncCallableWithConnectionAndSource() {
            @Override
            public void call(final AsyncConnectionSource source, final Connection connection, final Throwable t) {
                if (t != null) {
                    errorHandlingCallback(callback).onResult(null, t);
                } else {
                    final SingleResultCallback<AsyncBatchCursor<T>> wrappedCallback = releasingCallback(errorHandlingCallback(callback),
                                                                                                        source, connection);
                    if (serverIsAtLeastVersionTwoDotEight(connection)) {
                        executeWrappedCommandProtocolAsync(namespace.getDatabaseName(), getCommand(), createCommandDecoder(),
                                                           connection, binding.getReadPreference(), asyncTransformer(source),
                                                           new SingleResultCallback<AsyncBatchCursor<T>>() {
                                                               @Override
                                                               public void onResult(final AsyncBatchCursor<T> result,
                                                                                    final Throwable t) {
                                                                   if (t != null && !isNamespaceError(t)) {
                                                                       wrappedCallback.onResult(null, t);
                                                                   } else {
                                                                       AsyncBatchCursor<T> emptyCursor
                                                                       = createEmptyAsyncBatchCursor(source.getServerDescription()
                                                                                                           .getAddress());
                                                                       wrappedCallback.onResult(result != null ? result : emptyCursor,
                                                                                                null);
                                                                   }
                                                               }
                                                           });
                    } else {
                        connection.queryAsync(getIndexNamespace(), new BsonDocument(), null, 0, 0, binding.getReadPreference().isSlaveOk(),
                                              false, false, false, false, false, decoder, new SingleResultCallback<QueryResult<T>>() {
                            @Override
                            public void onResult(final QueryResult<T> result, final Throwable t) {
                                if (t != null && !isNamespaceError(t)) {
                                    wrappedCallback.onResult(null, t);
                                } else {
                                    wrappedCallback.onResult(new AsyncQueryBatchCursor<T>(result, 0, 0, decoder, source), null);
                                }
                            }
                        });
                    }
                }
            }
        });
    }

    private AsyncBatchCursor<T> createEmptyAsyncBatchCursor(final ServerAddress serverAddress) {
        return new AsyncQueryBatchCursor<T>(new QueryResult<T>(namespace, Collections.<T>emptyList(), 0L, serverAddress, 0), 0, 0, decoder);
    }

    private BsonDocument asQueryDocument() {
        return new BsonDocument("ns", new BsonString(namespace.getFullName()));
    }

    private MongoNamespace getIndexNamespace() {
        return new MongoNamespace(namespace.getDatabaseName(), "system.indexes");
    }

    private BsonDocument getCommand() {
        return new BsonDocument("listIndexes", new BsonString(namespace.getCollectionName()))
               .append("cursor", new BsonDocument());
    }

    private Function<BsonDocument, BatchCursor<T>> transformer(final ConnectionSource source) {
        return new Function<BsonDocument, BatchCursor<T>>() {
            @Override
            public BatchCursor<T> apply(final BsonDocument result) {
                return commandResultToBatchCursor(result.getDocument("cursor"), decoder, source);
            }
        };
    }

    private Function<BsonDocument, AsyncBatchCursor<T>> asyncTransformer(final AsyncConnectionSource source) {
        return new Function<BsonDocument, AsyncBatchCursor<T>>() {
            @Override
            public AsyncBatchCursor<T> apply(final BsonDocument result) {
                return commandResultToAsyncBatchCursor(result.getDocument("cursor"), decoder, source);
            }
        };
    }

    private Codec<BsonDocument> createCommandDecoder() {
        return CommandResultDocumentCodec.create(decoder, "firstBatch");
    }
}
