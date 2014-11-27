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

import com.mongodb.CommandFailureException;
import com.mongodb.Function;
import com.mongodb.MongoNamespace;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncConnectionSource;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.binding.ReadBinding;
import com.mongodb.connection.Connection;
import com.mongodb.connection.QueryResult;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;
import org.bson.codecs.DecoderContext;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.async.ErrorHandlingResultCallback.wrapCallback;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static com.mongodb.operation.CommandOperationHelper.isNamespaceError;
import static com.mongodb.operation.FindOperationHelper.queryResultToList;
import static com.mongodb.operation.FindOperationHelper.queryResultToListAsync;
import static com.mongodb.operation.OperationHelper.AsyncCallableWithConnectionAndSource;
import static com.mongodb.operation.OperationHelper.IdentityTransformer;
import static com.mongodb.operation.OperationHelper.serverIsAtLeastVersionTwoDotEight;
import static com.mongodb.operation.OperationHelper.withConnection;

/**
 * An operation that lists the indexes that have been created on a collection.  For flexibility, the type of each document returned is
 * generic.
 *
 * @param <T> the operations result type.
 * @since 3.0
 */
public class ListIndexesOperation<T> implements AsyncReadOperation<List<T>>, ReadOperation<List<T>> {
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
    public List<T> execute(final ReadBinding binding) {
        return withConnection(binding, new OperationHelper.CallableWithConnectionAndSource<List<T>>() {
            @Override
            public List<T> call(final ConnectionSource source, final Connection connection) {
                if (serverIsAtLeastVersionTwoDotEight(connection)) {
                    try {
                        return executeWrappedCommandProtocol(namespace.getDatabaseName(), getCommand(), new BsonDocumentCodec(), binding,
                                                             transformer());
                    } catch (CommandFailureException e) {
                        return CommandOperationHelper.rethrowIfNotNamespaceError(e, new ArrayList<T>());
                    }
                } else {
                    return queryResultToList(getIndexNamespace(), connection.query(getIndexNamespace(), asQueryDocument(), null, 0, 0,
                                                                                   binding.getReadPreference().isSlaveOk(), false,
                                                                                   false, false, false, false,
                                                                                   decoder), decoder, source, new IdentityTransformer<T>());
                }
            }
        });

    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<List<T>> callback) {
        withConnection(binding, new AsyncCallableWithConnectionAndSource() {
            @Override
            public void call(final AsyncConnectionSource source, final Connection connection, final Throwable t) {
                if (t != null) {
                    wrapCallback(callback).onResult(null, t);
                } else if (serverIsAtLeastVersionTwoDotEight(connection)) {
                    executeWrappedCommandProtocolAsync(namespace.getDatabaseName(),
                                                       getCommand(),
                                                       connection,
                                                       transformer(),
                                                       new SingleResultCallback<List<T>>() {
                                                           @Override
                                                           public void onResult(final List<T> result,
                                                                                final Throwable t) {
                                                               if (t != null && !isNamespaceError(t)) {
                                                                   callback.onResult(null, t);
                                                               } else {
                                                                   callback.onResult(result != null ? result : new ArrayList<T>(),
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
                                callback.onResult(null, t);
                            } else {
                                queryResultToListAsync(getIndexNamespace(), result, decoder, source, new IdentityTransformer<T>(),
                                                       callback);
                            }
                        }
                    });
                }
            }
        });
    }

    private BsonDocument asQueryDocument() {
        return new BsonDocument("ns", new BsonString(namespace.getFullName()));
    }

    private MongoNamespace getIndexNamespace() {
        return new MongoNamespace(namespace.getDatabaseName(), "system.indexes");
    }

    private BsonDocument getCommand() {
        return new BsonDocument("listIndexes", new BsonString(namespace.getCollectionName()));
    }

    private Function<BsonDocument, List<T>> transformer() {
        return new Function<BsonDocument, List<T>>() {
            @Override
            public List<T> apply(final BsonDocument results) {
                BsonArray bsonIndexes = results.getArray("indexes");
                List<T> indexes = new ArrayList<T>(bsonIndexes.size());
                for (BsonValue rawIndex : bsonIndexes) {
                    indexes.add(decoder.decode(new BsonDocumentReader((BsonDocument) rawIndex), DecoderContext.builder().build()));
                }
                return indexes;
            }
        };
    }

}
