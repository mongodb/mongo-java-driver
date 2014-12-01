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
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static com.mongodb.operation.CommandOperationHelper.isNamespaceError;
import static com.mongodb.operation.CommandOperationHelper.rethrowIfNotNamespaceError;
import static com.mongodb.operation.FindOperationHelper.queryResultToList;
import static com.mongodb.operation.FindOperationHelper.queryResultToListAsync;
import static com.mongodb.operation.OperationHelper.AsyncCallableWithConnectionAndSource;
import static com.mongodb.operation.OperationHelper.CallableWithConnectionAndSource;
import static com.mongodb.operation.OperationHelper.releasingCallback;
import static com.mongodb.operation.OperationHelper.serverIsAtLeastVersionTwoDotEight;
import static com.mongodb.operation.OperationHelper.withConnection;

/**
 * An operation that gets the names of all the collections in a database.
 *
 * @since 3.0
 */
public class ListCollectionNamesOperation implements AsyncReadOperation<List<String>>, ReadOperation<List<String>> {
    private final String databaseName;

    /**
     * Construct a new instance.
     *
     * @param databaseName the name of the database for the operation.
     */
    public ListCollectionNamesOperation(final String databaseName) {
        this.databaseName = notNull("databaseName", databaseName);
    }

    @Override
    public List<String> execute(final ReadBinding binding) {
        return withConnection(binding, new CallableWithConnectionAndSource<List<String>>() {
            @Override
            public List<String> call(final ConnectionSource source, final Connection connection) {
                if (serverIsAtLeastVersionTwoDotEight(connection)) {
                    try {
                        return executeWrappedCommandProtocol(databaseName,
                                                             getCommand(),
                                                             new BsonDocumentCodec(),
                                                             binding,
                                                             commandTransformer());
                    } catch (CommandFailureException e) {
                        return rethrowIfNotNamespaceError(e, new ArrayList<String>());
                    }
                } else {
                    return queryResultToList(getNamespace(), connection.query(getNamespace(), new BsonDocument(), null, 0, 0,
                                                                              binding.getReadPreference().isSlaveOk(), false,
                                                                              false, false, false, false,
                                                                              new BsonDocumentCodec()),
                                             new BsonDocumentCodec(), source, queryResultTransformer());
                }
            }
        });
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<List<String>> callback) {
        withConnection(binding, new AsyncCallableWithConnectionAndSource() {
            @Override
            public void call(final AsyncConnectionSource source, final Connection connection, final Throwable t) {
                if (t != null) {
                    errorHandlingCallback(callback).onResult(null, t);
                } else {
                    final SingleResultCallback<List<String>> wrappedCallback = releasingCallback(errorHandlingCallback(callback),
                                                                                                 source, connection);
                    if (serverIsAtLeastVersionTwoDotEight(connection)) {
                        executeWrappedCommandProtocolAsync(databaseName, getCommand(), connection, commandTransformer(),
                                                           new SingleResultCallback<List<String>>() {
                                                               @Override
                                                               public void onResult(final List<String> result, final Throwable t) {
                                                                   if (t != null && !isNamespaceError(t)) {
                                                                       wrappedCallback.onResult(null, t);
                                                                   } else {
                                                                       wrappedCallback.onResult(result != null
                                                                                                ? result : new ArrayList<String>(),
                                                                                         null);
                                                                   }
                                                               }
                                                           });
                    } else {
                        connection.queryAsync(getNamespace(), new BsonDocument(), null, 0, 0,
                                              binding.getReadPreference().isSlaveOk(), false,
                                              false, false, false, false,
                                              new BsonDocumentCodec(), new SingleResultCallback<QueryResult<BsonDocument>>() {
                            @Override
                            public void onResult(final QueryResult<BsonDocument> result, final Throwable t) {
                                if (t != null && !isNamespaceError(t)) {
                                    wrappedCallback.onResult(null, t);
                                } else {
                                    queryResultToListAsync(getNamespace(), result, new BsonDocumentCodec(), source,
                                                           queryResultTransformer(), wrappedCallback);
                                }
                            }
                        });
                    }
                }
            }
        });
    }

    private Function<BsonDocument, String> queryResultTransformer() {
        return new Function<BsonDocument, String>() {
            @Override
            public String apply(final BsonDocument document) {
                String collectionName = document.getString("name").getValue();
                if (!collectionName.contains("$")) {
                    return collectionName.substring(databaseName.length() + 1);
                }
                return null;
            }
        };
    }

    private Function<BsonDocument, List<String>> commandTransformer() {
        return new Function<BsonDocument, List<String>>() {
            @Override
            public List<String> apply(final BsonDocument results) {
                BsonArray collectionInfo = results.getArray("collections");
                List<String> names = new ArrayList<String>();
                for (BsonValue collection : collectionInfo) {
                    names.add(collection.asDocument().getString("name").getValue());
                }
                return names;
            }
        };
    }

    private MongoNamespace getNamespace() {
        return new MongoNamespace(databaseName, "system.namespaces");
    }

    private BsonDocument getCommand() {
        return new BsonDocument("listCollections", new BsonInt32(1));
    }
}
