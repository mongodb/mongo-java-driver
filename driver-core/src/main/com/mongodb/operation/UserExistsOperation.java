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
import com.mongodb.MongoNamespace;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.ReadBinding;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.Connection;
import com.mongodb.connection.QueryResult;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.codecs.BsonDocumentCodec;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static com.mongodb.operation.OperationHelper.AsyncCallableWithConnection;
import static com.mongodb.operation.OperationHelper.CallableWithConnection;
import static com.mongodb.operation.OperationHelper.releasingCallback;
import static com.mongodb.operation.OperationHelper.serverIsAtLeastVersionTwoDotSix;
import static com.mongodb.operation.OperationHelper.withConnection;

/**
 * An operation that determines if a user exists.
 *
 * @since 3.0
 */
public class UserExistsOperation implements AsyncReadOperation<Boolean>, ReadOperation<Boolean> {
    private final String databaseName;
    private final String userName;

    /**
     * Construct a new instance.
     *
     * @param databaseName the name of the database for the operation.
     * @param userName the name of the user to check if they exist.
     */
    public UserExistsOperation(final String databaseName, final String userName) {
        this.databaseName = notNull("databaseName", databaseName);
        this.userName = notNull("userName", userName);
    }

    @Override
    public Boolean execute(final ReadBinding binding) {
        return withConnection(binding, new CallableWithConnection<Boolean>() {
            @Override
            public Boolean call(final Connection connection) {
                if (serverIsAtLeastVersionTwoDotSix(connection.getDescription())) {
                    return executeWrappedCommandProtocol(databaseName, getCommand(), connection, binding.getReadPreference(),
                                                         transformer());
                } else {
                    return transformQueryResult().apply(connection.query(new MongoNamespace(databaseName, "system.users"),
                                                                         new BsonDocument("user", new BsonString(userName)), null, 1, 0,
                                                                         binding.getReadPreference().isSlaveOk(), false,
                                                                         false, false, false, false,
                                                                         new BsonDocumentCodec()));
                }
            }
        });
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<Boolean> callback) {
        withConnection(binding, new AsyncCallableWithConnection() {
            @Override
            public void call(final AsyncConnection connection, final Throwable t) {
                if (t != null) {
                    errorHandlingCallback(callback).onResult(null, t);
                } else {
                    final SingleResultCallback<Boolean> wrappedCallback = releasingCallback(errorHandlingCallback(callback), connection);
                    if (serverIsAtLeastVersionTwoDotSix(connection.getDescription())) {
                        executeWrappedCommandProtocolAsync(databaseName, getCommand(), connection, transformer(), wrappedCallback);
                    } else {
                        connection.queryAsync(new MongoNamespace(databaseName, "system.users"),
                                              new BsonDocument("user", new BsonString(userName)), null, 1, 0,
                                              binding.getReadPreference().isSlaveOk(), false,
                                              false, false, false, false,
                                              new BsonDocumentCodec(),
                         new SingleResultCallback<QueryResult<BsonDocument>>() {
                             @Override
                             public void onResult(final QueryResult<BsonDocument> result, final Throwable t) {
                                 if (t != null) {
                                     wrappedCallback.onResult(null, t);
                                 } else {
                                     try {
                                         wrappedCallback.onResult(transformQueryResult().apply(result), null);
                                     } catch (Throwable tr) {
                                         wrappedCallback.onResult(null, tr);
                                     }
                                 }
                             }
                         });
                    }
                }
            }
        });
    }

    private Function<BsonDocument, Boolean> transformer() {
        return new Function<BsonDocument, Boolean>() {
            @Override
            public Boolean apply(final BsonDocument result) {
                return result.get("users").isArray() && !result.getArray("users").isEmpty();
            }
        };
    }

    private Function<QueryResult<BsonDocument>, Boolean> transformQueryResult() {
        return new Function<QueryResult<BsonDocument>, Boolean>() {
            @Override
            public Boolean apply(final QueryResult<BsonDocument> queryResult) {
                return !queryResult.getResults().isEmpty();
            }
        };
    }

    private BsonDocument getCommand() {
        return new BsonDocument("usersInfo", new BsonString(userName));
    }

}
