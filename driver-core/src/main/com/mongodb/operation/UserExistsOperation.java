/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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

import com.mongodb.MongoNamespace;
import com.mongodb.ServerAddress;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.ReadBinding;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.Connection;
import com.mongodb.connection.QueryResult;
import com.mongodb.operation.CommandOperationHelper.CommandTransformer;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.codecs.BsonDocumentCodec;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static com.mongodb.operation.OperationHelper.AsyncCallableWithConnection;
import static com.mongodb.operation.OperationHelper.CallableWithConnection;
import static com.mongodb.operation.OperationHelper.LOGGER;
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
                    return executeWrappedCommandProtocol(binding, databaseName, getCommand(), connection, transformer());
                } else {
                    return transformQueryResult().apply(connection.query(new MongoNamespace(databaseName, "system.users"),
                                                                         new BsonDocument("user", new BsonString(userName)), null, 0, 1, 0,
                                                                         binding.getReadPreference().isSlaveOk(), false,
                                                                         false, false, false, false,
                                                                         new BsonDocumentCodec()),
                                                        connection.getDescription().getServerAddress());
                }
            }
        });
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<Boolean> callback) {
        withConnection(binding, new AsyncCallableWithConnection() {
            @Override
            public void call(final AsyncConnection connection, final Throwable t) {
                SingleResultCallback<Boolean> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
                if (t != null) {
                    errHandlingCallback.onResult(null, t);
                } else {
                    final SingleResultCallback<Boolean> wrappedCallback = releasingCallback(errHandlingCallback, connection);
                    if (serverIsAtLeastVersionTwoDotSix(connection.getDescription())) {
                        executeWrappedCommandProtocolAsync(binding, databaseName, getCommand(), new BsonDocumentCodec(), connection,
                                transformer(), wrappedCallback);
                    } else {
                        connection.queryAsync(new MongoNamespace(databaseName, "system.users"),
                                              new BsonDocument("user", new BsonString(userName)), null, 0, 1, 0,
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
                                         wrappedCallback.onResult(transformQueryResult().apply(result,
                                                                                               connection.getDescription()
                                                                                               .getServerAddress()),
                                                                  null);
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

    private CommandTransformer<BsonDocument, Boolean> transformer() {
        return new CommandTransformer<BsonDocument, Boolean>() {
            @Override
            public Boolean apply(final BsonDocument result, final ServerAddress serverAddress) {
                return result.get("users").isArray() && !result.getArray("users").isEmpty();
            }
        };
    }

    private CommandTransformer<QueryResult<BsonDocument>, Boolean> transformQueryResult() {
        return new CommandTransformer<QueryResult<BsonDocument>, Boolean>() {
            @Override
            public Boolean apply(final QueryResult<BsonDocument> queryResult, final ServerAddress serverAddress) {
                return !queryResult.getResults().isEmpty();
            }
        };
    }

    private BsonDocument getCommand() {
        return new BsonDocument("usersInfo", new BsonString(userName));
    }

}
