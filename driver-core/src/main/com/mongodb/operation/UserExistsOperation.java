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
import com.mongodb.async.MongoFuture;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.ReadBinding;
import com.mongodb.connection.Connection;
import com.mongodb.protocol.QueryProtocol;
import com.mongodb.protocol.QueryResult;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.codecs.BsonDocumentCodec;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static com.mongodb.operation.OperationHelper.AsyncCallableWithConnection;
import static com.mongodb.operation.OperationHelper.CallableWithConnection;
import static com.mongodb.operation.OperationHelper.executeProtocol;
import static com.mongodb.operation.OperationHelper.executeProtocolAsync;
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
                if (serverIsAtLeastVersionTwoDotSix(connection)) {
                    return executeWrappedCommandProtocol(databaseName, getCommand(), connection, binding.getReadPreference(),
                                                         transformer());
                } else {
                    return executeProtocol(getCollectionBasedProtocol(), connection, transformQueryResult());
                }
            }
        });
    }

    @Override
    public MongoFuture<Boolean> executeAsync(final AsyncReadBinding binding) {
        return withConnection(binding, new AsyncCallableWithConnection<Boolean>() {
            @Override
            public MongoFuture<Boolean> call(final Connection connection) {
                if (serverIsAtLeastVersionTwoDotSix(connection)) {
                    return executeWrappedCommandProtocolAsync(databaseName, getCommand(), connection, transformer());
                } else {
                    return executeProtocolAsync(getCollectionBasedProtocol(), connection, transformQueryResult());
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

    private QueryProtocol<BsonDocument> getCollectionBasedProtocol() {
        MongoNamespace namespace = new MongoNamespace(databaseName, "system.users");
        return new QueryProtocol<BsonDocument>(namespace, 0, 0, 1,
                                               new BsonDocument("user", new BsonString(userName)), null, new BsonDocumentCodec());
    }

    private BsonDocument getCommand() {
        return new BsonDocument("usersInfo", new BsonString(userName));
    }

}
