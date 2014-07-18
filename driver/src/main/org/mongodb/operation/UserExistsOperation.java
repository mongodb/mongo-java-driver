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

import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.ReadBinding;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.codecs.BsonDocumentCodec;
import org.mongodb.CommandResult;
import org.mongodb.Function;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.connection.Connection;
import org.mongodb.protocol.QueryProtocol;
import org.mongodb.protocol.QueryResult;

import java.util.EnumSet;

import static com.mongodb.assertions.Assertions.notNull;
import static org.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static org.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static org.mongodb.operation.OperationHelper.AsyncCallableWithConnection;
import static org.mongodb.operation.OperationHelper.CallableWithConnection;
import static org.mongodb.operation.OperationHelper.executeProtocol;
import static org.mongodb.operation.OperationHelper.executeProtocolAsync;
import static org.mongodb.operation.OperationHelper.serverIsAtLeastVersionTwoDotSix;
import static org.mongodb.operation.OperationHelper.withConnection;

/**
 * An operation that determines if a user exists.
 *
 * @since 3.0
 */
public class UserExistsOperation implements AsyncReadOperation<Boolean>, ReadOperation<Boolean> {

    private final String database;
    private final String userName;

    public UserExistsOperation(final String source, final String userName) {
        this.database = notNull("source", source);
        this.userName = notNull("userName", userName);
    }

    @Override
    public Boolean execute(final ReadBinding binding) {
        return withConnection(binding, new CallableWithConnection<Boolean>() {
            @Override
            public Boolean call(final Connection connection) {
                if (serverIsAtLeastVersionTwoDotSix(connection)) {
                    return executeWrappedCommandProtocol(database, getCommand(), connection, binding.getReadPreference(),
                                                         transformCommandResult());
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
                    return executeWrappedCommandProtocolAsync(database, getCommand(), connection, transformCommandResult());
                } else {
                    return executeProtocolAsync(getCollectionBasedProtocol(), connection, transformQueryResult());
                }
            }
        });
    }

    private Function<CommandResult, Boolean> transformCommandResult() {
        return new Function<CommandResult, Boolean>() {
            @Override
            public Boolean apply(final CommandResult result) {
                return result.getResponse().get("users").isArray() && !result.getResponse().getArray("users").isEmpty();
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
        MongoNamespace namespace = new MongoNamespace(database, "system.users");
        return new QueryProtocol<BsonDocument>(namespace, EnumSet.noneOf(QueryFlag.class), 0, 1,
                                               new BsonDocument("user", new BsonString(userName)), null, new BsonDocumentCodec());
    }

    private BsonDocument getCommand() {
        return new BsonDocument("usersInfo", new BsonString(userName));
    }

}
