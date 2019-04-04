/*
 * Copyright 2008-present MongoDB, Inc.
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

import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncConnectionSource;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.binding.ReadBinding;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.operation.CommandOperationHelper.CommandReadTransformer;
import com.mongodb.operation.CommandOperationHelper.CommandReadTransformerAsync;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.codecs.BsonDocumentCodec;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.operation.CommandOperationHelper.CommandCreator;
import static com.mongodb.operation.CommandOperationHelper.executeCommand;
import static com.mongodb.operation.CommandOperationHelper.executeCommandAsync;
import static com.mongodb.operation.OperationHelper.CallableWithConnection;
import static com.mongodb.operation.OperationHelper.LOGGER;
import static com.mongodb.operation.OperationHelper.withConnection;

/**
 * An operation that determines if a user exists.
 *
 * @since 3.0
 * @deprecated use {@link CommandWriteOperation} directly or the mongod shell helpers.
 */
@Deprecated
public class UserExistsOperation implements AsyncReadOperation<Boolean>, ReadOperation<Boolean> {
    private final String databaseName;
    private final String userName;
    private boolean retryReads;

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

    /**
     * Enables retryable reads if a read fails due to a network error.
     *
     * @param retryReads true if reads should be retried
     * @return this
     * @since 3.11
     */
    public UserExistsOperation retryReads(final boolean retryReads) {
        this.retryReads = retryReads;
        return this;
    }

    /**
     * Gets the value for retryable reads. The default is true.
     *
     * @return the retryable reads value
     * @since 3.11
     */
    public boolean getRetryReads() {
        return retryReads;
    }

    @Override
    public Boolean execute(final ReadBinding binding) {
        return withConnection(binding, new CallableWithConnection<Boolean>() {
            @Override
            public Boolean call(final Connection connection) {
                return executeCommand(binding, databaseName, getCommandCreator(), transformer(), retryReads);
            }
        });
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<Boolean> callback) {
        executeCommandAsync(binding, databaseName, getCommandCreator(), new BsonDocumentCodec(),
                asyncTransformer(), retryReads, errorHandlingCallback(callback, LOGGER));
    }

    private CommandReadTransformer<BsonDocument, Boolean> transformer() {
        return new CommandReadTransformer<BsonDocument, Boolean>() {
            @Override
            public Boolean apply(final BsonDocument result, final ConnectionSource source, final Connection connection) {
                return result.get("users").isArray() && !result.getArray("users").isEmpty();
            }
        };
    }

    private CommandReadTransformerAsync<BsonDocument, Boolean> asyncTransformer() {
        return new CommandReadTransformerAsync<BsonDocument, Boolean>() {
            @Override
            public Boolean apply(final BsonDocument result, final AsyncConnectionSource source, final AsyncConnection connection) {
                return result.get("users").isArray() && !result.getArray("users").isEmpty();
            }
        };
    }

    private CommandCreator getCommandCreator() {
        return new CommandCreator() {
            @Override
            public BsonDocument create(final ServerDescription serverDescription, final ConnectionDescription connectionDescription) {
                return getCommand();
            }
        };
    }

    private BsonDocument getCommand() {
        return new BsonDocument("usersInfo", new BsonString(userName));
    }

}
