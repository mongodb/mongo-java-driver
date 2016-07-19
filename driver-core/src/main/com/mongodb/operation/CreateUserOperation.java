/*
 * Copyright (c) 2008-2016 MongoDB, Inc.
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
import com.mongodb.MongoCredential;
import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.WriteConcernResult;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncWriteBinding;
import com.mongodb.binding.WriteBinding;
import com.mongodb.bulk.InsertRequest;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ConnectionDescription;
import org.bson.BsonDocument;

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
import static com.mongodb.operation.UserOperationHelper.asCollectionInsertDocument;
import static com.mongodb.operation.UserOperationHelper.asCommandDocument;
import static com.mongodb.operation.UserOperationHelper.translateUserCommandException;
import static com.mongodb.operation.UserOperationHelper.userCommandCallback;
import static com.mongodb.operation.WriteConcernHelper.appendWriteConcernToCommand;
import static com.mongodb.operation.WriteConcernHelper.writeConcernErrorTransformer;
import static java.util.Arrays.asList;

/**
 * An operation to create a user.
 *
 * @since 3.0
 */
public class CreateUserOperation implements AsyncWriteOperation<Void>, WriteOperation<Void> {
    private final MongoCredential credential;
    private final boolean readOnly;
    private final WriteConcern writeConcern;

    /**
     * Construct a new instance.
     *
     * @param credential the users credentials.
     * @param readOnly   true if the user is a readOnly user.
     * @deprecated Prefer {@link #CreateUserOperation(MongoCredential, boolean, WriteConcern)}
     */
    @Deprecated
    public CreateUserOperation(final MongoCredential credential, final boolean readOnly) {
        this(credential, readOnly, null);
    }

    /**
     * Construct a new instance.
     *
     * @param credential the users credentials.
     * @param readOnly   true if the user is a readOnly user.
     * @param writeConcern the write concern
     *
     * @since 3.4
     */
    public CreateUserOperation(final MongoCredential credential, final boolean readOnly, final WriteConcern writeConcern) {
        this.credential = notNull("credential", credential);
        this.readOnly = readOnly;
        this.writeConcern = writeConcern;
    }

    /**
     * Gets the users credentials.
     *
     * @return the users credentials.
     */
    public MongoCredential getCredential() {
        return credential;
    }

    /**
     * Returns true if the user is a readOnly user.
     *
     * @return true if the user is a readOnly user.
     */
    public boolean isReadOnly() {
        return readOnly;
    }

    @Override
    public Void execute(final WriteBinding binding) {
        return withConnection(binding, new CallableWithConnection<Void>() {
            @Override
            public Void call(final Connection connection) {
                if (serverIsAtLeastVersionTwoDotSix(connection.getDescription())) {
                    try {
                        executeWrappedCommandProtocol(binding, getCredential().getSource(), getCommand(connection.getDescription()),
                                connection, writeConcernErrorTransformer());
                    } catch (MongoCommandException e) {
                        translateUserCommandException(e);
                    }
                } else {
                    connection.insert(getNamespace(), true, WriteConcern.ACKNOWLEDGED, asList(getInsertRequest()));
                }
                return null;
            }
        });
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<Void> callback) {
        withConnection(binding, new AsyncCallableWithConnection() {
            @Override
            public void call(final AsyncConnection connection, final Throwable t) {
                SingleResultCallback<Void> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
                if (t != null) {
                    errHandlingCallback.onResult(null, t);
                } else {
                    final SingleResultCallback<Void> wrappedCallback = releasingCallback(errHandlingCallback, connection);
                    if (serverIsAtLeastVersionTwoDotSix(connection.getDescription())) {
                        executeWrappedCommandProtocolAsync(binding, credential.getSource(), getCommand(connection.getDescription()),
                                connection, writeConcernErrorTransformer(), userCommandCallback(wrappedCallback));
                    } else {
                        connection.insertAsync(getNamespace(), true, WriteConcern.ACKNOWLEDGED,
                                               asList(getInsertRequest()), new SingleResultCallback<WriteConcernResult>() {
                            @Override
                            public void onResult(final WriteConcernResult result, final Throwable t) {
                                wrappedCallback.onResult(null, t);
                            }
                        });
                    }
                }
            }
        });
    }

    private InsertRequest getInsertRequest() {
        return new InsertRequest(asCollectionInsertDocument(credential, readOnly));
    }

    private MongoNamespace getNamespace() {
        return new MongoNamespace(credential.getSource(), "system.users");
    }

    private BsonDocument getCommand(final ConnectionDescription description) {
        BsonDocument commandDocument = asCommandDocument(credential, readOnly, "createUser");
        appendWriteConcernToCommand(writeConcern, commandDocument, description);
        return commandDocument;
    }
}
