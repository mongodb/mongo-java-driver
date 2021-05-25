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

package com.mongodb.internal.operation;

import com.mongodb.CreateIndexCommitQuorum;
import com.mongodb.DuplicateKeyException;
import com.mongodb.ErrorCategory;
import com.mongodb.MongoClientException;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.WriteConcernResult;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.ClientSideOperationTimeout;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.internal.bulk.IndexRequest;
import com.mongodb.internal.connection.AsyncConnection;
import com.mongodb.internal.connection.Connection;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.AsyncCommandOperationHelper.executeCommandAsync;
import static com.mongodb.internal.operation.AsyncCommandOperationHelper.writeConcernErrorTransformerAsync;
import static com.mongodb.internal.operation.AsyncOperationHelper.AsyncCallableWithConnection;
import static com.mongodb.internal.operation.AsyncOperationHelper.releasingCallback;
import static com.mongodb.internal.operation.AsyncOperationHelper.withAsyncConnection;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotZero;
import static com.mongodb.internal.operation.IndexHelper.generateIndexName;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsAtLeastVersionFourDotFour;
import static com.mongodb.internal.operation.SyncCommandOperationHelper.executeCommand;
import static com.mongodb.internal.operation.SyncCommandOperationHelper.writeConcernErrorTransformer;
import static com.mongodb.internal.operation.SyncOperationHelper.CallableWithConnection;
import static com.mongodb.internal.operation.SyncOperationHelper.withConnection;
import static com.mongodb.internal.operation.WriteConcernHelper.appendWriteConcernToCommand;

/**
 * An operation that creates one or more indexes.
 *
 * @mongodb.driver.manual reference/command/createIndexes/ Create indexes
 * @since 3.0
 */
public class CreateIndexesOperation implements AsyncWriteOperation<Void>, WriteOperation<Void> {
    private final ClientSideOperationTimeout clientSideOperationTimeout;
    private final MongoNamespace namespace;
    private final List<IndexRequest> requests;
    private final WriteConcern writeConcern;
    private CreateIndexCommitQuorum commitQuorum;

    /**
     * Construct a new instance.
     *
     * @param clientSideOperationTimeout the client side operation timeout factory
     * @param namespace     the database and collection namespace for the operation.
     * @param requests the index request
     */
    public CreateIndexesOperation(final ClientSideOperationTimeout clientSideOperationTimeout,
                                  final MongoNamespace namespace, final List<IndexRequest> requests) {
        this(clientSideOperationTimeout, namespace, requests, null);
    }

    /**
     * Construct a new instance.
     *
     * @param clientSideOperationTimeout the client side operation timeout factory
     * @param namespace     the database and collection namespace for the operation.
     * @param requests the index request
     * @param writeConcern the write concern
     *
     * @since 3.4
     */
    public CreateIndexesOperation(final ClientSideOperationTimeout clientSideOperationTimeout,
                                  final MongoNamespace namespace, final List<IndexRequest> requests, final WriteConcern writeConcern) {
        this.clientSideOperationTimeout = notNull("clientSideOperationTimeout", clientSideOperationTimeout);
        this.namespace = notNull("namespace", namespace);
        this.requests = notNull("indexRequests", requests);
        this.writeConcern = writeConcern;
    }

    /**
     * Gets the write concern.
     *
     * @return the write concern, which may be null
     *
     * @since 3.4
     */
    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    /**
     * Gets the index requests.
     *
     * @return the index requests
     */
    public List<IndexRequest> getRequests() {
        return requests;
    }

    /**
     * Gets the index names.
     *
     * @return a list of index names
     */
    public List<String> getIndexNames() {
        List<String> indexNames = new ArrayList<String>(requests.size());
        for (IndexRequest request : requests) {
            if (request.getName() != null) {
                indexNames.add(request.getName());
            } else {
                indexNames.add(IndexHelper.generateIndexName(request.getKeys()));
            }
        }
        return indexNames;
    }

    /**
     * Gets the create index commit quorum.
     *
     * @return the create index commit quorum
     * @since 4.1
     */
    public CreateIndexCommitQuorum getCommitQuorum() {
        return commitQuorum;
    }

    /**
     * Sets the create index commit quorum.
     *
     * @param commitQuorum the create index commit quorum
     * @return this
     * @since 4.1
     */
    public CreateIndexesOperation commitQuorum(final CreateIndexCommitQuorum commitQuorum) {
        this.commitQuorum = commitQuorum;
        return this;
    }

    @Override
    public Void execute(final WriteBinding binding) {
        return withConnection(clientSideOperationTimeout, binding, new CallableWithConnection<Void>() {
            @Override
            public Void call(final ClientSideOperationTimeout clientSideOperationTimeout, final Connection connection) {
                try {
                    SyncOperationHelper.validateIndexRequestCollations(connection, requests);
                    executeCommand(clientSideOperationTimeout, binding, namespace.getDatabaseName(),
                            getCommand(clientSideOperationTimeout, connection.getDescription()),
                            connection, writeConcernErrorTransformer());
                } catch (MongoCommandException e) {
                    throw checkForDuplicateKeyError(e);
                }
                return null;
            }
        });
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<Void> callback) {
        withAsyncConnection(clientSideOperationTimeout, binding, new AsyncCallableWithConnection() {
            @Override
            public void call(final ClientSideOperationTimeout clientSideOperationTimeout, final AsyncConnection connection,
                             final Throwable t) {
                SingleResultCallback<Void> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
                if (t != null) {
                    errHandlingCallback.onResult(null, t);
                } else {
                    final SingleResultCallback<Void> wrappedCallback = releasingCallback(errHandlingCallback, connection);
                    AsyncOperationHelper.validateIndexRequestCollations(clientSideOperationTimeout, connection, requests,
                            new AsyncCallableWithConnection() {
                        @Override
                        public void call(final ClientSideOperationTimeout clientSideOperationTimeout,
                                         final AsyncConnection connection, final Throwable t) {
                            if (t != null) {
                                wrappedCallback.onResult(null, t);
                            } else {
                                try {
                                    executeCommandAsync(clientSideOperationTimeout, binding, namespace.getDatabaseName(),
                                            getCommand(clientSideOperationTimeout, connection.getDescription()), connection,
                                            writeConcernErrorTransformerAsync(),
                                            new SingleResultCallback<Void>() {
                                                @Override
                                                public void onResult(final Void result, final Throwable t) {
                                                    wrappedCallback.onResult(null, translateException(t));
                                                }
                                            });
                                } catch (Throwable t1) {
                                    wrappedCallback.onResult(null, t1);
                                }
                            }
                        }
                    });
                }
            }
        });
    }

    @SuppressWarnings("deprecation")
    private BsonDocument getIndex(final IndexRequest request) {
        BsonDocument index = new BsonDocument();
        index.append("key", request.getKeys());
        index.append("name", new BsonString(request.getName() != null ? request.getName() : generateIndexName(request.getKeys())));
        if (request.isBackground()) {
            index.append("background", BsonBoolean.TRUE);
        }
        if (request.isUnique()) {
            index.append("unique", BsonBoolean.TRUE);
        }
        if (request.isSparse()) {
            index.append("sparse", BsonBoolean.TRUE);
        }
        if (request.getExpireAfter(TimeUnit.SECONDS) != null) {
            index.append("expireAfterSeconds", new BsonInt64(request.getExpireAfter(TimeUnit.SECONDS)));
        }
        if (request.getVersion() != null) {
            index.append("v", new BsonInt32(request.getVersion()));
        }
        if (request.getWeights() != null) {
            index.append("weights", request.getWeights());
        }
        if (request.getDefaultLanguage() != null) {
            index.append("default_language", new BsonString(request.getDefaultLanguage()));
        }
        if (request.getLanguageOverride() != null) {
            index.append("language_override", new BsonString(request.getLanguageOverride()));
        }
        if (request.getTextVersion() != null) {
            index.append("textIndexVersion", new BsonInt32(request.getTextVersion()));
        }
        if (request.getSphereVersion() != null) {
            index.append("2dsphereIndexVersion", new BsonInt32(request.getSphereVersion()));
        }
        if (request.getBits() != null) {
            index.append("bits", new BsonInt32(request.getBits()));
        }
        if (request.getMin() != null) {
            index.append("min", new BsonDouble(request.getMin()));
        }
        if (request.getMax() != null) {
            index.append("max", new BsonDouble(request.getMax()));
        }
        if (request.getBucketSize() != null) {
            index.append("bucketSize", new BsonDouble(request.getBucketSize()));
        }
        if (request.getDropDups()) {
            index.append("dropDups", BsonBoolean.TRUE);
        }
        if (request.getStorageEngine() != null) {
            index.append("storageEngine", request.getStorageEngine());
        }
        if (request.getPartialFilterExpression() != null) {
            index.append("partialFilterExpression", request.getPartialFilterExpression());
        }
        if (request.getCollation() != null) {
            index.append("collation", request.getCollation().asDocument());
        }
        if (request.getWildcardProjection() != null) {
            index.append("wildcardProjection", request.getWildcardProjection());
        }
        if (request.isHidden()) {
            index.append("hidden", BsonBoolean.TRUE);
        }
        return index;
    }

    private BsonDocument getCommand(final ClientSideOperationTimeout clientSideOperationTimeout, final ConnectionDescription description) {
        BsonDocument command = new BsonDocument("createIndexes", new BsonString(namespace.getCollectionName()));
        List<BsonDocument> values = new ArrayList<BsonDocument>();
        for (IndexRequest request : requests) {
            values.add(getIndex(request));
        }
        command.put("indexes", new BsonArray(values));
        putIfNotZero(command, "maxTimeMS", clientSideOperationTimeout.getMaxTimeMS());
        appendWriteConcernToCommand(writeConcern, command, description);
        if (commitQuorum != null) {
            if (serverIsAtLeastVersionFourDotFour(description)) {
                command.put("commitQuorum", commitQuorum.toBsonValue());
            } else {
                throw new MongoClientException("Specifying a value for the create index commit quorum option "
                        + "requires a minimum MongoDB version of 4.4");
            }
        }
        return command;
    }

    private MongoException translateException(final Throwable t) {
        return (t instanceof MongoCommandException) ? checkForDuplicateKeyError((MongoCommandException) t)
                                                      : MongoException.fromThrowable(t);
    }

    private MongoException checkForDuplicateKeyError(final MongoCommandException e) {
        if (ErrorCategory.fromErrorCode(e.getCode()) == ErrorCategory.DUPLICATE_KEY) {
            return new DuplicateKeyException(e.getResponse(), e.getServerAddress(), WriteConcernResult.acknowledged(0, false, null));
        } else {
            return e;
        }
    }
}
