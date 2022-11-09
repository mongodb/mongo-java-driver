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

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.CommandOperationHelper.executeCommand;
import static com.mongodb.internal.operation.CommandOperationHelper.executeCommandAsync;
import static com.mongodb.internal.operation.CommandOperationHelper.writeConcernErrorTransformer;
import static com.mongodb.internal.operation.CommandOperationHelper.writeConcernErrorWriteTransformer;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotZero;
import static com.mongodb.internal.operation.IndexHelper.generateIndexName;
import static com.mongodb.internal.operation.OperationHelper.AsyncCallableWithConnection;
import static com.mongodb.internal.operation.OperationHelper.CallableWithConnection;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.OperationHelper.releasingCallback;
import static com.mongodb.internal.operation.OperationHelper.withAsyncConnection;
import static com.mongodb.internal.operation.OperationHelper.withConnection;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsAtLeastVersionFourDotFour;
import static com.mongodb.internal.operation.WriteConcernHelper.appendWriteConcernToCommand;

/**
 * An operation that creates one or more indexes.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class CreateIndexesOperation implements AsyncWriteOperation<Void>, WriteOperation<Void> {
    private final MongoNamespace namespace;
    private final List<IndexRequest> requests;
    private final WriteConcern writeConcern;
    private long maxTimeMS;
    private CreateIndexCommitQuorum commitQuorum;

    public CreateIndexesOperation(final MongoNamespace namespace, final List<IndexRequest> requests) {
        this(namespace, requests, null);
    }

    public CreateIndexesOperation(final MongoNamespace namespace, final List<IndexRequest> requests, final WriteConcern writeConcern) {
        this.namespace = notNull("namespace", namespace);
        this.requests = notNull("indexRequests", requests);
        this.writeConcern = writeConcern;
    }

    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    public List<IndexRequest> getRequests() {
        return requests;
    }

    public List<String> getIndexNames() {
        List<String> indexNames = new ArrayList<>(requests.size());
        for (IndexRequest request : requests) {
            if (request.getName() != null) {
                indexNames.add(request.getName());
            } else {
                indexNames.add(generateIndexName(request.getKeys()));
            }
        }
        return indexNames;
    }

    public long getMaxTime(final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        return timeUnit.convert(maxTimeMS, TimeUnit.MILLISECONDS);
    }

    public CreateIndexesOperation maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        isTrueArgument("maxTime >= 0", maxTime >= 0);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    public CreateIndexCommitQuorum getCommitQuorum() {
        return commitQuorum;
    }

    public CreateIndexesOperation commitQuorum(final CreateIndexCommitQuorum commitQuorum) {
        this.commitQuorum = commitQuorum;
        return this;
    }

    @Override
    public Void execute(final WriteBinding binding) {
        return withConnection(binding, new CallableWithConnection<Void>() {
            @Override
            public Void call(final Connection connection) {
                try {
                    executeCommand(binding, namespace.getDatabaseName(), getCommand(connection.getDescription()),
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
        withAsyncConnection(binding, new AsyncCallableWithConnection() {
            @Override
            public void call(final AsyncConnection connection, final Throwable t) {
                SingleResultCallback<Void> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
                if (t != null) {
                    errHandlingCallback.onResult(null, t);
                } else {
                    SingleResultCallback<Void> wrappedCallback = releasingCallback(errHandlingCallback, connection);
                    try {
                        executeCommandAsync(binding, namespace.getDatabaseName(),
                                getCommand(connection.getDescription()), connection, writeConcernErrorWriteTransformer(),
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

    private BsonDocument getCommand(final ConnectionDescription description) {
        BsonDocument command = new BsonDocument("createIndexes", new BsonString(namespace.getCollectionName()));
        List<BsonDocument> values = new ArrayList<>();
        for (IndexRequest request : requests) {
            values.add(getIndex(request));
        }
        command.put("indexes", new BsonArray(values));
        putIfNotZero(command, "maxTimeMS", maxTimeMS);
        appendWriteConcernToCommand(writeConcern, command);
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
