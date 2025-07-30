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
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.internal.bulk.IndexRequest;
import com.mongodb.lang.Nullable;
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

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.operation.AsyncOperationHelper.executeCommandAsync;
import static com.mongodb.internal.operation.AsyncOperationHelper.writeConcernErrorTransformerAsync;
import static com.mongodb.internal.operation.IndexHelper.generateIndexName;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsAtLeastVersionFourDotFour;
import static com.mongodb.internal.operation.SyncOperationHelper.executeCommand;
import static com.mongodb.internal.operation.SyncOperationHelper.writeConcernErrorTransformer;
import static com.mongodb.internal.operation.WriteConcernHelper.appendWriteConcernToCommand;

/**
 * An operation that creates one or more indexes.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class CreateIndexesOperation implements AsyncWriteOperation<Void>, WriteOperation<Void> {
    private static final String COMMAND_NAME = "createIndexes";
    private final MongoNamespace namespace;
    private final List<IndexRequest> requests;
    private final WriteConcern writeConcern;
    private CreateIndexCommitQuorum commitQuorum;

    public CreateIndexesOperation(final MongoNamespace namespace, final List<IndexRequest> requests,
            @Nullable final WriteConcern writeConcern) {
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

    public CreateIndexCommitQuorum getCommitQuorum() {
        return commitQuorum;
    }

    public CreateIndexesOperation commitQuorum(@Nullable final CreateIndexCommitQuorum commitQuorum) {
        this.commitQuorum = commitQuorum;
        return this;
    }

    @Override
    public String getCommandName() {
        return COMMAND_NAME;
    }

    @Override
    public Void execute(final WriteBinding binding) {
        try {
            return executeCommand(binding, namespace.getDatabaseName(), getCommandCreator(), writeConcernErrorTransformer(
                    binding.getOperationContext().getTimeoutContext()));
        } catch (MongoCommandException e) {
            throw checkForDuplicateKeyError(e);
        }
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<Void> callback) {
        executeCommandAsync(binding, namespace.getDatabaseName(), getCommandCreator(), writeConcernErrorTransformerAsync(binding
                        .getOperationContext().getTimeoutContext()),
                ((result, t) -> {
                    if (t != null) {
                        callback.onResult(null, translateException(t));
                    } else {
                        callback.onResult(result, null);
                    }
                }));
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
            index.append("expireAfterSeconds", new BsonInt64(assertNotNull(request.getExpireAfter(TimeUnit.SECONDS))));
        }
        if (request.getVersion() != null) {
            index.append("v", new BsonInt32(assertNotNull(request.getVersion())));
        }
        if (request.getWeights() != null) {
            index.append("weights", assertNotNull(request.getWeights()));
        }
        if (request.getDefaultLanguage() != null) {
            index.append("default_language", new BsonString(assertNotNull(request.getDefaultLanguage())));
        }
        if (request.getLanguageOverride() != null) {
            index.append("language_override", new BsonString(assertNotNull(request.getLanguageOverride())));
        }
        if (request.getTextVersion() != null) {
            index.append("textIndexVersion", new BsonInt32(assertNotNull(request.getTextVersion())));
        }
        if (request.getSphereVersion() != null) {
            index.append("2dsphereIndexVersion", new BsonInt32(assertNotNull(request.getSphereVersion())));
        }
        if (request.getBits() != null) {
            index.append("bits", new BsonInt32(assertNotNull(request.getBits())));
        }
        if (request.getMin() != null) {
            index.append("min", new BsonDouble(assertNotNull(request.getMin())));
        }
        if (request.getMax() != null) {
            index.append("max", new BsonDouble(assertNotNull(request.getMax())));
        }
        if (request.getDropDups()) {
            index.append("dropDups", BsonBoolean.TRUE);
        }
        if (request.getStorageEngine() != null) {
            index.append("storageEngine", assertNotNull(request.getStorageEngine()));
        }
        if (request.getPartialFilterExpression() != null) {
            index.append("partialFilterExpression", assertNotNull(request.getPartialFilterExpression()));
        }
        if (request.getCollation() != null) {
            index.append("collation", assertNotNull(request.getCollation().asDocument()));
        }
        if (request.getWildcardProjection() != null) {
            index.append("wildcardProjection", assertNotNull(request.getWildcardProjection()));
        }
        if (request.isHidden()) {
            index.append("hidden", BsonBoolean.TRUE);
        }
        return index;
    }

    private CommandOperationHelper.CommandCreator getCommandCreator() {
        return (operationContext, serverDescription, connectionDescription) -> {
            BsonDocument command = new BsonDocument(getCommandName(), new BsonString(namespace.getCollectionName()));
            List<BsonDocument> values = new ArrayList<>();
            for (IndexRequest request : requests) {
                values.add(getIndex(request));
            }
            command.put("indexes", new BsonArray(values));
            appendWriteConcernToCommand(writeConcern, command);
            if (commitQuorum != null) {
                if (serverIsAtLeastVersionFourDotFour(connectionDescription)) {
                    command.put("commitQuorum", commitQuorum.toBsonValue());
                } else {
                    throw new MongoClientException("Specifying a value for the create index commit quorum option "
                            + "requires a minimum MongoDB version of 4.4");
                }
            }
            return command;
        };
    }

    @Nullable
    private MongoException translateException(@Nullable final Throwable t) {
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
