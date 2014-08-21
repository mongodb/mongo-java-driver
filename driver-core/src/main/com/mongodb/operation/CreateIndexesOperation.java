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
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.async.MongoFuture;
import com.mongodb.async.SingleResultFuture;
import com.mongodb.binding.AsyncWriteBinding;
import com.mongodb.binding.WriteBinding;
import com.mongodb.connection.Connection;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.protocol.InsertProtocol;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.codecs.BsonDocumentCodec;
import org.mongodb.CommandResult;
import org.mongodb.WriteResult;

import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static com.mongodb.operation.DocumentHelper.putIfTrue;
import static com.mongodb.operation.OperationHelper.AsyncCallableWithConnection;
import static com.mongodb.operation.OperationHelper.CallableWithConnection;
import static com.mongodb.operation.OperationHelper.DUPLICATE_KEY_ERROR_CODES;
import static com.mongodb.operation.OperationHelper.serverIsAtLeastVersionTwoDotSix;
import static com.mongodb.operation.OperationHelper.withConnection;
import static java.util.Arrays.asList;

/**
 * An operation that creates one or more indexes.
 *
 * @since 3.0
 */
public class CreateIndexesOperation implements AsyncWriteOperation<Void>, WriteOperation<Void> {
    private final MongoNamespace namespace;
    private final List<Index> indexes;
    private final MongoNamespace systemIndexes;

    public CreateIndexesOperation(final MongoNamespace namespace, final List<Index> indexes) {
        this.namespace = notNull("namespace", namespace);
        this.indexes = notNull("indexes", indexes);
        this.systemIndexes = new MongoNamespace(namespace.getDatabaseName(), "system.indexes");
    }

    @Override
    public Void execute(final WriteBinding binding) {
        return withConnection(binding, new CallableWithConnection<Void>() {
            @Override
            public Void call(final Connection connection) {
                if (serverIsAtLeastVersionTwoDotSix(connection)) {
                    try {
                        executeWrappedCommandProtocol(namespace.getDatabaseName(), getCommand(), connection);
                    } catch (CommandFailureException e) {
                        throw checkForDuplicateKeyError(e);
                    }
                } else {
                    for (Index index : indexes) {
                        asInsertProtocol(index).execute(connection);
                    }
                }
                return null;
            }
        });
    }

    @Override
    public MongoFuture<Void> executeAsync(final AsyncWriteBinding binding) {
        return withConnection(binding, new AsyncCallableWithConnection<Void>() {
            @Override
            public MongoFuture<Void> call(final Connection connection) {
                final SingleResultFuture<Void> future = new SingleResultFuture<Void>();
                if (serverIsAtLeastVersionTwoDotSix(connection)) {
                    executeWrappedCommandProtocolAsync(namespace, getCommand(), connection)
                    .register(new SingleResultCallback<CommandResult>() {
                        @Override
                        public void onResult(final CommandResult result, final MongoException e) {
                            future.init(null, translateException(e));
                        }
                    });
                } else {
                    executeInsertProtocolAsync(indexes, connection, future);
                }
                return future;
            }
        });
    }

    private void executeInsertProtocolAsync(final List<Index> indexesRemaining, final Connection connection,
                                            final SingleResultFuture<Void> future) {
        Index index = indexesRemaining.remove(0);
        asInsertProtocol(index).executeAsync(connection)
                               .register(new SingleResultCallback<WriteResult>() {
                                   @Override
                                   public void onResult(final WriteResult result, final MongoException e) {
                                       MongoException translatedException = translateException(e);
                                       if (translatedException != null) {
                                           future.init(null, translatedException);
                                       } else if (indexesRemaining.isEmpty()) {
                                           future.init(null, null);
                                       } else {
                                           executeInsertProtocolAsync(indexesRemaining, connection, future);
                                       }
                                   }
                               });
    }

    private BsonDocument getCommand() {
        BsonDocument command = new BsonDocument("createIndexes", new BsonString(namespace.getCollectionName()));
        BsonArray array = new BsonArray();
        for (Index index : indexes) {
            array.add(toDocument(index));
        }
        command.put("indexes", array);

        return command;
    }

    @SuppressWarnings("unchecked")
    private InsertProtocol<BsonDocument> asInsertProtocol(final Index index) {
        return new InsertProtocol<BsonDocument>(systemIndexes, true, WriteConcern.ACKNOWLEDGED,
                                                asList(new InsertRequest<BsonDocument>(toDocument(index))),
                                                new BsonDocumentCodec());
    }

    private BsonDocument toDocument(final Index index) {
        BsonDocument indexDetails = new BsonDocument();
        indexDetails.append("name", new BsonString(index.getName()));
        indexDetails.append("key", index.getKeys());
        putIfTrue(indexDetails, "unique", index.isUnique());
        putIfTrue(indexDetails, "sparse", index.isSparse());
        putIfTrue(indexDetails, "dropDups", index.isDropDups());
        putIfTrue(indexDetails, "background", index.isBackground());
        if (index.getExpireAfterSeconds() != -1) {
            indexDetails.append("expireAfterSeconds", new BsonInt32(index.getExpireAfterSeconds()));
        }
        indexDetails.putAll(index.getExtra());
        indexDetails.put("ns", new BsonString(namespace.getFullName()));

        return indexDetails;
    }

    private MongoException translateException(final MongoException e) {
        return (e instanceof CommandFailureException) ? checkForDuplicateKeyError((CommandFailureException) e) : e;
    }

    private MongoException checkForDuplicateKeyError(final CommandFailureException e) {
        if (DUPLICATE_KEY_ERROR_CODES.contains(e.getCode())) {
            return new MongoException.DuplicateKey(e.getResponse(), e.getServerAddress(), new com.mongodb.WriteResult(0, false, null));
        } else {
            return e;
        }
    }
}
