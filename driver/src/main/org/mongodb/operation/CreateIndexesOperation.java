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

import org.mongodb.CommandResult;
import org.mongodb.Document;
import org.mongodb.Index;
import org.mongodb.MongoCommandFailureException;
import org.mongodb.MongoDuplicateKeyException;
import org.mongodb.MongoException;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.MongoServerException;
import org.mongodb.WriteConcern;
import org.mongodb.WriteResult;
import org.mongodb.binding.AsyncWriteBinding;
import org.mongodb.binding.WriteBinding;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ServerVersion;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.protocol.InsertProtocol;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static org.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static org.mongodb.operation.OperationHelper.AsyncCallableWithConnection;
import static org.mongodb.operation.OperationHelper.CallableWithConnection;
import static org.mongodb.operation.OperationHelper.DUPLICATE_KEY_ERROR_CODES;
import static org.mongodb.operation.OperationHelper.serverIsAtLeastVersionTwoDotSix;
import static org.mongodb.operation.OperationHelper.withConnection;

/**
 * An operation that creates one or more indexes.
 *
 * @since 3.0
 */
public class CreateIndexesOperation implements AsyncWriteOperation<Void>, WriteOperation<Void> {
    private final List<Index> indexes;
    private final MongoNamespace namespace;
    private final MongoNamespace systemIndexes;

    public CreateIndexesOperation(final List<Index> indexes, final MongoNamespace namespace) {
        this.indexes = indexes;
        this.namespace = namespace;
        this.systemIndexes = new MongoNamespace(namespace.getDatabaseName(), "system.indexes");
    }

    @Override
    public Void execute(final WriteBinding binding) {
        return withConnection(binding, new CallableWithConnection<Void>() {
            @Override
            public Void call(final Connection connection) {
                if (connection.getServerDescription().getVersion().compareTo(new ServerVersion(2, 6)) >= 0) {
                    try {
                        executeWrappedCommandProtocol(namespace.getDatabaseName(), getCommand(), connection);
                    } catch (MongoCommandFailureException e) {
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
                                            final SingleResultFuture<Void> retVal) {
        Index index = indexesRemaining.remove(0);
        asInsertProtocol(index).executeAsync(connection)
        .register(new SingleResultCallback<WriteResult>() {
            @Override
            public void onResult(final WriteResult result, final MongoException e) {
                MongoException translatedException = translateException(e);
                if (translatedException != null) {
                    retVal.init(null, translatedException);
                } else if (indexesRemaining.isEmpty()) {
                    retVal.init(null, null);
                } else {
                    executeInsertProtocolAsync(indexesRemaining, connection, retVal);
                }
            }
        });
    }

    private Document getCommand() {
        Document command = new Document("createIndexes", namespace.getCollectionName());
        List<Document> list = new ArrayList<Document>();
        for (Index index : indexes) {
            list.add(toDocument(index));
        }
        command.append("indexes", list);

        return command;
    }

    @SuppressWarnings("unchecked")
    private InsertProtocol<Document> asInsertProtocol(final Index index) {
        return new InsertProtocol<Document>(systemIndexes, true, WriteConcern.ACKNOWLEDGED,
                                            asList(new InsertRequest<Document>(toDocument(index))),
                                            new DocumentCodec());
    }

    private Document toDocument(final Index index) {
        Document indexDetails = new Document();
        indexDetails.append("name", index.getName());
        indexDetails.append("key", index.getKeys());
        if (index.isUnique()) {
            indexDetails.append("unique", index.isUnique());
        }
        if (index.isSparse()) {
            indexDetails.append("sparse", index.isSparse());
        }
        if (index.isDropDups()) {
            indexDetails.append("dropDups", index.isDropDups());
        }
        if (index.isBackground()) {
            indexDetails.append("background", index.isBackground());
        }
        if (index.getExpireAfterSeconds() != -1) {
            indexDetails.append("expireAfterSeconds", index.getExpireAfterSeconds());
        }
        indexDetails.putAll(index.getExtra());
        indexDetails.put("ns", namespace.toString());

        return indexDetails;
    }

    private MongoException translateException(final MongoException e) {
        return (e instanceof MongoCommandFailureException) ? checkForDuplicateKeyError((MongoCommandFailureException) e) : null;
    }

    private MongoServerException checkForDuplicateKeyError(final MongoCommandFailureException e) {
        if (DUPLICATE_KEY_ERROR_CODES.contains(e.getErrorCode())) {
            return new MongoDuplicateKeyException(e.getErrorCode(), e.getErrorMessage(), e.getCommandResult());
        } else {
            return e;
        }
    }
}
