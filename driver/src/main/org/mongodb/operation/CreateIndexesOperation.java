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
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.ServerVersion;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.protocol.InsertProtocol;
import org.mongodb.session.ServerConnectionProvider;
import org.mongodb.session.Session;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.mongodb.operation.OperationHelper.DUPLICATE_KEY_ERROR_CODES;
import static org.mongodb.operation.OperationHelper.executeProtocol;
import static org.mongodb.operation.OperationHelper.executeProtocolAsync;
import static org.mongodb.operation.OperationHelper.executeWrappedCommandProtocol;
import static org.mongodb.operation.OperationHelper.executeWrappedCommandProtocolAsync;
import static org.mongodb.operation.OperationHelper.getPrimaryConnectionProvider;
import static org.mongodb.operation.OperationHelper.getPrimaryConnectionProviderAsync;
import static org.mongodb.operation.OperationHelper.serverVersionIsAtLeast;

public class CreateIndexesOperation implements AsyncOperation<Void>, Operation<Void> {
    private final List<Index> indexes;
    private final MongoNamespace namespace;
    private final MongoNamespace systemIndexes;

    public CreateIndexesOperation(final List<Index> indexes, final MongoNamespace namespace) {
        this.indexes = indexes;
        this.namespace = namespace;
        this.systemIndexes = new MongoNamespace(namespace.getDatabaseName(), "system.indexes");
    }

    @Override
    public Void execute(final Session session) {
        ServerConnectionProvider connectionProvider = getPrimaryConnectionProvider(session);
        if (connectionProvider.getServerDescription().getVersion().compareTo(new ServerVersion(2, 6)) >= 0) {
            try {
                executeWrappedCommandProtocol(namespace.getDatabaseName(), getCommand(), connectionProvider);
            } catch (MongoCommandFailureException e) {
                throw checkForDuplicateKeyError(e);
            }
        } else {
            for (Index index : indexes) {
                executeProtocol(asInsertProtocol(index), connectionProvider);
            }
        }
        return null;
    }

    @Override
    public MongoFuture<Void> executeAsync(final Session session) {
        final SingleResultFuture<Void> retVal = new SingleResultFuture<Void>();
        getPrimaryConnectionProviderAsync(session).register(new SingleResultCallback<ServerConnectionProvider>() {
            @Override
            public void onResult(final ServerConnectionProvider connectionProvider, final MongoException e) {
                if (serverVersionIsAtLeast(connectionProvider, new ServerVersion(2, 6))) {
                    executeWrappedCommandProtocolAsync(namespace.getDatabaseName(), getCommand(), connectionProvider)
                    .register(new SingleResultCallback<CommandResult>() {
                        @Override
                        public void onResult(final CommandResult result, final MongoException e1) {
                            retVal.init(null, translateException(e1));
                        }
                    });
                } else {
                    executeInsertProtocolAsync(retVal, connectionProvider, indexes);
                }
            }
        });
        return retVal;
    }

    private void executeInsertProtocolAsync(final SingleResultFuture<Void> retVal, final ServerConnectionProvider connectionProvider,
                                            final List<Index> indexesRemaining) {
        Index index = indexesRemaining.remove(0);
        executeProtocolAsync(asInsertProtocol(index), connectionProvider)
        .register(new SingleResultCallback<WriteResult>() {
            @Override
            public void onResult(final WriteResult result, final MongoException e) {
                MongoException translatedException = translateException(e);
                if (translatedException != null) {
                    retVal.init(null, translatedException);
                } else if (indexesRemaining.isEmpty()) {
                    retVal.init(null, null);
                } else {
                    executeInsertProtocolAsync(retVal, connectionProvider, indexesRemaining);
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
