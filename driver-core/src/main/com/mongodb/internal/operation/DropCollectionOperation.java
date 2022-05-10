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

import com.mongodb.MongoCommandException;
import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.binding.AsyncReadWriteBinding;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.binding.ReadWriteBinding;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.internal.connection.AsyncConnection;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.BsonValueCodec;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.function.Function;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.CommandOperationHelper.executeCommand;
import static com.mongodb.internal.operation.CommandOperationHelper.executeCommandAsync;
import static com.mongodb.internal.operation.CommandOperationHelper.isNamespaceError;
import static com.mongodb.internal.operation.CommandOperationHelper.rethrowIfNotNamespaceError;
import static com.mongodb.internal.operation.CommandOperationHelper.writeConcernErrorTransformer;
import static com.mongodb.internal.operation.CommandOperationHelper.writeConcernErrorWriteTransformer;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.OperationHelper.releasingCallback;
import static com.mongodb.internal.operation.OperationHelper.withAsyncConnection;
import static com.mongodb.internal.operation.OperationHelper.withConnection;
import static com.mongodb.internal.operation.WriteConcernHelper.appendWriteConcernToCommand;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * Operation to drop a Collection in MongoDB.  The {@code execute} method throws MongoCommandFailureException if something goes wrong, but
 * it will not throw an Exception if the collection does not exist before trying to drop it.
 *
 * @since 3.0
 */
public class DropCollectionOperation implements AsyncWriteOperation<Void>, WriteOperation<Void> {
    private static final String ENCRYPT_PREFIX = "enxcol_.";
    private static final BsonValueCodec BSON_VALUE_CODEC = new BsonValueCodec();
    private final MongoNamespace namespace;
    private final WriteConcern writeConcern;
    private BsonDocument encryptedFields;
    private boolean autoEncryptedFields;

    /**
     * Construct a new instance.
     *
     * @param namespace the database and collection namespace for the operation.
     */
    public DropCollectionOperation(final MongoNamespace namespace) {
        this(namespace, null);
    }

    /**
     * Construct a new instance.
     *
     * @param namespace    the database and collection namespace for the operation.
     * @param writeConcern the write concern
     * @since 3.4
     */
    public DropCollectionOperation(final MongoNamespace namespace, final WriteConcern writeConcern) {
        this.namespace = notNull("namespace", namespace);
        this.writeConcern = writeConcern;
    }

    /**
     * Gets the write concern.
     *
     * @return the write concern, which may be null
     * @since 3.4
     */
    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    public DropCollectionOperation encryptedFields(final BsonDocument encryptedFields) {
        this.encryptedFields = encryptedFields;
        return this;
    }

    public DropCollectionOperation autoEncryptedFields(final boolean autoEncryptedFields) {
        this.autoEncryptedFields = autoEncryptedFields;
        return this;
    }

    @Override
    public Void execute(final WriteBinding binding) {
        BsonDocument localEncryptedFields = getEncryptedFields((ReadWriteBinding) binding);
        return withConnection(binding, connection -> {
            getCommands(localEncryptedFields).forEach(command -> {
                try {
                    executeCommand(binding, namespace.getDatabaseName(), command.apply(connection.getDescription()),
                            connection, writeConcernErrorTransformer());
                } catch (MongoCommandException e) {
                    rethrowIfNotNamespaceError(e);
                }
            });
            return null;
        });
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<Void> callback) {
        SingleResultCallback<Void> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
        getEncryptedFields((AsyncReadWriteBinding) binding, (result, t) -> {
            if (t != null) {
                errHandlingCallback.onResult(null, t);
            } else {
                withAsyncConnection(binding, (connection, t1) -> {
                    if (t1 != null) {
                        errHandlingCallback.onResult(null, t1);
                    } else {
                        new ProcessCommandsCallback(binding, connection, getCommands(result), releasingCallback(errHandlingCallback,
                                connection))
                                .onResult(null, null);
                    }
                });
            }
        });
    }

    /**
     * With FLE2 dropping a collection can involve more logic and commands.
     *
     * <p>
     * A call to a driver helper Collection.Drop(dropOptions) must check if the collection namespace (<databaseName>.<collectionName>)
     * has an associated encryptedFields. Check for an associated encryptedFields from the following:
     * <ol>
     *     <li>The encryptedFields option passed in dropOptions.</li>
     *     <li>The value of AutoEncryptionOpts.encryptedFieldsMap[<databaseName>.<collectionName>].</li>
     *     <li>If AutoEncryptionOpts.encryptedFieldsMap is not null, run a listCollections command on the database databaseName with the
     *          filter { "name": "<collectionName>" }. Check the returned options for the encryptedFields option.</li>
     * </ol>
     * </p>
     * <p>
     * If the collection namespace has an associated encryptedFields, then do the following operations.
     * If any of the following operations error, the remaining operations are not attempted:
     * <ol>
     * <li>Drop the collection collectionName.
     * <li>Drop the collection with name encryptedFields["escCollection"].
     *    If encryptedFields["escCollection"] is not set, use the collection name enxcol_.<collectionName>.esc.</li>
     * <li>Drop the collection with name encryptedFields["eccCollection"].
     *    If encryptedFields["eccCollection"] is not set, use the collection name enxcol_.<collectionName>.ecc.</li>
     * <li>Drop the collection with name encryptedFields["ecocCollection"].
     *    If encryptedFields["ecocCollection"] is not set, use the collection name enxcol_.<collectionName>.ecoc.</li>
     * </ol>
     * </p>
     *
     * @return the list of commands to run to create the collection
     */
    private List<Function<ConnectionDescription, BsonDocument>> getCommands(final BsonDocument encryptedFields) {
        if (encryptedFields == null) {
            return singletonList(this::dropCollectionCommand);
        } else  {
            return asList(
                    this::dropCollectionCommand,
                    connectionDescription -> new BsonDocument("drop", getEncryptedFieldsCollection("esc")),
                    connectionDescription -> new BsonDocument("drop", getEncryptedFieldsCollection("ecc")),
                    connectionDescription -> new BsonDocument("drop", getEncryptedFieldsCollection("ecoc"))
            );
        }
    }

    private BsonValue getEncryptedFieldsCollection(final String collectionSuffix) {
        BsonString defaultCollectionName = new BsonString(ENCRYPT_PREFIX + namespace.getCollectionName() + "." + collectionSuffix);
        return encryptedFields.getOrDefault(collectionSuffix + "Collection", defaultCollectionName);
    }

    private BsonDocument dropCollectionCommand(final ConnectionDescription description) {
        BsonDocument commandDocument = new BsonDocument("drop", new BsonString(namespace.getCollectionName()));
        appendWriteConcernToCommand(writeConcern, commandDocument, description);
        return commandDocument;
    }

    private BsonDocument getEncryptedFields(final ReadWriteBinding readWriteBinding) {
        BsonDocument localEncryptedFields = encryptedFields;
        if (localEncryptedFields == null && autoEncryptedFields) {
            try (BatchCursor<BsonValue> cursor =  listCollectionOperation().execute(readWriteBinding)) {
                List<BsonValue> bsonValues = cursor.tryNext();
                if (bsonValues != null && bsonValues.size() > 0) {
                    localEncryptedFields =  bsonValues.get(0).asDocument().getDocument("encryptedFields", new BsonDocument());
                }
            }
        }
        return localEncryptedFields;
    }

    private void getEncryptedFields(
            final AsyncReadWriteBinding asyncReadWriteBinding,
            final SingleResultCallback<BsonDocument> callback) {
        BsonDocument localEncryptedFields = encryptedFields;
        if (localEncryptedFields == null && autoEncryptedFields) {
            listCollectionOperation().executeAsync(asyncReadWriteBinding, (cursor, t) -> {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    cursor.next((bsonValues, t1) -> {
                        if (t1 != null) {
                            callback.onResult(null, t1);
                        } else if (bsonValues != null && bsonValues.size() > 0) {
                            callback.onResult(bsonValues.get(0).asDocument().getDocument("encryptedFields", new BsonDocument()), null);
                        } else {
                            callback.onResult(null, null);
                        }
                    });
                }
            });
        } else {
            callback.onResult(localEncryptedFields, null);
        }
    }

    private ListCollectionsOperation<BsonValue> listCollectionOperation() {
        return new ListCollectionsOperation<>(namespace.getDatabaseName(), BSON_VALUE_CODEC)
                .filter(new BsonDocument("name", new BsonString(namespace.getCollectionName())))
                .batchSize(1);
    }

    /**
     * A SingleResultCallback that can be repeatedly called via onResult until all commands have been run.
     */
    class ProcessCommandsCallback implements SingleResultCallback<Void> {
        private final AsyncWriteBinding binding;
        private final AsyncConnection connection;
        private final SingleResultCallback<Void> finalCallback;
        private final Deque<Function<ConnectionDescription, BsonDocument>> commands;

        ProcessCommandsCallback(
                final AsyncWriteBinding binding, final AsyncConnection connection,
                final List<Function<ConnectionDescription, BsonDocument>> commands,
                final SingleResultCallback<Void> finalCallback) {
            this.binding = binding;
            this.connection = connection;
            this.finalCallback = finalCallback;
            this.commands = new ArrayDeque<>(commands);
        }

        @Override
        public void onResult(final Void result, final Throwable t) {
            if (t != null && !isNamespaceError(t)) {
                finalCallback.onResult(null, t);
            }
            Function<ConnectionDescription, BsonDocument> nextCommandFunction = commands.poll();
            if (nextCommandFunction == null) {
                finalCallback.onResult(null, null);
            } else {
                executeCommandAsync(binding, namespace.getDatabaseName(), nextCommandFunction.apply(connection.getDescription()),
                        connection, writeConcernErrorWriteTransformer(), this);
            }
        }
    }

}
