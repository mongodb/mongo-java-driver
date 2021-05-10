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

import com.mongodb.WriteConcern;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.ValidationAction;
import com.mongodb.client.model.ValidationLevel;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.ClientSideOperationTimeout;
import com.mongodb.internal.ClientSideOperationTimeoutFactory;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.internal.connection.AsyncConnection;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.operation.SyncOperationHelper.CallableWithConnection;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.AsyncCommandOperationHelper.executeCommandAsync;
import static com.mongodb.internal.operation.AsyncCommandOperationHelper.writeConcernErrorTransformerAsync;
import static com.mongodb.internal.operation.AsyncOperationHelper.AsyncCallableWithConnection;
import static com.mongodb.internal.operation.AsyncOperationHelper.releasingCallback;
import static com.mongodb.internal.operation.AsyncOperationHelper.withAsyncConnection;
import static com.mongodb.internal.operation.DocumentHelper.putIfFalse;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotZero;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.SyncCommandOperationHelper.executeCommand;
import static com.mongodb.internal.operation.SyncCommandOperationHelper.writeConcernErrorTransformer;
import static com.mongodb.internal.operation.SyncOperationHelper.withConnection;
import static com.mongodb.internal.operation.WriteConcernHelper.appendWriteConcernToCommand;

/**
 * An operation to create a collection
 *
 * @since 3.0
 * @mongodb.driver.manual reference/method/db.createCollection Create Collection
 */
public class CreateCollectionOperation implements AsyncWriteOperation<Void>, WriteOperation<Void> {
    private final ClientSideOperationTimeoutFactory clientSideOperationTimeoutFactory;
    private final String databaseName;
    private final String collectionName;
    private final WriteConcern writeConcern;
    private boolean capped = false;
    private long sizeInBytes = 0;
    private boolean autoIndex = true;
    private long maxDocuments = 0;
    private BsonDocument storageEngineOptions;
    private BsonDocument indexOptionDefaults;
    private BsonDocument validator;
    private ValidationLevel validationLevel = null;
    private ValidationAction validationAction = null;
    private Collation collation = null;

    /**
     * Construct a new instance.
     *
     * @param clientSideOperationTimeoutFactory the client side operation timeout factory
     * @param databaseName   the name of the database for the operation.
     * @param collectionName the name of the collection to be created.
     */
    public CreateCollectionOperation(final ClientSideOperationTimeoutFactory clientSideOperationTimeoutFactory, final String databaseName,
                                     final String collectionName) {
        this(clientSideOperationTimeoutFactory, databaseName, collectionName, null);
    }

    /**
     * Construct a new instance.
     *
     * @param clientSideOperationTimeoutFactory the client side operation  factory
     * @param databaseName   the name of the database for the operation.
     * @param collectionName the name of the collection to be created.
     * @param writeConcern   the write concern
     *
     * @since 3.4
     */
    public CreateCollectionOperation(final ClientSideOperationTimeoutFactory clientSideOperationTimeoutFactory, final String databaseName,
                                     final String collectionName, final WriteConcern writeConcern) {
        this.clientSideOperationTimeoutFactory = notNull("clientSideOperationTimeoutFactory", clientSideOperationTimeoutFactory);
        this.databaseName = notNull("databaseName", databaseName);
        this.collectionName = notNull("collectionName", collectionName);
        this.writeConcern = writeConcern;
    }

    /**
     * Gets the name of the collection to create.
     *
     * @return the collection name
     */
    public String getCollectionName() {
        return collectionName;
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
     * The auto index value.
     *
     * @return true if auto-index is enabled
     */
    public boolean isAutoIndex() {
        return autoIndex;
    }

    /**
     * Sets if _id field of the collection is indexed.  Only applies to capped collections and defaults to true.
     *
     * @param autoIndex true if auto-index of _id is enabled. Only applies to capped collections.
     * @return this
     */
    public CreateCollectionOperation autoIndex(final boolean autoIndex) {
        this.autoIndex = autoIndex;
        return this;
    }

    /**
     * Gets the maximum number of documents allowed in the collection.
     *
     * @return max number of documents in the collection
     */
    public long getMaxDocuments() {
        return maxDocuments;
    }

    /**
     * Set the maximum number of documents in the collection.  Only applies to capped collections
     *
     * @param maxDocuments the maximum number of documents in the collection.  Only applies to capped collections.
     * @return this
     */
    public CreateCollectionOperation maxDocuments(final long maxDocuments) {
        this.maxDocuments = maxDocuments;
        return this;
    }

    /**
     * Gets whether the collection is capped.
     *
     * @return whether the collection is capped
     */
    public boolean isCapped() {
        return capped;
    }

    /**
     * Sets whether the collection is capped.
     *
     * Capped collections also require the size set see {@link #sizeInBytes }.
     *
     * @param capped whether the collection is capped. Defaults to false.
     * @return this
     */
    public CreateCollectionOperation capped(final boolean capped) {
        this.capped = capped;
        return this;
    }

    /**
     * Gets the maximum size of the collection in bytes.
     *
     * @return the maximum size of the collection
     */
    public long getSizeInBytes() {
        return sizeInBytes;
    }

    /**
     * Sets the maximum size of the collection in bytes. Required for capped collections.
     *
     * @param sizeInBytes the maximum size of the collection
     * @return this
     */
    public CreateCollectionOperation sizeInBytes(final long sizeInBytes) {
        this.sizeInBytes = sizeInBytes;
        return this;
    }

    /**
     * Gets the storage engine options document for this collection.
     *
     * @return the storage engine options
     * @mongodb.server.release 3.0
     */
    public BsonDocument getStorageEngineOptions() {
        return storageEngineOptions;
    }

    /**
     * Sets the storage engine options document for this collection.
     *
     * @param storageEngineOptions the storage engine options
     * @return this
     * @mongodb.server.release 3.0
     */
    public CreateCollectionOperation storageEngineOptions(final BsonDocument storageEngineOptions) {
        this.storageEngineOptions = storageEngineOptions;
        return this;
    }

    /**
     * Gets the index option defaults for the collection.
     *
     * @return the index option defaults
     * @since 3.2
     * @mongodb.server.release 3.2
     */
    public BsonDocument getIndexOptionDefaults() {
        return indexOptionDefaults;
    }

    /**
     * Sets the index option defaults document for the collection.
     *
     * @param indexOptionDefaults the index option defaults
     * @return this
     * @since 3.2
     * @mongodb.server.release 3.2
     */
    public CreateCollectionOperation indexOptionDefaults(final BsonDocument indexOptionDefaults) {
        this.indexOptionDefaults = indexOptionDefaults;
        return this;
    }

    /**
     * Gets the validation rules for inserting or updating documents
     *
     * @return the validation rules if set or null
     * @since 3.2
     * @mongodb.server.release 3.2
     */
    public BsonDocument getValidator() {
        return validator;
    }

    /**
     * Sets the validation rules for inserting or updating documents
     *
     * @param validator the validation rules for inserting or updating documents
     * @return this
     * @since 3.2
     * @mongodb.server.release 3.2
     */
    public CreateCollectionOperation validator(final BsonDocument validator) {
        this.validator = validator;
        return this;
    }

    /**
     * Gets the {@link ValidationLevel} that determines how strictly MongoDB applies the validation rules to existing documents during an
     * insert or update.
     *
     * @return the ValidationLevel if set or null
     * @since 3.2
     * @mongodb.server.release 3.2
     */
    public ValidationLevel getValidationLevel() {
        return validationLevel;
    }

    /**
     * Sets the validation level that determines how strictly MongoDB applies the validation rules to existing documents during an insert
     * or update.
     *
     * @param validationLevel the validation level
     * @return this
     * @since 3.2
     * @mongodb.server.release 3.2
     */
    public CreateCollectionOperation validationLevel(final ValidationLevel validationLevel) {
        this.validationLevel = validationLevel;
        return this;
    }

    /**
     * Gets the {@link ValidationAction}.
     *
     * @return the ValidationAction if set or null
     * @since 3.2
     * @mongodb.server.release 3.2
     */
    public ValidationAction getValidationAction() {
        return validationAction;
    }

    /**
     * Sets the {@link ValidationAction} that determines whether to error on invalid documents or just warn about the violations but allow
     * invalid documents.
     *
     * @param validationAction the validation action
     * @return this
     * @since 3.2
     * @mongodb.server.release 3.2
     */
    public CreateCollectionOperation validationAction(final ValidationAction validationAction) {
        this.validationAction = validationAction;
        return this;
    }

    /**
     * Returns the collation options
     *
     * @return the collation options
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    public Collation getCollation() {
        return collation;
    }

    /**
     * Sets the collation options
     *
     * <p>A null value represents the server default.</p>
     * @param collation the collation options to use
     * @return this
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    public CreateCollectionOperation collation(final Collation collation) {
        this.collation = collation;
        return this;
    }

    @Override
    public Void execute(final WriteBinding binding) {
        return withConnection(clientSideOperationTimeoutFactory.create(), binding, new CallableWithConnection<Void>() {
            @Override
            public Void call(final ClientSideOperationTimeout clientSideOperationTimeout, final Connection connection) {
                SyncOperationHelper.validateCollation(connection, collation);
                executeCommand(clientSideOperationTimeout, binding, databaseName, getCommand(connection.getDescription()), connection,
                        writeConcernErrorTransformer());
                return null;
            }
        });
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<Void> callback) {
        withAsyncConnection(clientSideOperationTimeoutFactory.create(), binding, new AsyncCallableWithConnection() {
            @Override
            public void call(final ClientSideOperationTimeout clientSideOperationTimeout, final AsyncConnection connection,
                             final Throwable t) {
                SingleResultCallback<Void> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
                if (t != null) {
                    errHandlingCallback.onResult(null, t);
                } else {
                    final SingleResultCallback<Void> wrappedCallback = releasingCallback(errHandlingCallback, connection);
                    AsyncOperationHelper.validateCollation(clientSideOperationTimeout, connection, collation,
                            new AsyncCallableWithConnection() {
                        @Override
                        public void call(final ClientSideOperationTimeout clientSideOperationTimeout, final AsyncConnection connection,
                                         final Throwable t) {
                            if (t != null) {
                                wrappedCallback.onResult(null, t);
                            } else {
                                executeCommandAsync(clientSideOperationTimeout, binding, databaseName,
                                        getCommand(connection.getDescription()),
                                        connection, writeConcernErrorTransformerAsync(), wrappedCallback);
                            }
                        }
                    });
                }
            }
        });
    }

    private BsonDocument getCommand(final ConnectionDescription description) {
        BsonDocument document = new BsonDocument("create", new BsonString(collectionName));
        putIfFalse(document, "autoIndexId", autoIndex);
        document.put("capped", BsonBoolean.valueOf(capped));
        if (capped) {
            putIfNotZero(document, "size", sizeInBytes);
            putIfNotZero(document, "max", maxDocuments);
        }
        if (storageEngineOptions != null) {
            document.put("storageEngine", storageEngineOptions);
        }
        if (indexOptionDefaults != null) {
            document.put("indexOptionDefaults", indexOptionDefaults);
        }
        if (validator != null) {
            document.put("validator", validator);
        }
        if (validationLevel != null) {
            document.put("validationLevel", new BsonString(validationLevel.getValue()));
        }
        if (validationAction != null) {
            document.put("validationAction", new BsonString(validationAction.getValue()));
        }
        appendWriteConcernToCommand(writeConcern, document, description);
        if (collation != null) {
            document.put("collation", collation.asDocument());
        }
        return document;
    }

}
