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
import com.mongodb.client.model.ChangeStreamPreAndPostImagesOptions;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.TimeSeriesGranularity;
import com.mongodb.client.model.TimeSeriesOptions;
import com.mongodb.client.model.ValidationAction;
import com.mongodb.client.model.ValidationLevel;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.internal.connection.AsyncConnection;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.function.Function;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.CommandOperationHelper.executeCommand;
import static com.mongodb.internal.operation.CommandOperationHelper.executeCommandAsync;
import static com.mongodb.internal.operation.CommandOperationHelper.writeConcernErrorTransformer;
import static com.mongodb.internal.operation.CommandOperationHelper.writeConcernErrorWriteTransformer;
import static com.mongodb.internal.operation.DocumentHelper.putIfFalse;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotNull;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotZero;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.OperationHelper.releasingCallback;
import static com.mongodb.internal.operation.OperationHelper.validateCollation;
import static com.mongodb.internal.operation.OperationHelper.withAsyncConnection;
import static com.mongodb.internal.operation.OperationHelper.withConnection;
import static com.mongodb.internal.operation.WriteConcernHelper.appendWriteConcernToCommand;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * An operation to create a collection
 *
 * @since 3.0
 * @mongodb.driver.manual reference/method/db.createCollection Create Collection
 */
public class CreateCollectionOperation implements AsyncWriteOperation<Void>, WriteOperation<Void> {
    private static final String ENCRYPT_PREFIX = "enxcol_.";
    private static final BsonDocument ENCRYPT_CLUSTERED_INDEX = BsonDocument.parse("{key: {_id: 1}, unique: true}");
    private static final BsonArray SAFE_CONTENT_ARRAY = new BsonArray(
            singletonList(BsonDocument.parse("{key: {__safeContent__: 1}, name: '__safeContent___1'}")));
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
    private long expireAfterSeconds;
    private TimeSeriesOptions timeSeriesOptions;
    private ChangeStreamPreAndPostImagesOptions changeStreamPreAndPostImagesOptions;
    private BsonDocument clusteredIndexKey;
    private boolean clusteredIndexUnique;
    private String clusteredIndexName;
    private BsonDocument encryptedFields;

    /**
     * Construct a new instance.
     *
     * @param databaseName   the name of the database for the operation.
     * @param collectionName the name of the collection to be created.
     */
    public CreateCollectionOperation(final String databaseName, final String collectionName) {
        this(databaseName, collectionName, null);
    }

    /**
     * Construct a new instance.
     *
     * @param databaseName   the name of the database for the operation.
     * @param collectionName the name of the collection to be created.
     * @param writeConcern   the write concern
     *
     * @since 3.4
     */
    public CreateCollectionOperation(final String databaseName, final String collectionName, final WriteConcern writeConcern) {
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

    public CreateCollectionOperation expireAfter(final long expireAfterSeconds) {
        this.expireAfterSeconds = expireAfterSeconds;
        return this;
    }

    public CreateCollectionOperation timeSeriesOptions(@Nullable final TimeSeriesOptions timeSeriesOptions) {
        this.timeSeriesOptions = timeSeriesOptions;
        return this;
    }

    public CreateCollectionOperation changeStreamPreAndPostImagesOptions(
            @Nullable final ChangeStreamPreAndPostImagesOptions changeStreamPreAndPostImagesOptions) {
        this.changeStreamPreAndPostImagesOptions = changeStreamPreAndPostImagesOptions;
        return this;
    }

    public CreateCollectionOperation clusteredIndexKey(final BsonDocument clusteredIndexKey) {
        this.clusteredIndexKey = clusteredIndexKey;
        return this;
    }

    public CreateCollectionOperation clusteredIndexUnique(final boolean clusteredIndexUnique) {
        this.clusteredIndexUnique = clusteredIndexUnique;
        return this;
    }

    public CreateCollectionOperation clusteredIndexName(@Nullable final String clusteredIndexName) {
        this.clusteredIndexName = clusteredIndexName;
        return this;
    }
    public CreateCollectionOperation encryptedFields(final BsonDocument encryptedFields) {
        this.encryptedFields = encryptedFields;
        return this;
    }

    @Override
    public Void execute(final WriteBinding binding) {
        return withConnection(binding, connection -> {
            validateCollation(connection, collation);
            getCommandFunctions().forEach(commandCreator ->
                executeCommand(binding, databaseName, commandCreator.apply(connection.getDescription()), connection,
                        writeConcernErrorTransformer())
            );
            return null;
        });
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<Void> callback) {
        withAsyncConnection(binding, (connection, t) -> {
            SingleResultCallback<Void> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
            if (t != null) {
                errHandlingCallback.onResult(null, t);
            } else {
                validateCollation(connection, collation, (connection1, t1) ->
                    new ProcessCommandsCallback(binding, connection1, releasingCallback(errHandlingCallback, connection))
                            .onResult(null, t1));
            }
        });
    }

    private String getGranularityAsString(final TimeSeriesGranularity granularity) {
        switch (granularity) {
            case SECONDS:
                return "seconds";
            case MINUTES:
                return "minutes";
            case HOURS:
                return "hours";
            default:
                throw new AssertionError("Unexpected granularity " + granularity);
        }
    }

    /**
     * With Queryable Encryption creating a collection can involve more logic and commands.
     *
     * <p>
     *   If the collection namespace has an associated encryptedFields, then do the following operations.
     *   If any of the following operations error, the remaining operations are not attempted:
     *   <ol>
     * <li>Create the collection with name encryptedFields["escCollection"] using default options.
     *   If encryptedFields["escCollection"] is not set, use the collection name enxcol_.<collectionName>.esc.
     *   Creating this collection MUST NOT check if the collection namespace is in the AutoEncryptionOpts.encryptedFieldsMap.
     * <li>Create the collection with name encryptedFields["eccCollection"] using default options.
     *   If encryptedFields["eccCollection"] is not set, use the collection name enxcol_.<collectionName>.ecc.
     *   Creating this collection MUST NOT check if the collection namespace is in the AutoEncryptionOpts.encryptedFieldsMap.
     * <li>Create the collection with name encryptedFields["ecocCollection"] using default options.
     *   If encryptedFields["ecocCollection"] is not set, use the collection name enxcol_.<collectionName>.ecoc.
     *   Creating this collection MUST NOT check if the collection namespace is in the AutoEncryptionOpts.encryptedFieldsMap.
     * <li>Create the collection collectionName with collectionOptions and the option encryptedFields set to the encryptedFields.
     * <li>Create the the index {"__safeContent__": 1} on collection collectionName.
     *  </ol>
     * </p>
     * @return the list of commands to run to create the collection
     */
    private List<Function<ConnectionDescription, BsonDocument>> getCommandFunctions() {
        if (encryptedFields == null) {
            return singletonList(this::getCreateCollectionCommand);
        }
        return asList(
                connectionDescription -> getCreateEncryptedFieldsCollectionCommand("esc"),
                connectionDescription -> getCreateEncryptedFieldsCollectionCommand("ecc"),
                connectionDescription -> getCreateEncryptedFieldsCollectionCommand("ecoc"),
                this::getCreateCollectionCommand,
                connectionDescription -> new BsonDocument("createIndexes", new BsonString(collectionName))
                        .append("indexes", SAFE_CONTENT_ARRAY)
        );
    }

    private BsonDocument getCreateEncryptedFieldsCollectionCommand(final String collectionSuffix) {
        return new BsonDocument()
                .append("create", encryptedFields
                        .getOrDefault(collectionSuffix + "Collection",
                                new BsonString(ENCRYPT_PREFIX + collectionName + "." + collectionSuffix)))
                .append("clusteredIndex", ENCRYPT_CLUSTERED_INDEX);
    }

    private BsonDocument getCreateCollectionCommand(final ConnectionDescription description) {
        BsonDocument document = new BsonDocument("create", new BsonString(collectionName));
        putIfFalse(document, "autoIndexId", autoIndex);
        document.put("capped", BsonBoolean.valueOf(capped));
        if (capped) {
            putIfNotZero(document, "size", sizeInBytes);
            putIfNotZero(document, "max", maxDocuments);
        }
        putIfNotNull(document, "storageEngine", storageEngineOptions);
        putIfNotNull(document, "indexOptionDefaults", indexOptionDefaults);
        putIfNotNull(document, "validator", validator);
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
        putIfNotZero(document, "expireAfterSeconds", expireAfterSeconds);
        if (timeSeriesOptions != null) {
            BsonDocument timeSeriesDocument = new BsonDocument("timeField", new BsonString(timeSeriesOptions.getTimeField()));
            String metaField = timeSeriesOptions.getMetaField();
            if (metaField != null) {
                timeSeriesDocument.put("metaField", new BsonString(metaField));
            }
            TimeSeriesGranularity granularity = timeSeriesOptions.getGranularity();
            if (granularity != null) {
                timeSeriesDocument.put("granularity", new BsonString(getGranularityAsString(granularity)));
            }
            document.put("timeseries", timeSeriesDocument);
        }
        if (changeStreamPreAndPostImagesOptions != null) {
            document.put("changeStreamPreAndPostImages", new BsonDocument("enabled",
                    BsonBoolean.valueOf(changeStreamPreAndPostImagesOptions.isEnabled())));
        }
        if (clusteredIndexKey != null) {
            BsonDocument clusteredIndexDocument = new BsonDocument()
                    .append("key", clusteredIndexKey)
                    .append("unique", BsonBoolean.valueOf(clusteredIndexUnique));
            if (clusteredIndexName != null) {
                clusteredIndexDocument.put("name", new BsonString(clusteredIndexName));
            }
            document.put("clusteredIndex", clusteredIndexDocument);
        }
        putIfNotNull(document, "encryptedFields", encryptedFields);
        return document;
    }

    /**
     * A SingleResultCallback that can be repeatedly called via onResult until all commands have been run.
     */
    class ProcessCommandsCallback implements SingleResultCallback<Void> {
        private final AsyncWriteBinding binding;
        private final AsyncConnection connection;
        private final SingleResultCallback<Void>  finalCallback;
        private final Deque<Function<ConnectionDescription, BsonDocument>> commands;

        ProcessCommandsCallback(
                final AsyncWriteBinding binding, final AsyncConnection connection, final SingleResultCallback<Void> finalCallback) {
            this.binding = binding;
            this.connection = connection;
            this.finalCallback = finalCallback;
            this.commands = new ArrayDeque<>(getCommandFunctions());
        }

        @Override
        public void onResult(final Void result, final Throwable t) {
            if (t != null) {
                finalCallback.onResult(null, t);
                return;
            }
            Function<ConnectionDescription, BsonDocument> nextCommandFunction = commands.poll();
            if (nextCommandFunction == null) {
                finalCallback.onResult(null, null);
            } else {
                executeCommandAsync(binding, databaseName, nextCommandFunction.apply(connection.getDescription()),
                        connection, writeConcernErrorWriteTransformer(), this);
            }
        }
    }

}
