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
import java.util.function.Supplier;

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
import static com.mongodb.internal.operation.OperationHelper.withAsyncConnection;
import static com.mongodb.internal.operation.OperationHelper.withConnection;
import static com.mongodb.internal.operation.WriteConcernHelper.appendWriteConcernToCommand;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * An operation to create a collection
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
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

    public CreateCollectionOperation(final String databaseName, final String collectionName) {
        this(databaseName, collectionName, null);
    }

    public CreateCollectionOperation(final String databaseName, final String collectionName, @Nullable final WriteConcern writeConcern) {
        this.databaseName = notNull("databaseName", databaseName);
        this.collectionName = notNull("collectionName", collectionName);
        this.writeConcern = writeConcern;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    public boolean isAutoIndex() {
        return autoIndex;
    }

    public CreateCollectionOperation autoIndex(final boolean autoIndex) {
        this.autoIndex = autoIndex;
        return this;
    }

    public long getMaxDocuments() {
        return maxDocuments;
    }

    public CreateCollectionOperation maxDocuments(final long maxDocuments) {
        this.maxDocuments = maxDocuments;
        return this;
    }

    public boolean isCapped() {
        return capped;
    }

    public CreateCollectionOperation capped(final boolean capped) {
        this.capped = capped;
        return this;
    }

    public long getSizeInBytes() {
        return sizeInBytes;
    }

    public CreateCollectionOperation sizeInBytes(final long sizeInBytes) {
        this.sizeInBytes = sizeInBytes;
        return this;
    }

    public BsonDocument getStorageEngineOptions() {
        return storageEngineOptions;
    }

    public CreateCollectionOperation storageEngineOptions(@Nullable final BsonDocument storageEngineOptions) {
        this.storageEngineOptions = storageEngineOptions;
        return this;
    }

    public BsonDocument getIndexOptionDefaults() {
        return indexOptionDefaults;
    }

    public CreateCollectionOperation indexOptionDefaults(@Nullable final BsonDocument indexOptionDefaults) {
        this.indexOptionDefaults = indexOptionDefaults;
        return this;
    }

    public BsonDocument getValidator() {
        return validator;
    }

    public CreateCollectionOperation validator(@Nullable final BsonDocument validator) {
        this.validator = validator;
        return this;
    }

    public ValidationLevel getValidationLevel() {
        return validationLevel;
    }

    public CreateCollectionOperation validationLevel(@Nullable final ValidationLevel validationLevel) {
        this.validationLevel = validationLevel;
        return this;
    }

    public ValidationAction getValidationAction() {
        return validationAction;
    }

    public CreateCollectionOperation validationAction(@Nullable final ValidationAction validationAction) {
        this.validationAction = validationAction;
        return this;
    }

    public Collation getCollation() {
        return collation;
    }

    public CreateCollectionOperation collation(@Nullable final Collation collation) {
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

    public CreateCollectionOperation clusteredIndexKey(@Nullable final BsonDocument clusteredIndexKey) {
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
    public CreateCollectionOperation encryptedFields(@Nullable final BsonDocument encryptedFields) {
        this.encryptedFields = encryptedFields;
        return this;
    }

    @Override
    public Void execute(final WriteBinding binding) {
        return withConnection(binding, connection -> {
            getCommandFunctions().forEach(commandCreator ->
                executeCommand(binding, databaseName, commandCreator.get(), connection,
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
                new ProcessCommandsCallback(binding, connection, releasingCallback(errHandlingCallback, connection))
                        .onResult(null, null);
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
    private List<Supplier<BsonDocument>> getCommandFunctions() {
        if (encryptedFields == null) {
            return singletonList(this::getCreateCollectionCommand);
        }
        return asList(
                () -> getCreateEncryptedFieldsCollectionCommand("esc"),
                () -> getCreateEncryptedFieldsCollectionCommand("ecc"),
                () -> getCreateEncryptedFieldsCollectionCommand("ecoc"),
                this::getCreateCollectionCommand,
                () -> new BsonDocument("createIndexes", new BsonString(collectionName))
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

    private BsonDocument getCreateCollectionCommand() {
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
        appendWriteConcernToCommand(writeConcern, document);
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
        private final Deque<Supplier<BsonDocument>> commands;

        ProcessCommandsCallback(
                final AsyncWriteBinding binding, final AsyncConnection connection, final SingleResultCallback<Void> finalCallback) {
            this.binding = binding;
            this.connection = connection;
            this.finalCallback = finalCallback;
            this.commands = new ArrayDeque<>(getCommandFunctions());
        }

        @Override
        public void onResult(@Nullable final Void result, @Nullable final Throwable t) {
            if (t != null) {
                finalCallback.onResult(null, t);
                return;
            }
            Supplier<BsonDocument> nextCommandFunction = commands.poll();
            if (nextCommandFunction == null) {
                finalCallback.onResult(null, null);
            } else {
                executeCommandAsync(binding, databaseName, nextCommandFunction.get(),
                        connection, writeConcernErrorWriteTransformer(), this);
            }
        }
    }

}
