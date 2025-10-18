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

import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.client.model.Collation;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.client.model.AggregationLevel;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;

import java.util.List;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.operation.AsyncOperationHelper.CommandReadTransformerAsync;
import static com.mongodb.internal.operation.AsyncOperationHelper.executeRetryableReadAsync;
import static com.mongodb.internal.operation.ServerVersionHelper.FIVE_DOT_ZERO_WIRE_VERSION;
import static com.mongodb.internal.operation.SyncOperationHelper.CommandReadTransformer;
import static com.mongodb.internal.operation.SyncOperationHelper.executeRetryableRead;
import static com.mongodb.internal.operation.WriteConcernHelper.appendWriteConcernToCommand;
import static com.mongodb.internal.operation.WriteConcernHelper.throwOnWriteConcernError;

/**
 * An operation that executes an aggregation that writes its results to a collection.
 *
 * <p>Drivers are required to execute this operation on a secondary as of MongoDB 5.0, and otherwise execute it on a primary. That's why
 * this is a ReadOperation, not a WriteOperation: because it now uses the read preference to select the server.
 * </p>
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class AggregateToCollectionOperation implements ReadOperationSimple<Void> {
    private static final String COMMAND_NAME = "aggregate";
    private final MongoNamespace namespace;
    private final List<BsonDocument> pipeline;
    private final WriteConcern writeConcern;
    private final ReadConcern readConcern;
    private final AggregationLevel aggregationLevel;

    private Boolean allowDiskUse;
    private Boolean bypassDocumentValidation;
    private Collation collation;
    private BsonValue comment;
    private BsonValue hint;
    private BsonDocument variables;

    public AggregateToCollectionOperation(final MongoNamespace namespace, final List<BsonDocument> pipeline, final ReadConcern readConcern,
            final WriteConcern writeConcern) {
        this(namespace, pipeline, readConcern, writeConcern, AggregationLevel.COLLECTION);
    }

    public AggregateToCollectionOperation(final MongoNamespace namespace, final List<BsonDocument> pipeline,
            @Nullable final ReadConcern readConcern, @Nullable final WriteConcern writeConcern, final AggregationLevel aggregationLevel) {
        this.namespace = notNull("namespace", namespace);
        this.pipeline = notNull("pipeline", pipeline);
        this.writeConcern = writeConcern;
        this.readConcern = readConcern;
        this.aggregationLevel = notNull("aggregationLevel", aggregationLevel);

        isTrueArgument("pipeline is not empty", !pipeline.isEmpty());
    }

    public List<BsonDocument> getPipeline() {
        return pipeline;
    }

    public ReadConcern getReadConcern() {
        return readConcern;
    }

    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    public Boolean getAllowDiskUse() {
        return allowDiskUse;
    }

    public AggregateToCollectionOperation allowDiskUse(@Nullable final Boolean allowDiskUse) {
        this.allowDiskUse = allowDiskUse;
        return this;
    }

    public Boolean getBypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    public AggregateToCollectionOperation bypassDocumentValidation(@Nullable final Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    public Collation getCollation() {
        return collation;
    }

    public AggregateToCollectionOperation collation(@Nullable final Collation collation) {
        this.collation = collation;
        return this;
    }

    public BsonValue getComment() {
        return comment;
    }

    public AggregateToCollectionOperation let(@Nullable final BsonDocument variables) {
        this.variables = variables;
        return this;
    }

    public AggregateToCollectionOperation comment(final BsonValue comment) {
        this.comment = comment;
        return this;
    }

    public BsonValue getHint() {
        return hint;
    }

    public AggregateToCollectionOperation hint(@Nullable final BsonValue hint) {
        this.hint = hint;
        return this;
    }

    public AggregateToCollectionOperation timeoutMode(@Nullable final TimeoutMode timeoutMode) {
        isTrueArgument("timeoutMode cannot be ITERATION.", timeoutMode == null || timeoutMode.equals(TimeoutMode.CURSOR_LIFETIME));
        return this;
    }

    @Override
    public String getCommandName() {
        return COMMAND_NAME;
    }

    @Override
    public MongoNamespace getNamespace() {
        return namespace;
    }

    @Override
    public Void execute(final ReadBinding binding, final OperationContext operationContext) {
        return executeRetryableRead(
                operationContext,
                (serverSelectionOperationContext) ->
                        binding.getReadConnectionSource(
                                FIVE_DOT_ZERO_WIRE_VERSION,
                                ReadPreference.primary(),
                                serverSelectionOperationContext),
                namespace.getDatabaseName(),
                getCommandCreator(),
                new BsonDocumentCodec(),
                transformer(),
                false);
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final OperationContext operationContext,
                             final SingleResultCallback<Void> callback) {
        executeRetryableReadAsync(
                binding,
                operationContext,
                (serverSelectionOperationContext, connectionSourceCallback) ->
                        binding.getReadConnectionSource(FIVE_DOT_ZERO_WIRE_VERSION, ReadPreference.primary(), serverSelectionOperationContext, connectionSourceCallback),
                namespace.getDatabaseName(),
                getCommandCreator(),
                new BsonDocumentCodec(),
                asyncTransformer(),
                false,
                callback);
    }

    private CommandOperationHelper.CommandCreator getCommandCreator() {
        return (operationContext, serverDescription, connectionDescription) -> {
            BsonValue aggregationTarget = (aggregationLevel == AggregationLevel.DATABASE)
                    ? new BsonInt32(1) : new BsonString(namespace.getCollectionName());

            BsonDocument commandDocument = new BsonDocument(getCommandName(), aggregationTarget);
            commandDocument.put("pipeline", new BsonArray(pipeline));
            if (allowDiskUse != null) {
                commandDocument.put("allowDiskUse", BsonBoolean.valueOf(allowDiskUse));
            }
            if (bypassDocumentValidation != null) {
                commandDocument.put("bypassDocumentValidation", BsonBoolean.valueOf(bypassDocumentValidation));
            }

            commandDocument.put("cursor", new BsonDocument());

            appendWriteConcernToCommand(writeConcern, commandDocument);
            if (readConcern != null && !readConcern.isServerDefault()) {
                commandDocument.put("readConcern", readConcern.asDocument());
            }

            if (collation != null) {
                commandDocument.put("collation", collation.asDocument());
            }
            if (comment != null) {
                commandDocument.put("comment", comment);
            }
            if (hint != null) {
                commandDocument.put("hint", hint);
            }
            if (variables != null) {
                commandDocument.put("let", variables);
            }
            return commandDocument;
        };
    }

    private static CommandReadTransformer<BsonDocument, Void> transformer() {
        return (result, source, connection, operationContext) -> {
            throwOnWriteConcernError(result, connection.getDescription().getServerAddress(),
                    connection.getDescription().getMaxWireVersion(), operationContext.getTimeoutContext());
            return null;
        };
    }

    private static CommandReadTransformerAsync<BsonDocument, Void> asyncTransformer() {
        return (result, source, connection, operationContext) -> {
            throwOnWriteConcernError(result, connection.getDescription().getServerAddress(),
                    connection.getDescription().getMaxWireVersion(), operationContext.getTimeoutContext());
            return null;
        };
    }
}
