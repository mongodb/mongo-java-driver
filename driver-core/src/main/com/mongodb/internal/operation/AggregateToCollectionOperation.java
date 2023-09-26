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
import com.mongodb.client.model.Collation;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.TimeoutSettings;
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
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;

import java.util.List;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.operation.AsyncOperationHelper.executeRetryableReadAsync;
import static com.mongodb.internal.operation.ServerVersionHelper.FIVE_DOT_ZERO_WIRE_VERSION;
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
public class AggregateToCollectionOperation implements AsyncReadOperation<Void>, ReadOperation<Void> {
    private final TimeoutSettings timeoutSettings;
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

    public AggregateToCollectionOperation(final TimeoutSettings timeoutSettings, final MongoNamespace namespace,
            final List<BsonDocument> pipeline, final ReadConcern readConcern, final WriteConcern writeConcern) {
        this(timeoutSettings, namespace, pipeline, readConcern, writeConcern, AggregationLevel.COLLECTION);
    }

    public AggregateToCollectionOperation(final TimeoutSettings timeoutSettings, final MongoNamespace namespace,
            final List<BsonDocument> pipeline, @Nullable final ReadConcern readConcern, @Nullable final WriteConcern writeConcern,
            final AggregationLevel aggregationLevel) {
        this.timeoutSettings = timeoutSettings;
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

    @Override
    public TimeoutSettings getTimeoutSettings() {
        return timeoutSettings;
    }

    @Override
    public Void execute(final ReadBinding binding) {
        return executeRetryableRead(binding,
                                    () -> binding.getReadConnectionSource(FIVE_DOT_ZERO_WIRE_VERSION, ReadPreference.primary()),
                                    namespace.getDatabaseName(),
                                    getCommandCreator(),
                                    new BsonDocumentCodec(), (result, source, connection) -> {
                    throwOnWriteConcernError(result, connection.getDescription().getServerAddress(),
                            connection.getDescription().getMaxWireVersion());
                    return null;
                }, false);
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<Void> callback) {
        executeRetryableReadAsync(binding,
                                  (connectionSourceCallback) ->
                        binding.getReadConnectionSource(FIVE_DOT_ZERO_WIRE_VERSION, ReadPreference.primary(), connectionSourceCallback),
                                  namespace.getDatabaseName(),
                                  getCommandCreator(),
                                  new BsonDocumentCodec(), (result, source, connection) -> {
                    throwOnWriteConcernError(result, connection.getDescription().getServerAddress(),
                            connection.getDescription().getMaxWireVersion());
                    return null;
                }, false, callback);
    }

    private CommandOperationHelper.CommandCreator getCommandCreator() {
        return (operationContext, serverDescription, connectionDescription) -> {
            BsonValue aggregationTarget = (aggregationLevel == AggregationLevel.DATABASE)
                    ? new BsonInt32(1) : new BsonString(namespace.getCollectionName());

            BsonDocument commandDocument = new BsonDocument("aggregate", aggregationTarget);
            commandDocument.put("pipeline", new BsonArray(pipeline));
            long maxTimeMS = operationContext.getTimeoutContext().getMaxTimeMS();
            if (maxTimeMS > 0) {
                commandDocument.put("maxTimeMS", new BsonInt64(maxTimeMS));
            }
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
}
