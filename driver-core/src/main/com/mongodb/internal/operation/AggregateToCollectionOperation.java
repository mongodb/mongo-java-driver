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
import com.mongodb.WriteConcern;
import com.mongodb.client.model.Collation;
import com.mongodb.internal.ClientSideOperationTimeout;
import com.mongodb.internal.ClientSideOperationTimeoutFactory;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncConnectionSource;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.internal.client.model.AggregationLevel;
import com.mongodb.internal.connection.AsyncConnection;
import com.mongodb.internal.connection.Connection;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;

import java.util.List;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.AsyncCommandOperationHelper.executeCommandAsync;
import static com.mongodb.internal.operation.AsyncCommandOperationHelper.writeConcernErrorTransformerAsync;
import static com.mongodb.internal.operation.AsyncOperationHelper.AsyncCallableWithConnectionAndSource;
import static com.mongodb.internal.operation.AsyncOperationHelper.releasingCallback;
import static com.mongodb.internal.operation.AsyncOperationHelper.withAsyncConnection;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsAtLeastVersionThreeDotFour;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsAtLeastVersionThreeDotSix;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsAtLeastVersionThreeDotTwo;
import static com.mongodb.internal.operation.SyncCommandOperationHelper.executeCommand;
import static com.mongodb.internal.operation.SyncCommandOperationHelper.writeConcernErrorTransformer;
import static com.mongodb.internal.operation.SyncOperationHelper.CallableWithConnectionAndSource;
import static com.mongodb.internal.operation.SyncOperationHelper.withConnection;
import static com.mongodb.internal.operation.WriteConcernHelper.appendWriteConcernToCommand;

/**
 * An operation that executes an aggregation that writes its results to a collection (which is what makes this a write operation rather than
 * a read operation).
 *
 * @mongodb.driver.manual reference/command/aggregate/ Aggregation
 * @since 3.0
 */
public class AggregateToCollectionOperation implements AsyncWriteOperation<Void>, WriteOperation<Void> {
    private final ClientSideOperationTimeoutFactory clientSideOperationTimeoutFactory;
    private final MongoNamespace namespace;
    private final List<BsonDocument> pipeline;
    private final WriteConcern writeConcern;
    private final ReadConcern readConcern;
    private final AggregationLevel aggregationLevel;

    private Boolean allowDiskUse;
    private Boolean bypassDocumentValidation;
    private Collation collation;
    private String comment;
    private BsonDocument hint;

    /**
     * Construct a new instance.
     *
     * @param clientSideOperationTimeoutFactory the client side operation timeout factory
     * @param namespace the database and collection namespace for the operation.
     * @param pipeline the aggregation pipeline.
     */
    public AggregateToCollectionOperation(final ClientSideOperationTimeoutFactory  clientSideOperationTimeoutFactory,
                                          final MongoNamespace namespace, final List<BsonDocument> pipeline) {
        this(clientSideOperationTimeoutFactory, namespace, pipeline, null, null, AggregationLevel.COLLECTION);
    }

    /**
     * Construct a new instance.
     *
     * @param clientSideOperationTimeoutFactory the client side operation timeout factory
     * @param namespace the database and collection namespace for the operation.
     * @param pipeline the aggregation pipeline.
     * @param writeConcern the write concern to apply
     *
     * @since 3.4
     */
    public AggregateToCollectionOperation(final ClientSideOperationTimeoutFactory clientSideOperationTimeoutFactory,
                                          final MongoNamespace namespace, final List<BsonDocument> pipeline,
                                          final WriteConcern writeConcern) {
        this(clientSideOperationTimeoutFactory, namespace, pipeline, null, writeConcern, AggregationLevel.COLLECTION);
    }

    /**
     * Construct a new instance.
     *
     * @param clientSideOperationTimeoutFactory the client side operation timeout factory
     * @param namespace the database and collection namespace for the operation.
     * @param pipeline the aggregation pipeline.
     * @param readConcern the read concern to apply
     *
     * @since 3.11
     */
    public AggregateToCollectionOperation(final ClientSideOperationTimeoutFactory clientSideOperationTimeoutFactory,
                                          final MongoNamespace namespace, final List<BsonDocument> pipeline,
                                          final ReadConcern readConcern) {
        this(clientSideOperationTimeoutFactory, namespace, pipeline, readConcern, null, AggregationLevel.COLLECTION);
    }

    /**
     * Construct a new instance.
     *
     * @param clientSideOperationTimeoutFactory the client side operation timeout factory
     * @param namespace the database and collection namespace for the operation.
     * @param pipeline the aggregation pipeline.
     * @param readConcern the read concern to apply
     * @param writeConcern the write concern to apply
     * @since 3.11
     */
    public AggregateToCollectionOperation(final ClientSideOperationTimeoutFactory clientSideOperationTimeoutFactory,
                                          final MongoNamespace namespace, final List<BsonDocument> pipeline,
                                          final ReadConcern readConcern, final WriteConcern writeConcern) {
        this(clientSideOperationTimeoutFactory, namespace, pipeline, readConcern, writeConcern, AggregationLevel.COLLECTION);
    }

    /**
     * Construct a new instance.
     *
     * @param clientSideOperationTimeoutFactory the client side operation timeout factory
     * @param namespace the database and collection namespace for the operation.
     * @param pipeline the aggregation pipeline.
     * @param readConcern the read concern to apply
     * @param writeConcern the write concern to apply
     * @param aggregationLevel the aggregation level
     * @since 3.11
     */
    public AggregateToCollectionOperation(final ClientSideOperationTimeoutFactory clientSideOperationTimeoutFactory,
                                          final MongoNamespace namespace, final List<BsonDocument> pipeline,
                                          final ReadConcern readConcern, final WriteConcern writeConcern,
                                          final AggregationLevel aggregationLevel) {
        this.clientSideOperationTimeoutFactory = notNull("clientSideOperationTimeoutFactory", clientSideOperationTimeoutFactory);
        this.namespace = notNull("namespace", namespace);
        this.pipeline = notNull("pipeline", pipeline);
        this.writeConcern = writeConcern;
        this.readConcern = readConcern;
        this.aggregationLevel = notNull("aggregationLevel", aggregationLevel);

        isTrueArgument("pipeline is not empty", !pipeline.isEmpty());
    }

    /**
     * Gets the aggregation pipeline.
     *
     * @return the pipeline
     * @mongodb.driver.manual core/aggregation-introduction/#aggregation-pipelines Aggregation Pipeline
     */
    public List<BsonDocument> getPipeline() {
        return pipeline;
    }

    /**
     * Gets the read concern.
     *
     * @return the read concern, which may be null
     *
     * @since 3.11
     */
    public ReadConcern getReadConcern() {
        return readConcern;
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
     * Whether writing to temporary files is enabled. A null value indicates that it's unspecified.
     *
     * @return true if writing to temporary files is enabled
     * @mongodb.driver.manual reference/command/aggregate/ Aggregation
     */
    public Boolean getAllowDiskUse() {
        return allowDiskUse;
    }

    /**
     * Enables writing to temporary files. A null value indicates that it's unspecified.
     *
     * @param allowDiskUse true if writing to temporary files is enabled
     * @return this
     * @mongodb.driver.manual reference/command/aggregate/ Aggregation
     */
    public AggregateToCollectionOperation allowDiskUse(final Boolean allowDiskUse) {
        this.allowDiskUse = allowDiskUse;
        return this;
    }

    /**
     * Gets the bypass document level validation flag
     *
     * @return the bypass document level validation flag
     * @since 3.2
     */
    public Boolean getBypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    /**
     * Sets the bypass document level validation flag.
     *
     * <p>Note: This only applies when an $out or $merge stage is specified</p>.
     *
     * @param bypassDocumentValidation If true, allows the write to opt-out of document level validation.
     * @return this
     * @since 3.2
     * @mongodb.server.release 3.2
     */
    public AggregateToCollectionOperation bypassDocumentValidation(final Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
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
    public AggregateToCollectionOperation collation(final Collation collation) {
        this.collation = collation;
        return this;
    }

    /**
     * Returns the comment to send with the aggregate. The default is not to include a comment with the aggregation.
     *
     * @return the comment
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    public String getComment() {
        return comment;
    }

    /**
     * Sets the comment to the aggregation. A null value means no comment is set.
     *
     * @param comment the comment
     * @return this
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    public AggregateToCollectionOperation comment(final String comment) {
        this.comment = comment;
        return this;
    }

    /**
     * Returns the hint for which index to use. The default is not to set a hint.
     *
     * @return the hint
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    public BsonDocument getHint() {
        return hint;
    }

    /**
     * Sets the hint for which index to use. A null value means no hint is set.
     *
     * @param hint the hint
     * @return this
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    public AggregateToCollectionOperation hint(final BsonDocument hint) {
        this.hint = hint;
        return this;
    }

    @Override
    public Void execute(final WriteBinding binding) {
        return withConnection(clientSideOperationTimeoutFactory.create(), binding, new CallableWithConnectionAndSource<Void>() {
            @Override
            public Void call(final ClientSideOperationTimeout clientSideOperationTimeout,
                             final ConnectionSource source, final Connection connection) {
                SyncOperationHelper.validateCollation(connection, collation);
                return executeCommand(clientSideOperationTimeout, binding, namespace.getDatabaseName(),
                        getCommandCreator().create(clientSideOperationTimeout, source.getServerDescription(), connection.getDescription()),
                        connection, writeConcernErrorTransformer());
            }
        });
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<Void> callback) {
        withAsyncConnection(clientSideOperationTimeoutFactory.create(), binding, new AsyncCallableWithConnectionAndSource() {
            @Override
            public void call(final ClientSideOperationTimeout clientSideOperationTimeout, final AsyncConnectionSource source,
                             final AsyncConnection connection, final Throwable t) {
                SingleResultCallback<Void> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
                if (t != null) {
                    errHandlingCallback.onResult(null, t);
                } else {
                    final SingleResultCallback<Void> wrappedCallback = releasingCallback(errHandlingCallback, source, connection);
                    AsyncOperationHelper.validateCollation(clientSideOperationTimeout, source, connection, collation,
                            new AsyncCallableWithConnectionAndSource() {
                                @Override
                                public void call(final ClientSideOperationTimeout clientSideOperationTimeout,
                                                 final AsyncConnectionSource source, final AsyncConnection connection, final Throwable t) {
                            if (t != null) {
                                wrappedCallback.onResult(null, t);
                            } else {
                                executeCommandAsync(clientSideOperationTimeout, binding, namespace.getDatabaseName(),
                                        getCommandCreator().create(clientSideOperationTimeout, source.getServerDescription(),
                                                connection.getDescription()), connection,
                                        writeConcernErrorTransformerAsync(),
                                        wrappedCallback);
                            }
                        }
                    });
                }
            }
        });
    }

    private CommandCreator getCommandCreator() {
        return (clientSideOperationTimeout, serverDescription, connectionDescription) -> {
            BsonValue aggregationTarget = (aggregationLevel == AggregationLevel.DATABASE)
                    ? new BsonInt32(1) : new BsonString(namespace.getCollectionName());

            BsonDocument commandDocument = new BsonDocument("aggregate", aggregationTarget);
            commandDocument.put("pipeline", new BsonArray(pipeline));

            long maxTimeMS = clientSideOperationTimeout.getMaxTimeMS();
            if (maxTimeMS > 0) {
                commandDocument.put("maxTimeMS", new BsonInt64(maxTimeMS));
            }
            if (allowDiskUse != null) {
                commandDocument.put("allowDiskUse", BsonBoolean.valueOf(allowDiskUse));
            }
            if (bypassDocumentValidation != null && serverIsAtLeastVersionThreeDotTwo(connectionDescription)) {
                commandDocument.put("bypassDocumentValidation", BsonBoolean.valueOf(bypassDocumentValidation));
            }

            if (serverIsAtLeastVersionThreeDotSix(connectionDescription)) {
                commandDocument.put("cursor", new BsonDocument());
            }

            appendWriteConcernToCommand(writeConcern, commandDocument, connectionDescription);
            if (readConcern != null && !readConcern.isServerDefault() && serverIsAtLeastVersionThreeDotFour(connectionDescription)) {
                commandDocument.put("readConcern", readConcern.asDocument());
            }

            if (collation != null) {
                commandDocument.put("collation", collation.asDocument());
            }
            if (comment != null) {
                commandDocument.put("comment", new BsonString(comment));
            }
            if (hint != null) {
                commandDocument.put("hint", hint);
            }
            return commandDocument;
        };
    }

}
