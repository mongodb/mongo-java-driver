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
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.client.model.AggregationLevel;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.operation.CommandOperationHelper.executeRetryableRead;
import static com.mongodb.internal.operation.CommandOperationHelper.executeRetryableReadAsync;
import static com.mongodb.internal.operation.ServerVersionHelper.FIVE_DOT_ZERO_WIRE_VERSION;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsAtLeastVersionThreeDotSix;
import static com.mongodb.internal.operation.WriteConcernHelper.appendWriteConcernToCommand;
import static com.mongodb.internal.operation.WriteConcernHelper.throwOnWriteConcernError;

/**
 * An operation that executes an aggregation that writes its results to a collection.
 *
 * <p>Drivers are required to execute this operation on a secondary as of MongoDB 5.0, and otherwise execute it on a primary. That's why
 * this is a ReadOperation, not a WriteOperation: because it now uses the read preference to select the server.
 * </p>
 *
 * @see ReadBinding#getReadConnectionSource(int, com.mongodb.ReadPreference)
 * @mongodb.driver.manual reference/command/aggregate/ Aggregation
 * @since 3.0
 */
public class AggregateToCollectionOperation implements AsyncReadOperation<Void>, ReadOperation<Void> {
    private final MongoNamespace namespace;
    private final List<BsonDocument> pipeline;
    private final WriteConcern writeConcern;
    private final ReadConcern readConcern;
    private final AggregationLevel aggregationLevel;

    private Boolean allowDiskUse;
    private long maxTimeMS;
    private Boolean bypassDocumentValidation;
    private Collation collation;
    private BsonValue comment;
    private BsonValue hint;
    private BsonDocument variables;

    /**
     * Construct a new instance.
     *
     * @param namespace the database and collection namespace for the operation.
     * @param pipeline the aggregation pipeline.
     */
    public AggregateToCollectionOperation(final MongoNamespace namespace, final List<BsonDocument> pipeline) {
        this(namespace, pipeline, null, null, AggregationLevel.COLLECTION);
    }

    /**
     * Construct a new instance.
     *
     * @param namespace the database and collection namespace for the operation.
     * @param pipeline the aggregation pipeline.
     * @param writeConcern the write concern to apply
     *
     * @since 3.4
     */
    public AggregateToCollectionOperation(final MongoNamespace namespace, final List<BsonDocument> pipeline,
                                          final WriteConcern writeConcern) {
        this(namespace, pipeline, null, writeConcern, AggregationLevel.COLLECTION);
    }

    /**
     * Construct a new instance.
     *
     * @param namespace the database and collection namespace for the operation.
     * @param pipeline the aggregation pipeline.
     * @param readConcern the read concern to apply
     *
     * @since 3.11
     */
    public AggregateToCollectionOperation(final MongoNamespace namespace, final List<BsonDocument> pipeline,
                                          final ReadConcern readConcern) {
        this(namespace, pipeline, readConcern, null, AggregationLevel.COLLECTION);
    }

    /**
     * Construct a new instance.
     *
     * @param namespace the database and collection namespace for the operation.
     * @param pipeline the aggregation pipeline.
     * @param writeConcern the write concern to apply
     * @param readConcern the read concern to apply
     * @since 3.11
     */
    public AggregateToCollectionOperation(final MongoNamespace namespace, final List<BsonDocument> pipeline,
                                          final ReadConcern readConcern, final WriteConcern writeConcern) {
        this(namespace, pipeline, readConcern, writeConcern, AggregationLevel.COLLECTION);
    }

    /**
     * Construct a new instance.
     *
     * @param namespace the database and collection namespace for the operation.
     * @param pipeline the aggregation pipeline.
     * @param readConcern the read concern to apply
     * @param writeConcern the write concern to apply
     * @param aggregationLevel the aggregation level
     * @since 3.11
     */
    public AggregateToCollectionOperation(final MongoNamespace namespace, final List<BsonDocument> pipeline,
                                          final ReadConcern readConcern, final WriteConcern writeConcern,
                                          final AggregationLevel aggregationLevel) {
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
     * Gets the maximum execution time on the server for this operation.  The default is 0, which places no limit on the execution time.
     *
     * @param timeUnit the time unit to return the result in
     * @return the maximum execution time in the given time unit
     * @mongodb.driver.manual reference/method/cursor.maxTimeMS/#cursor.maxTimeMS Max Time
     */
    public long getMaxTime(final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        return timeUnit.convert(maxTimeMS, TimeUnit.MILLISECONDS);
    }

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime  the max time
     * @param timeUnit the time unit, which may not be null
     * @return this
     * @mongodb.driver.manual reference/method/cursor.maxTimeMS/#cursor.maxTimeMS Max Time
     */
    public AggregateToCollectionOperation maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
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
     * @since 4.6
     * @mongodb.server.release 3.6
     */
    public BsonValue getComment() {
        return comment;
    }

    public AggregateToCollectionOperation let(final BsonDocument variables) {
        this.variables = variables;
        return this;
    }

    /**
     * Sets the comment for this operation. A null value means no comment is set.
     *
     * @param comment the comment
     * @return this
     * @since 4.6
     * @mongodb.server.release 3.6
     */
    public AggregateToCollectionOperation comment(final BsonValue comment) {
        this.comment = comment;
        return this;
    }

    /**
     * Returns the hint for which index to use. The default is not to set a hint.
     *
     * @return the hint
     */
    public BsonValue getHint() {
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
    public AggregateToCollectionOperation hint(final BsonValue hint) {
        this.hint = hint;
        return this;
    }

    @Override
    public Void execute(final ReadBinding binding) {
        return executeRetryableRead(binding,
                () -> binding.getReadConnectionSource(FIVE_DOT_ZERO_WIRE_VERSION, ReadPreference.primary()),
                namespace.getDatabaseName(),
                (serverDescription, connectionDescription) -> getCommand(connectionDescription),
                new BsonDocumentCodec(), (result, source, connection) -> {
                    throwOnWriteConcernError(result, connection.getDescription().getServerAddress(),
                            connection.getDescription().getMaxWireVersion());
                    return null;
                }, false);
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<Void> callback) {
        executeRetryableReadAsync(binding,
                (connectionSourceCallback) -> {
                        binding.getReadConnectionSource(FIVE_DOT_ZERO_WIRE_VERSION, ReadPreference.primary(), connectionSourceCallback);
                },
                namespace.getDatabaseName(),
                (serverDescription, connectionDescription) -> getCommand(connectionDescription),
                new BsonDocumentCodec(), (result, source, connection) -> {
                    throwOnWriteConcernError(result, connection.getDescription().getServerAddress(),
                            connection.getDescription().getMaxWireVersion());
                    return null;
                }, false, callback);
    }

    private BsonDocument getCommand(final ConnectionDescription description) {
        BsonValue aggregationTarget = (aggregationLevel == AggregationLevel.DATABASE)
                ? new BsonInt32(1) : new BsonString(namespace.getCollectionName());

        BsonDocument commandDocument = new BsonDocument("aggregate", aggregationTarget);
        commandDocument.put("pipeline", new BsonArray(pipeline));
        if (maxTimeMS > 0) {
            commandDocument.put("maxTimeMS", new BsonInt64(maxTimeMS));
        }
        if (allowDiskUse != null) {
            commandDocument.put("allowDiskUse", BsonBoolean.valueOf(allowDiskUse));
        }
        if (bypassDocumentValidation != null) {
            commandDocument.put("bypassDocumentValidation", BsonBoolean.valueOf(bypassDocumentValidation));
        }

        if (serverIsAtLeastVersionThreeDotSix(description)) {
            commandDocument.put("cursor", new BsonDocument());
        }

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
    }
}
