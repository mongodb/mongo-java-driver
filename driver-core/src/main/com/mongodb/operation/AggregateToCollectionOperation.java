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

package com.mongodb.operation;

import com.mongodb.MongoNamespace;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncWriteBinding;
import com.mongodb.binding.WriteBinding;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.codecs.BsonDocumentCodec;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static com.mongodb.operation.OperationHelper.VoidTransformer;

/**
 * An operation that executes an aggregation that writes its results to a collection (which is what makes this a write operation rather than
 * a read operation).
 *
 * @mongodb.server.release 2.6
 * @since 3.0
 */
public class AggregateToCollectionOperation implements AsyncWriteOperation<Void>, WriteOperation<Void> {
    private final MongoNamespace namespace;
    private final List<BsonDocument> pipeline;
    private Boolean allowDiskUse;
    private long maxTimeMS;

    /**
     * Construct a new instance.
     *
     * @param namespace the database and collection namespace for the operation.
     * @param pipeline the aggregation pipeline.
     */
    public AggregateToCollectionOperation(final MongoNamespace namespace, final List<BsonDocument> pipeline) {
        this.namespace = notNull("namespace", namespace);
        this.pipeline = notNull("pipeline", pipeline);

        isTrueArgument("pipeline is empty", !pipeline.isEmpty());
        isTrueArgument("last stage of pipeline does not contain an output collection",
                       pipeline.get(pipeline.size() - 1).get("$out") != null);
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
     * Whether writing to temporary files is enabled. A null value indicates that it's unspecified.
     *
     * @return true if writing to temporary files is enabled
     * @mongodb.driver.manual reference/command/aggregate/ Aggregation
     * @mongodb.server.release 2.6
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
     * @mongodb.server.release 2.6
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

    @SuppressWarnings("unchecked")
    @Override
    public Void execute(final WriteBinding binding) {
        executeWrappedCommandProtocol(namespace.getDatabaseName(), getCommand(), new BsonDocumentCodec(), binding,
                                      new VoidTransformer<BsonDocument>());

        return null;
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<Void> callback) {
        executeWrappedCommandProtocolAsync(namespace.getDatabaseName(), getCommand(), new BsonDocumentCodec(), binding,
                                           new VoidTransformer<BsonDocument>(), callback);
    }

    private BsonDocument getCommand() {
        BsonDocument commandDocument = new BsonDocument("aggregate", new BsonString(namespace.getCollectionName()));
        commandDocument.put("pipeline", new BsonArray(pipeline));
        if (maxTimeMS > 0) {
            commandDocument.put("maxTimeMS", new BsonInt64(maxTimeMS));
        }
        if (allowDiskUse != null) {
            commandDocument.put("allowDiskUse", BsonBoolean.valueOf(allowDiskUse));
        }
        return commandDocument;
    }
}
