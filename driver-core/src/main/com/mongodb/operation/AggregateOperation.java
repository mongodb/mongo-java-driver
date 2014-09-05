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

import com.mongodb.Function;
import com.mongodb.MongoCursor;
import com.mongodb.MongoNamespace;
import com.mongodb.async.MongoAsyncCursor;
import com.mongodb.async.MongoFuture;
import com.mongodb.binding.AsyncConnectionSource;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.binding.ReadBinding;
import com.mongodb.connection.Connection;
import com.mongodb.protocol.QueryResult;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.codecs.Decoder;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static com.mongodb.operation.OperationHelper.AsyncCallableWithConnectionAndSource;
import static com.mongodb.operation.OperationHelper.CallableWithConnectionAndSource;
import static com.mongodb.operation.OperationHelper.serverIsAtLeastVersionTwoDotSix;
import static com.mongodb.operation.OperationHelper.withConnection;

/**
 * An operation that executes an aggregation query.
 *
 * @param <T> the type to deserialize the results to
 * @since 3.0
 * @mongodb.driver.manual manual/aggregation/ Aggregation
 * @mongodb.server.release 2.2
 */
public class AggregateOperation<T> implements AsyncReadOperation<MongoAsyncCursor<T>>, ReadOperation<MongoCursor<T>> {
    private static final String RESULT = "result";
    private static final String FIRST_BATCH = "firstBatch";

    private final MongoNamespace namespace;
    private final List<BsonDocument> pipeline;
    private final Decoder<T> decoder;
    private Boolean allowDiskUse;
    private Integer batchSize;
    private long maxTimeMS;
    private Boolean useCursor;

    /**
     * Construct a new instance.
     *
     * @param namespace the namespace of the collection to aggregate
     * @param pipeline the pipeline of operators
     * @param decoder the decoder to decode the result documents
     */
    public AggregateOperation(final MongoNamespace namespace, final List<BsonDocument> pipeline, final Decoder<T> decoder) {
        this.namespace = notNull("namespace", namespace);
        this.pipeline = notNull("pipeline", pipeline);
        this.decoder = notNull("decoder", decoder);
    }

    /**
     * Gets the aggregation pipeline.
     *
     * @return the pipeline
     * @mongodb.driver.manual manual/core/aggregation-introduction/#aggregation-pipelines Aggregation Pipeline
     */
    public List<BsonDocument> getPipeline() {
        return pipeline;
    }

    /**
     * Whether writing to temporary files is enabled. A null value indicates that it's unspecified.
     *
     * @return true if writing to temporary files is enabled
     * @mongodb.driver.manual manual/reference/command/aggregate/ Aggregation
     * @mongodb.server.release 2.6
     */
    public Boolean getAllowDiskUse() {
        return allowDiskUse;
    }

    /**
     * Enables writing to temporary files. A null value indicates that it's unspecified.
     *
     * @param allowDiskUse true if writing to temporary files is enabled
     * @mongodb.driver.manual manual/reference/command/aggregate/ Aggregation
     * @mongodb.server.release 2.6
     */
    public void setAllowDiskUse(final Boolean allowDiskUse) {
        this.allowDiskUse = allowDiskUse;
    }

    /**
     * Gets the number of documents to return per batch.  Default to 0, which indicates that the server chooses an appropriate batch
     * size.
     *
     * @return the batch size, which may be null
     * @mongodb.driver.manual manual/reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    public Integer getBatchSize() {
        return batchSize;
    }

    /**
     * Sets the number of documents to return per batch.
     *
     * @param batchSize the batch size
     * @mongodb.driver.manual manual/reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    public void setBatchSize(final Integer batchSize) {
        this.batchSize = batchSize;
    }

    /**
     * Gets the maximum execution time on the server for this operation.  The default is 0, which places no limit on the execution time.
     *
     * @param timeUnit the time unit to return the result in
     * @return the maximum execution time in the given time unit
     * @mongodb.driver.manual manual/reference/method/cursor.maxTimeMS/#cursor.maxTimeMS Max Time
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
     * @mongodb.driver.manual manual/reference/method/cursor.maxTimeMS/#cursor.maxTimeMS Max Time
     */
    public void setMaxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
    }

    /**
     * Gets whether the server should use a cursor to return results.  The default value is null, in which case
     * a cursor will be used if the server supports it.
     *
     * @return whether the server should use a cursor to return results
     * @mongodb.driver.manual manual/reference/command/aggregate/ Aggregation
     * @mongodb.server.release 2.6
     */
    public Boolean getUseCursor() {
        return useCursor;
    }

    /**
     * Sets whether the server should use a cursor to return results.
     *
     * @param useCursor whether the server should use a cursor to return results
     * @mongodb.driver.manual manual/reference/command/aggregate/ Aggregation
     * @mongodb.server.release 2.6
     */
    public void setUseCursor(final Boolean useCursor) {
        this.useCursor = useCursor;
    }

    @Override
    public MongoCursor<T> execute(final ReadBinding binding) {
        return withConnection(binding, new CallableWithConnectionAndSource<MongoCursor<T>>() {
            @Override
            public MongoCursor<T> call(final ConnectionSource source, final Connection connection) {
                return executeWrappedCommandProtocol(namespace.getDatabaseName(), asCommandDocument(connection),
                                                     CommandResultDocumentCodec.create(decoder,
                                                                                       getFieldNameWithResults(connection)),
                                                     connection, binding.getReadPreference(), transformer(source, connection));
            }
        });
    }

    @Override
    public MongoFuture<MongoAsyncCursor<T>> executeAsync(final AsyncReadBinding binding) {
        return withConnection(binding, new AsyncCallableWithConnectionAndSource<MongoAsyncCursor<T>>() {

            @Override
            public MongoFuture<MongoAsyncCursor<T>> call(final AsyncConnectionSource source, final Connection connection) {
                return executeWrappedCommandProtocolAsync(namespace.getDatabaseName(), asCommandDocument(connection),
                                                          CommandResultDocumentCodec.create(decoder,
                                                                                            getFieldNameWithResults(connection)),
                                                          binding, asyncTransformer(source, connection));
            }
        });
    }

    private boolean isInline(final Connection connection) {
        return (useCursor != null && !useCursor) || !serverIsAtLeastVersionTwoDotSix(connection);
    }

    private BsonDocument asCommandDocument(final Connection connection) {
        BsonDocument commandDocument = new BsonDocument("aggregate", new BsonString(namespace.getCollectionName()));
        commandDocument.put("pipeline", new BsonArray(pipeline));
        if (maxTimeMS > 0) {
            commandDocument.put("maxTimeMS", new BsonInt64(maxTimeMS));
        }
        if ((useCursor == null || useCursor) && serverIsAtLeastVersionTwoDotSix(connection)) {
            BsonDocument cursor = new BsonDocument();
            if (batchSize != null) {
                cursor.put("batchSize", new BsonInt32(batchSize));
            }
            commandDocument.put("cursor", cursor);
        }
        if (allowDiskUse != null) {
            commandDocument.put("allowDiskUse", BsonBoolean.valueOf(allowDiskUse));
        }
        return commandDocument;
    }

    String getFieldNameWithResults(final Connection connection) {
        return ((useCursor == null || useCursor) && serverIsAtLeastVersionTwoDotSix(connection)) ? FIRST_BATCH : RESULT;
    }

    @SuppressWarnings("unchecked")
    private QueryResult<T> createQueryResult(final BsonDocument result, final Connection connection) {
        long cursorId;
        BsonArray results;
        if (isInline(connection)) {
            cursorId = 0;
            results = result.getArray(RESULT);
        } else {
            BsonDocument cursor = result.getDocument("cursor");
            cursorId = ((BsonInt64) cursor.get("id")).getValue();
            results = cursor.getArray(FIRST_BATCH);
        }
        return new QueryResult<T>(BsonDocumentWrapperHelper.<T>toList(results), cursorId, connection.getServerAddress(), 0);
    }

    private Function<BsonDocument, MongoCursor<T>> transformer(final ConnectionSource source, final Connection connection) {
        return new Function<BsonDocument, MongoCursor<T>>() {
            @Override
            public MongoCursor<T> apply(final BsonDocument result) {
                QueryResult<T> queryResult = createQueryResult(result, connection);
                if (isInline(connection)) {
                    return new InlineMongoCursor<T>(source.getServerDescription().getAddress(), queryResult.getResults());
                } else {
                    return new MongoQueryCursor<T>(namespace, queryResult, 0, batchSize != null ? batchSize : 0, decoder, source);
                }
            }
        };
    }

    private Function<BsonDocument, MongoAsyncCursor<T>> asyncTransformer(final AsyncConnectionSource source, final Connection connection) {
        return new Function<BsonDocument, MongoAsyncCursor<T>>() {

            @Override
            public MongoAsyncCursor<T> apply(final BsonDocument result) {
                QueryResult<T> queryResult = createQueryResult(result, connection);
                if (isInline(connection)) {
                    return new InlineMongoAsyncCursor<T>(queryResult.getResults());
                } else {
                    return new MongoAsyncQueryCursor<T>(namespace, queryResult, 0, batchSize != null ? batchSize : 0, decoder, source);
                }
            }
        };
    }
}
