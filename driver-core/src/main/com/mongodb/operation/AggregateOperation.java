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
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.codecs.Decoder;
import org.mongodb.CommandResult;

import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.operation.AggregateHelper.asCommandDocument;
import static com.mongodb.operation.AggregationOptions.OutputMode.INLINE;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static com.mongodb.operation.OperationHelper.AsyncCallableWithConnectionAndSource;
import static com.mongodb.operation.OperationHelper.CallableWithConnectionAndSource;
import static com.mongodb.operation.OperationHelper.withConnection;

/**
 * An operation that executes an aggregation query
 *
 * @param <T> the type to deserialize the results to
 * @since 3.0
 */
public class AggregateOperation<T> implements AsyncReadOperation<MongoAsyncCursor<T>>, ReadOperation<MongoCursor<T>> {
    private static final String RESULT = "result";
    private static final String FIRST_BATCH = "firstBatch";

    private final MongoNamespace namespace;
    private final List<BsonDocument> pipeline;
    private final Decoder<T> decoder;
    private final AggregationOptions options;

    /**
     * Construct a new instance.
     *
     * @param namespace the namespace of the collection to aggregate
     * @param pipeline the pipeline of operators
     * @param decoder the decoder to decode the result documents
     * @param options the options
     */
    public AggregateOperation(final MongoNamespace namespace, final List<BsonDocument> pipeline,
                              final Decoder<T> decoder, final AggregationOptions options) {
        this.namespace = notNull("namespace", namespace);
        this.pipeline = notNull("pipeline", pipeline);
        this.decoder = notNull("decoder", decoder);
        this.options = notNull("options", options);
    }

    @Override
    public MongoCursor<T> execute(final ReadBinding binding) {
        return withConnection(binding, new CallableWithConnectionAndSource<MongoCursor<T>>() {
            @Override
            public MongoCursor<T> call(final ConnectionSource source, final Connection connection) {
                return executeWrappedCommandProtocol(namespace, asCommandDocument(namespace, pipeline, options),
                                                     CommandResultDocumentCodec.create(decoder, getFieldNameWithResults()),
                                                     connection, binding.getReadPreference(), transformer(source));
            }
        });
    }

    @Override
    public MongoFuture<MongoAsyncCursor<T>> executeAsync(final AsyncReadBinding binding) {
        return withConnection(binding, new AsyncCallableWithConnectionAndSource<MongoAsyncCursor<T>>() {

            @Override
            public MongoFuture<MongoAsyncCursor<T>> call(final AsyncConnectionSource source, final Connection connection) {
                return executeWrappedCommandProtocolAsync(namespace, asCommandDocument(namespace, pipeline, options),
                                                          CommandResultDocumentCodec.create(decoder, getFieldNameWithResults()),
                                                          binding, asyncTransformer(source));
            }
        });
    }

    private boolean isInline() {
        return options.getOutputMode() == INLINE;
    }

    private String getFieldNameWithResults() {
        return options.getOutputMode() == INLINE ? RESULT : FIRST_BATCH;
    }

    @SuppressWarnings("unchecked")
    private QueryResult<T> createQueryResult(final CommandResult result) {
        long cursorId;
        BsonArray results;
        if (isInline()) {
            cursorId = 0;
            results = result.getResponse().getArray(RESULT);
        } else {
            BsonDocument cursor = result.getResponse().getDocument("cursor");
            cursorId = ((BsonInt64) cursor.get("id")).getValue();
            results = cursor.getArray(FIRST_BATCH);
        }
        return new QueryResult<T>(BsonDocumentWrapperHelper.<T>toList(results), cursorId, result.getAddress(), 0);
    }

    private Function<CommandResult, MongoCursor<T>> transformer(final ConnectionSource source) {
        return new Function<CommandResult, MongoCursor<T>>() {
            @Override
            public MongoCursor<T> apply(final CommandResult result) {
                QueryResult<T> queryResult = createQueryResult(result);
                if (isInline()) {
                    return new InlineMongoCursor<T>(result.getAddress(), queryResult.getResults());
                } else {
                    int batchSize = options.getBatchSize() == null ? 0 : options.getBatchSize();
                    return new MongoQueryCursor<T>(namespace, queryResult, 0, batchSize, decoder, source);
                }
            }
        };
    }

    private Function<CommandResult, MongoAsyncCursor<T>> asyncTransformer(final AsyncConnectionSource source) {
        return new Function<CommandResult, MongoAsyncCursor<T>>() {

            @Override
            public MongoAsyncCursor<T> apply(final CommandResult result) {
                QueryResult<T> queryResult = createQueryResult(result);
                if (isInline()) {
                    return new InlineMongoAsyncCursor<T>(queryResult.getResults());
                } else {
                    int batchSize = options.getBatchSize() == null ? 0 : options.getBatchSize();
                    return new MongoAsyncQueryCursor<T>(namespace, queryResult, 0, batchSize, decoder, source);
                }
            }
        };
    }
}
