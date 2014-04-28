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

package org.mongodb.operation;

import org.mongodb.AggregationOptions;
import org.mongodb.CommandResult;
import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.Function;
import org.mongodb.MongoAsyncCursor;
import org.mongodb.MongoCursor;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.binding.AsyncConnectionSource;
import org.mongodb.binding.AsyncReadBinding;
import org.mongodb.binding.ConnectionSource;
import org.mongodb.binding.ReadBinding;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.Connection;
import org.mongodb.protocol.QueryResult;

import java.util.List;

import static org.mongodb.AggregationOptions.OutputMode.INLINE;
import static org.mongodb.operation.AggregateHelper.asCommandDocument;
import static org.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static org.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static org.mongodb.operation.OperationHelper.AsyncCallableWithConnectionAndSource;
import static org.mongodb.operation.OperationHelper.CallableWithConnectionAndSource;
import static org.mongodb.operation.OperationHelper.withConnection;

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
    private final List<Document> pipeline;
    private final Encoder<Document> encoder;
    private final Decoder<T> decoder;
    private final AggregationOptions options;

    public AggregateOperation(final MongoNamespace namespace, final List<Document> pipeline, final Encoder<Document> encoder,
                              final Decoder<T> decoder, final AggregationOptions options) {
        this.namespace = namespace;
        this.pipeline = pipeline;
        this.encoder = encoder;
        this.decoder = decoder;
        this.options = options;
    }

    @Override
    public MongoCursor<T> execute(final ReadBinding binding) {
        return withConnection(binding, new CallableWithConnectionAndSource<MongoCursor<T>>() {
            @Override
            public MongoCursor<T> call(final ConnectionSource source, final Connection connection) {
                return executeWrappedCommandProtocol(namespace, asCommandDocument(namespace, pipeline, options), encoder,
                                                     new CommandResultWithPayloadDecoder<T>(decoder, getFieldNameWithResults()),
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
                                                          new DocumentCodec(), new CommandResultWithPayloadDecoder<T>(decoder, "result"),
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
        List<T> results;
        if (isInline()) {
            cursorId = 0;
            results = (List<T>) result.getResponse().get(RESULT);
        } else {
            Document cursor = (Document) result.getResponse().get("cursor");
            cursorId = cursor.getLong("id");
            results = (List<T>) cursor.get(FIRST_BATCH);
        }
        return new QueryResult<T>(results, cursorId, result.getAddress(), 0);
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
