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
import com.mongodb.MongoNamespace;
import com.mongodb.async.MapReduceAsyncCursor;
import com.mongodb.async.MongoFuture;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.ReadBinding;
import com.mongodb.connection.Connection;
import org.bson.BsonDocument;
import org.bson.codecs.Decoder;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.operation.CommandDocuments.createMapReduce;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static com.mongodb.operation.OperationHelper.withConnection;

/**
 * <p>Operation that runs a Map Reduce against a MongoDB instance.  This operation only supports "inline" results, i.e. the results will be
 * returned as a result of running this operation.</p>
 *
 * <p>To run a map reduce operation into a given collection, use {@code MapReduceToCollectionOperation}.</p>
 *
 * @param <T> the operations result type.
 * @mongodb.driver.manual core/map-reduce Map-Reduce
 * @since 3.0
 */
public class MapReduceWithInlineResultsOperation<T> implements AsyncReadOperation<MapReduceAsyncCursor<T>>,
                                                               ReadOperation<MapReduceCursor<T>> {
    private final MongoNamespace namespace;
    private final MapReduce mapReduce;
    private final Decoder<T> decoder;

    /**
     * Construct a MapReduceOperation with all the criteria it needs to execute.
     *
     * @param namespace the database and collection namespace for the operation.
     * @param mapReduce the bean containing all the details of the Map Reduce operation to perform.
     * @param decoder the decoder for the result documents.
     */
    public MapReduceWithInlineResultsOperation(final MongoNamespace namespace, final MapReduce mapReduce,
                                               final Decoder<T> decoder) {
        this.namespace = notNull("namespace", namespace);
        this.mapReduce = notNull("mapReduce", mapReduce);
        this.decoder = notNull("decoder", decoder);

        if (!mapReduce.isInline()) {
            throw new IllegalArgumentException("This operation can only be used with inline map reduce operations.  Invalid MapReduce: "
                                               + mapReduce);
        }
    }

    /**
     * Executing this will return a cursor with your results and the statistics in.
     *
     * @param binding the binding
     * @return a MapReduceCursor that can be iterated over to find all the results of the Map Reduce operation.
     */
    @Override
    @SuppressWarnings("unchecked")
    public MapReduceCursor<T> execute(final ReadBinding binding) {
        return withConnection(binding, new OperationHelper.CallableWithConnection<MapReduceCursor<T>>() {
            @Override
            public MapReduceCursor<T> call(final Connection connection) {
                return executeWrappedCommandProtocol(namespace.getDatabaseName(), getCommand(),
                                                     CommandResultDocumentCodec.create(decoder, "results"),
                                                     connection, transformer(connection));
            }
        });
    }

    @Override
    public MongoFuture<MapReduceAsyncCursor<T>> executeAsync(final AsyncReadBinding binding) {
        return executeWrappedCommandProtocolAsync(namespace.getDatabaseName(), getCommand(),
                                                  CommandResultDocumentCodec.create(decoder, "results"),
                                                  binding, asyncTransformer());
    }

    private Function<BsonDocument, MapReduceCursor<T>> transformer(final Connection connection) {
        return new Function<BsonDocument, MapReduceCursor<T>>() {
            @SuppressWarnings("unchecked")
            @Override
            public MapReduceCursor<T> apply(final BsonDocument result) {
                return new MapReduceInlineResultsCursor<T>(BsonDocumentWrapperHelper.<T>toList(result.getArray("results")),
                                                           MapReduceHelper.createStatistics(result), connection.getServerAddress());
            }
        };
    }

    private Function<BsonDocument, MapReduceAsyncCursor<T>> asyncTransformer() {
        return new Function<BsonDocument, MapReduceAsyncCursor<T>>() {
            @SuppressWarnings("unchecked")
            @Override
            public MapReduceAsyncCursor<T> apply(final BsonDocument result) {
                return new MapReduceInlineResultsAsyncCursor<T>(BsonDocumentWrapperHelper.<T>toList(result.getArray("results")),
                                                                MapReduceHelper.createStatistics(result));
            }
        };
    }

    private BsonDocument getCommand() {
        return createMapReduce(namespace.getCollectionName(), mapReduce);
    }

}
