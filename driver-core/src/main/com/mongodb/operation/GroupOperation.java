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
import com.mongodb.ServerAddress;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.binding.ReadBinding;
import com.mongodb.client.model.Collation;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.QueryResult;
import com.mongodb.operation.CommandOperationHelper.CommandTransformer;
import org.bson.BsonDocument;
import org.bson.BsonJavaScript;
import org.bson.BsonString;
import org.bson.codecs.Decoder;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static com.mongodb.operation.OperationHelper.AsyncCallableWithConnection;
import static com.mongodb.operation.OperationHelper.CallableWithConnectionAndSource;
import static com.mongodb.operation.OperationHelper.LOGGER;
import static com.mongodb.operation.OperationHelper.checkValidCollation;
import static com.mongodb.operation.OperationHelper.releasingCallback;
import static com.mongodb.operation.OperationHelper.withConnection;

/**
 * Groups documents in a collection by the specified key and performs simple aggregation functions, such as computing counts and sums. The
 * command is analogous to a SELECT ... GROUP BY statement in SQL.
 *
 * @param <T> the operations result type.
 * @mongodb.driver.manual reference/command/group Group Command
 * @since 3.0
 */
public class GroupOperation<T> implements AsyncReadOperation<AsyncBatchCursor<T>>, ReadOperation<BatchCursor<T>> {
    private final MongoNamespace namespace;
    private final Decoder<T> decoder;
    private final BsonJavaScript reduceFunction;
    private final BsonDocument initial;
    private BsonDocument key;
    private BsonJavaScript keyFunction;
    private BsonDocument filter;
    private BsonJavaScript finalizeFunction;
    private Collation collation;

    /**
     * Create an operation that will perform a Group on a given collection.
     *
     * @param namespace the database and collection namespace for the operation.
     * @param reduceFunction The aggregation function that operates on the documents during the grouping operation.
     * @param initial The initial the aggregation result document.
     * @param decoder the decoder for the result documents.
     * @mongodb.driver.manual reference/command/group Group Command
     */
    public GroupOperation(final MongoNamespace namespace, final BsonJavaScript reduceFunction,
                          final BsonDocument initial, final Decoder<T> decoder) {
        this.namespace = notNull("namespace", namespace);
        this.reduceFunction = notNull("reduceFunction", reduceFunction);
        this.initial = notNull("initial", initial);
        this.decoder = notNull("decoder", decoder);
    }

    /**
     * Gets the namespace.
     *
     * @return the namespace
     * @since 3.4
     */
    public MongoNamespace getNamespace() {
        return namespace;
    }


    /**
     * Gets the decoder used to decode the result documents.
     *
     * @return the decoder
     * @since 3.4
     */
    public Decoder<T> getDecoder() {
        return decoder;
    }

    /**
     * Gets the document containing the field or fields to group.
     *
     * @return the document containing the field or fields to group.
     */
    public BsonDocument getKey() {
        return key;
    }

    /**
     * Sets the document containing the field or fields to group.
     *
     * @param key the document containing the field or fields to group.
     * @return this
     */
    public GroupOperation<T> key(final BsonDocument key) {
        this.key = key;
        return this;
    }

    /**
     * Gets the function that creates a "key object" for use as the grouping key.
     *
     * @return the function that creates a "key object" for use as the grouping key.
     */
    public BsonJavaScript getKeyFunction() {
        return keyFunction;
    }

    /**
     * Sets the function that creates a "key object" for use as the grouping key.
     *
     * @param keyFunction the function that creates a "key object" for use as the grouping key.
     * @return this
     */
    public GroupOperation<T> keyFunction(final BsonJavaScript keyFunction) {
        this.keyFunction = keyFunction;
        return this;
    }

    /**
     * Gets the initial the aggregation result document.
     *
     * @return the initial the aggregation result document.
     */
    public BsonDocument getInitial() {
        return initial;
    }

    /**
     * Gets the aggregation function that operates on the documents during the grouping operation.
     *
     * @return the aggregation function that operates on the documents during the grouping operation.
     */
    public BsonJavaScript getReduceFunction() {
        return reduceFunction;
    }

    /**
     * Gets the query filter to determine which documents in the collection to process.
     *
     * @return the query filter to determine which documents in the collection to process.
     */
    public BsonDocument getFilter() {
        return filter;
    }

    /**
     * Sets the optional query filter to determine which documents in the collection to process.
     *
     * @param filter the query filter to determine which documents in the collection to process.
     * @return this
     */
    public GroupOperation<T> filter(final BsonDocument filter) {
        this.filter = filter;
        return this;
    }

    /**
     * Gets the function that runs each item in the result before returning the final value.
     *
     * @return the function that runs each item in the result set before returning the final value.
     */
    public BsonJavaScript getFinalizeFunction() {
        return finalizeFunction;
    }

    /**
     * Sets the function that runs each item in the result set before returning the final value.
     *
     * @param finalizeFunction the function that runs each item in the result set before returning the final value.
     * @return this
     */
    public GroupOperation<T> finalizeFunction(final BsonJavaScript finalizeFunction) {
        this.finalizeFunction = finalizeFunction;
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
    public GroupOperation<T> collation(final Collation collation) {
        this.collation = collation;
        return this;
    }

    /**
     * Will return a cursor of Documents containing the results of the group operation.
     *
     * @param binding the binding
     * @return a MongoCursor of T, the results of the group operation in a form to be iterated over.
     */
    @Override
    public BatchCursor<T> execute(final ReadBinding binding) {
        return withConnection(binding, new CallableWithConnectionAndSource<BatchCursor<T>>() {
            @Override
            public BatchCursor<T> call(final ConnectionSource connectionSource, final Connection connection) {
                checkValidCollation(connection, collation);
                return executeWrappedCommandProtocol(binding, namespace.getDatabaseName(), getCommand(),
                                                     CommandResultDocumentCodec.create(decoder, "retval"),
                                                     connection, transformer(connectionSource, connection));
            }
        });
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        withConnection(binding, new AsyncCallableWithConnection() {
            @Override
            public void call(final AsyncConnection connection, final Throwable t) {
                SingleResultCallback<AsyncBatchCursor<T>> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
                if (t != null) {
                    errHandlingCallback.onResult(null, t);
                } else {
                    final SingleResultCallback<AsyncBatchCursor<T>> wrappedCallback = releasingCallback(errHandlingCallback, connection);
                    checkValidCollation(connection, collation, new AsyncCallableWithConnection() {
                        @Override
                        public void call(final AsyncConnection connection, final Throwable t) {
                            if (t != null) {
                                wrappedCallback.onResult(null, t);
                            } else {
                                executeWrappedCommandProtocolAsync(binding, namespace.getDatabaseName(), getCommand(),
                                        CommandResultDocumentCodec.create(decoder, "retval"), connection, asyncTransformer(connection),
                                        wrappedCallback);
                            }
                        }
                    });
                }
            }
        });
    }

    private BsonDocument getCommand() {
        BsonDocument commandDocument = new BsonDocument("ns", new BsonString(namespace.getCollectionName()));

        if (getKey() != null) {
            commandDocument.put("key", getKey());
        } else if (getKeyFunction() != null) {
            commandDocument.put("$keyf", getKeyFunction());
        }

        commandDocument.put("initial", getInitial());
        commandDocument.put("$reduce", getReduceFunction());

        if (getFinalizeFunction() != null) {
            commandDocument.put("finalize", getFinalizeFunction());
        }
        if (getFilter() != null) {
            commandDocument.put("cond", getFilter());
        }
        if (getCollation() != null) {
            commandDocument.put("collation", collation.asDocument());
        }
        return new BsonDocument("group", commandDocument);
    }

    private CommandTransformer<BsonDocument, BatchCursor<T>> transformer(final ConnectionSource source, final Connection connection) {
        return new CommandTransformer<BsonDocument, BatchCursor<T>>() {
            @Override
            public BatchCursor<T> apply(final BsonDocument result, final ServerAddress serverAddress) {
                return new QueryBatchCursor<T>(createQueryResult(result, connection.getDescription()), 0, 0, decoder, source);

            }
        };
    }

    private CommandTransformer<BsonDocument, AsyncBatchCursor<T>> asyncTransformer(final AsyncConnection connection) {
        return new CommandTransformer<BsonDocument, AsyncBatchCursor<T>>() {
            @Override
            public AsyncBatchCursor<T> apply(final BsonDocument result, final ServerAddress serverAddress) {
                return new AsyncQueryBatchCursor<T>(createQueryResult(result, connection.getDescription()), 0, 0, decoder);
            }
        };
    }

    @SuppressWarnings("unchecked")
    private QueryResult<T> createQueryResult(final BsonDocument result, final ConnectionDescription description) {
        return new QueryResult<T>(namespace, BsonDocumentWrapperHelper.<T>toList(result, "retval"), 0,
                                  description.getServerAddress());
    }
}
