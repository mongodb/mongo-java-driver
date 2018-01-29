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

package com.mongodb.operation;

import com.mongodb.ServerAddress;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncConnectionSource;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.binding.ReadBinding;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.QueryResult;
import com.mongodb.operation.CommandOperationHelper.CommandTransformer;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.codecs.Decoder;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static com.mongodb.operation.OperationHelper.AsyncCallableWithConnectionAndSource;
import static com.mongodb.operation.OperationHelper.LOGGER;
import static com.mongodb.operation.OperationHelper.releasingCallback;
import static com.mongodb.operation.OperationHelper.withConnection;


/**
 * An operation that provides a cursor allowing iteration through the metadata of all the databases for a MongoClient.
 *
 * @param <T> the document type
 * @since 3.0
 */
public class ListDatabasesOperation<T> implements AsyncReadOperation<AsyncBatchCursor<T>>, ReadOperation<BatchCursor<T>> {
    private final Decoder<T> decoder;

    private long maxTimeMS;
    private BsonDocument filter;
    private Boolean nameOnly;

    /**
     * Construct a new instance.
     *
     * @param decoder the decoder to use for the results
     */
    public ListDatabasesOperation(final Decoder<T> decoder) {
        this.decoder = notNull("decoder", decoder);
    }

    /**
     * Gets the maximum execution time on the server for this operation.  The default is 0, which places no limit on the execution time.
     *
     * @param timeUnit the time unit to return the result in
     * @return the maximum execution time in the given time unit
     * @mongodb.driver.manual reference/operator/meta/maxTimeMS/ Max Time
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
     * @mongodb.driver.manual reference/operator/meta/maxTimeMS/ Max Time
     */
    public ListDatabasesOperation<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    /**
     * Sets the query filter to apply to the returned database names.
     *
     * @param filter the filter, which may be null.
     * @return this
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    public ListDatabasesOperation<T> filter(final BsonDocument filter) {
        this.filter = filter;
        return this;
    }

    /**
     * Gets the query filter to apply to the returned database names.
     *
     * @return this
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    public BsonDocument getFilter() {
        return filter;
    }

    /**
     * Sets the nameOnly flag that indicates whether the command should return just the database names or return the database names and
     * size information.
     *
     * @param nameOnly the nameOnly flag, which may be null
     * @return this
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    public ListDatabasesOperation<T> nameOnly(final Boolean nameOnly) {
        this.nameOnly = nameOnly;
        return this;
    }

    /**
     * Gets the nameOnly flag that indicates whether the command should return just the database names or return the database names and
     * size information.
     *
     * @return this
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    public Boolean getNameOnly() {
        return nameOnly;
    }

    /**
     * Executing this will return a list of all the databases names in the MongoDB instance.
     *
     * @param binding the binding.
     * @return a List of Strings of the names of all the databases in the MongoDB instance.
     */
    @Override
    public BatchCursor<T> execute(final ReadBinding binding) {
        return withConnection(binding, new OperationHelper.CallableWithConnectionAndSource<BatchCursor<T>>() {
            @Override
            public BatchCursor<T> call(final ConnectionSource source, final Connection connection) {
                return executeWrappedCommandProtocol(binding, "admin", getCommand(),
                        CommandResultDocumentCodec.create(decoder, "databases"), connection, transformer(source, connection));
            }
        });
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        withConnection(binding, new AsyncCallableWithConnectionAndSource() {
            @Override
            public void call(final AsyncConnectionSource source, final AsyncConnection connection, final Throwable t) {
                SingleResultCallback<AsyncBatchCursor<T>> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
                if (t != null) {
                    errHandlingCallback.onResult(null, t);
                } else {
                    executeWrappedCommandProtocolAsync(binding,  "admin", getCommand(),
                            CommandResultDocumentCodec.create(decoder, "databases"), connection, asyncTransformer(source, connection),
                            releasingCallback(errHandlingCallback, connection));
                }
            }
        });
    }

    private CommandTransformer<BsonDocument, BatchCursor<T>> transformer(final ConnectionSource source, final Connection connection) {
        return new CommandTransformer<BsonDocument, BatchCursor<T>>() {
            @Override
            public BatchCursor<T> apply(final BsonDocument result, final ServerAddress serverAddress) {
                return new QueryBatchCursor<T>(createQueryResult(result, connection.getDescription()), 0, 0, decoder, source);
            }
        };
    }

    private CommandTransformer<BsonDocument, AsyncBatchCursor<T>> asyncTransformer(final AsyncConnectionSource source,
                                                                         final AsyncConnection connection) {
        return new CommandTransformer<BsonDocument, AsyncBatchCursor<T>>() {
            @Override
            public AsyncBatchCursor<T> apply(final BsonDocument result, final ServerAddress serverAddress) {
                return new AsyncQueryBatchCursor<T>(createQueryResult(result, connection.getDescription()), 0, 0, 0, decoder, source,
                                                    connection);
            }
        };
    }

    @SuppressWarnings("unchecked")
    private QueryResult<T> createQueryResult(final BsonDocument result, final ConnectionDescription description) {
        return new QueryResult<T>(null, BsonDocumentWrapperHelper.<T>toList(result, "databases"), 0,
                description.getServerAddress());
    }

    private BsonDocument getCommand() {
        BsonDocument command = new BsonDocument("listDatabases", new BsonInt32(1));
        if (maxTimeMS > 0) {
            command.put("maxTimeMS", new BsonInt64(maxTimeMS));
        }
        if (filter != null) {
            command.put("filter", filter);
        }
        if (nameOnly != null) {
            command.put("nameOnly", new BsonBoolean(nameOnly));
        }
        return command;
    }
}
