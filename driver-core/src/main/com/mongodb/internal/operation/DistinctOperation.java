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
import com.mongodb.client.model.Collation;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.ClientSideOperationTimeout;
import com.mongodb.internal.ClientSideOperationTimeoutFactory;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncConnectionSource;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.connection.AsyncConnection;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.QueryResult;
import com.mongodb.internal.operation.AsyncCommandOperationHelper.CommandReadTransformerAsync;
import com.mongodb.internal.operation.SyncCommandOperationHelper.CommandReadTransformer;
import com.mongodb.internal.session.SessionContext;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.codecs.Codec;
import org.bson.codecs.Decoder;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.AsyncCommandOperationHelper.executeCommandAsync;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotNullOrEmpty;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotZero;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.OperationHelper.validateReadConcernAndCollation;
import static com.mongodb.internal.operation.OperationReadConcernHelper.appendReadConcernToCommand;
import static com.mongodb.internal.operation.SyncCommandOperationHelper.executeCommand;

/**
 * Finds the distinct values for a specified field across a single collection.
 *
 * <p>When possible, the distinct command uses an index to find documents and return values.</p>
 *
 * @param <T> the type of the distinct value
 * @mongodb.driver.manual reference/command/distinct Distinct Command
 * @since 3.0
 */
public class DistinctOperation<T> implements AsyncReadOperation<AsyncBatchCursor<T>>, ReadOperation<BatchCursor<T>> {
    private static final String VALUES = "values";
    private final ClientSideOperationTimeoutFactory clientSideOperationTimeoutFactory;
    private final MongoNamespace namespace;
    private final String fieldName;
    private final Decoder<T> decoder;
    private boolean retryReads;
    private BsonDocument filter;
    private Collation collation;

    /**
     * Construct an instance.
     *
     * @param clientSideOperationTimeoutFactory the client side operation timeout factory
     * @param namespace the database and collection namespace for the operation.
     * @param fieldName the name of the field to return distinct values.
     * @param decoder   the decoder for the result documents.
     */
    public DistinctOperation(final ClientSideOperationTimeoutFactory clientSideOperationTimeoutFactory, final MongoNamespace namespace,
                             final String fieldName, final Decoder<T> decoder) {
        this.clientSideOperationTimeoutFactory = notNull("clientSideOperationTimeoutFactory", clientSideOperationTimeoutFactory);
        this.namespace = notNull("namespace", namespace);
        this.fieldName = notNull("fieldName", fieldName);
        this.decoder = notNull("decoder", decoder);
    }

    /**
     * Gets the query filter.
     *
     * @return the query filter
     * @mongodb.driver.manual reference/method/db.collection.find/ Filter
     */
    public BsonDocument getFilter() {
        return filter;
    }

    /**
     * Sets the query filter to apply to the query.
     *
     * @param filter the query filter, which may be null.
     * @return this
     * @mongodb.driver.manual reference/method/db.collection.find/ Filter
     */
    public DistinctOperation<T> filter(final BsonDocument filter) {
        this.filter = filter;
        return this;
    }

    /**
     * Enables retryable reads if a read fails due to a network error.
     *
     * @param retryReads true if reads should be retried
     * @return this
     * @mongodb.driver.manual reference/method/db.collection.find/ Filter
     * @since 3.11
     */
    public DistinctOperation<T> retryReads(final boolean retryReads) {
        this.retryReads = retryReads;
        return this;
    }

    /**
     * Gets the value for retryable reads. The default is true.
     *
     * @return the retryable reads value
     * @since 3.11
     */
    public boolean getRetryReads() {
        return retryReads;
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
    public DistinctOperation<T> collation(final Collation collation) {
        this.collation = collation;
        return this;
    }

    @Override
    public BatchCursor<T> execute(final ReadBinding binding) {
        return executeCommand(clientSideOperationTimeoutFactory.create(), binding, namespace.getDatabaseName(),
                getCommandCreator(binding.getSessionContext()), createCommandDecoder(), transformer(), retryReads);
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        executeCommandAsync(clientSideOperationTimeoutFactory.create(), binding, namespace.getDatabaseName(),
                getCommandCreator(binding.getSessionContext()), createCommandDecoder(),
                asyncTransformer(), retryReads, errorHandlingCallback(callback, LOGGER));
    }

    private Codec<BsonDocument> createCommandDecoder() {
        return CommandResultDocumentCodec.create(decoder, VALUES);
    }

    private QueryResult<T> createQueryResult(final BsonDocument result, final ConnectionDescription description) {
        return new QueryResult<T>(namespace, BsonDocumentWrapperHelper.<T>toList(result, VALUES), 0L,
                description.getServerAddress());
    }

    private CommandReadTransformer<BsonDocument, BatchCursor<T>> transformer() {
        return new CommandReadTransformer<BsonDocument, BatchCursor<T>>() {
            @Override
            public BatchCursor<T> apply(final ClientSideOperationTimeout clientSideOperationTimeout, final ConnectionSource source,
                                        final Connection connection, final BsonDocument result) {
                QueryResult<T> queryResult = createQueryResult(result, connection.getDescription());
                return new QueryBatchCursor<T>(clientSideOperationTimeout, queryResult, 0, 0, decoder, source);
            }
        };
    }

    private CommandReadTransformerAsync<BsonDocument, AsyncBatchCursor<T>> asyncTransformer() {
        return new CommandReadTransformerAsync<BsonDocument, AsyncBatchCursor<T>>() {
            @Override
            public AsyncBatchCursor<T> apply(final ClientSideOperationTimeout clientSideOperationTimeout,
                                             final AsyncConnectionSource source, final AsyncConnection connection,
                                              final BsonDocument result) {
                QueryResult<T> queryResult = createQueryResult(result, connection.getDescription());
                return new AsyncSingleBatchQueryCursor<T>(clientSideOperationTimeout, queryResult);
            }
        };
    }

    private CommandCreator getCommandCreator(final SessionContext sessionContext) {
        return new CommandCreator() {
            @Override
            public BsonDocument create(final ClientSideOperationTimeout clientSideOperationTimeout,
                                       final ServerDescription serverDescription, final ConnectionDescription connectionDescription) {
                validateReadConcernAndCollation(connectionDescription, sessionContext.getReadConcern(), collation);
                return getCommand(clientSideOperationTimeout, sessionContext);
            }
        };
    }

    private BsonDocument getCommand(final ClientSideOperationTimeout clientSideOperationTimeout, final SessionContext sessionContext) {
        BsonDocument commandDocument = new BsonDocument("distinct", new BsonString(namespace.getCollectionName()));
        appendReadConcernToCommand(sessionContext, commandDocument);
        commandDocument.put("key", new BsonString(fieldName));
        putIfNotNullOrEmpty(commandDocument, "query", filter);
        putIfNotZero(commandDocument, "maxTimeMS", clientSideOperationTimeout.getMaxTimeMS());
        if (collation != null) {
            commandDocument.put("collation", collation.asDocument());
        }
        return commandDocument;
    }
}
