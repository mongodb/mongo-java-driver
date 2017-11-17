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

import com.mongodb.ExplainVerbosity;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ServerAddress;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.ReadBinding;
import com.mongodb.client.model.Collation;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.Connection;
import com.mongodb.session.SessionContext;
import com.mongodb.internal.connection.NoOpSessionContext;
import com.mongodb.operation.CommandOperationHelper.CommandTransformer;
import com.mongodb.operation.OperationHelper.AsyncCallableWithConnection;
import com.mongodb.operation.OperationHelper.CallableWithConnection;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static com.mongodb.operation.DocumentHelper.putIfNotNull;
import static com.mongodb.operation.DocumentHelper.putIfNotZero;
import static com.mongodb.operation.ExplainHelper.asExplainCommand;
import static com.mongodb.operation.OperationHelper.LOGGER;
import static com.mongodb.operation.OperationHelper.validateReadConcernAndCollation;
import static com.mongodb.operation.OperationHelper.releasingCallback;
import static com.mongodb.operation.OperationHelper.withConnection;
import static com.mongodb.operation.ReadConcernHelper.appendReadConcernToCommand;

/**
 * An operation that executes a count.
 *
 * @since 3.0
 */
public class CountOperation implements AsyncReadOperation<Long>, ReadOperation<Long> {
    private final MongoNamespace namespace;
    private BsonDocument filter;
    private BsonValue hint;
    private long skip;
    private long limit;
    private long maxTimeMS;
    private ReadConcern readConcern = ReadConcern.DEFAULT;
    private Collation collation;

    /**
     * Construct a new instance.
     *
     * @param namespace the database and collection namespace for the operation.
     */
    public CountOperation(final MongoNamespace namespace) {
        this.namespace = notNull("namespace", namespace);
    }

    /**
     * Gets the query filter.
     *
     * @return the query filter
     * @mongodb.driver.manual reference/method/db.collection.find/ filter
     */
    public BsonDocument getFilter() {
        return filter;
    }

    /**
     * Sets the filter to apply to the query.
     *
     * @param filter the filter, which may be null.
     * @return this
     * @mongodb.driver.manual reference/method/db.collection.find/ Filter
     */
    public CountOperation filter(final BsonDocument filter) {
        this.filter = filter;
        return this;
    }

    /**
     * Gets the hint to apply.
     *
     * @return the hint, which should describe an existing
     */
    public BsonValue getHint() {
        return hint;
    }

    /**
     * Sets the hint to apply.
     *
     * @param hint a value describing the index which should be used for this operation.
     * @return this
     */
    public CountOperation hint(final BsonValue hint) {
        this.hint = hint;
        return this;
    }

    /**
     * Gets the limit to apply.  The default is 0, which means there is no limit.
     *
     * @return the limit
     * @mongodb.driver.manual reference/method/cursor.limit/#cursor.limit Limit
     */
    public long getLimit() {
        return limit;
    }

    /**
     * Sets the limit to apply.
     *
     * @param limit the limit
     * @return this
     * @mongodb.driver.manual reference/method/cursor.limit/#cursor.limit Limit
     */
    public CountOperation limit(final long limit) {
        this.limit = limit;
        return this;
    }

    /**
     * Gets the number of documents to skip.  The default is 0.
     *
     * @return the number of documents to skip, which may be null
     * @mongodb.driver.manual reference/method/cursor.skip/#cursor.skip Skip
     */
    public long getSkip() {
        return skip;
    }

    /**
     * Sets the number of documents to skip.
     *
     * @param skip the number of documents to skip
     * @return this
     * @mongodb.driver.manual reference/method/cursor.skip/#cursor.skip Skip
     */
    public CountOperation skip(final long skip) {
        this.skip = skip;
        return this;
    }

    /**
     * Gets the maximum execution time on the server for this operation.  The default is 0, which places no limit on the execution time.
     *
     * @param timeUnit the time unit to return the result in
     * @return the maximum execution time in the given time unit
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
     */
    public CountOperation maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    /**
     * Gets the read concern
     *
     * @return the read concern
     * @since 3.2
     * @mongodb.driver.manual reference/readConcern/ Read Concern
     */
    public ReadConcern getReadConcern() {
        return readConcern;
    }

    /**
     * Sets the read concern
     * @param readConcern the read concern
     * @return this
     * @since 3.2
     * @mongodb.driver.manual reference/readConcern/ Read Concern
     */
    public CountOperation readConcern(final ReadConcern readConcern) {
        this.readConcern = notNull("readConcern", readConcern);
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
     */
    public CountOperation collation(final Collation collation) {
        this.collation = collation;
        return this;
    }

    @Override
    public Long execute(final ReadBinding binding) {
        return withConnection(binding, new CallableWithConnection<Long>() {
            @Override
            public Long call(final Connection connection) {
                validateReadConcernAndCollation(connection, readConcern, collation);
                return executeWrappedCommandProtocol(binding, namespace.getDatabaseName(), getCommand(binding.getSessionContext()),
                        new BsonDocumentCodec(), connection, transformer());
            }
        });
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<Long> callback) {
        withConnection(binding, new AsyncCallableWithConnection() {
            @Override
            public void call(final AsyncConnection connection, final Throwable t) {
                SingleResultCallback<Long> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
                if (t != null) {
                    errHandlingCallback.onResult(null, t);
                } else {
                    final SingleResultCallback<Long> wrappedCallback = releasingCallback(errHandlingCallback, connection);
                    validateReadConcernAndCollation(connection, readConcern, collation, new AsyncCallableWithConnection() {
                        @Override
                        public void call(final AsyncConnection connection, final Throwable t) {
                            if (t != null) {
                                wrappedCallback.onResult(null, t);
                            } else {
                                executeWrappedCommandProtocolAsync(binding, namespace.getDatabaseName(),
                                        getCommand(binding.getSessionContext()), new BsonDocumentCodec(), connection, transformer(),
                                        wrappedCallback);
                            }
                        }
                    });
                }
            }
        });
    }

    /**
     * Gets an operation whose execution explains this operation.
     *
     * @param explainVerbosity the explain verbosity
     * @return a read operation that when executed will explain this operation
     */
    public ReadOperation<BsonDocument> asExplainableOperation(final ExplainVerbosity explainVerbosity) {
        return createExplainableOperation(explainVerbosity);
    }

    /**
     * Gets an operation whose execution explains this operation.
     *
     * @param explainVerbosity the explain verbosity
     * @return a read operation that when executed will explain this operation
     */
    public AsyncReadOperation<BsonDocument> asExplainableOperationAsync(final ExplainVerbosity explainVerbosity) {
        return createExplainableOperation(explainVerbosity);
    }

    private CommandReadOperation<BsonDocument> createExplainableOperation(final ExplainVerbosity explainVerbosity) {
        return new CommandReadOperation<BsonDocument>(namespace.getDatabaseName(),
                                                      asExplainCommand(getCommand(NoOpSessionContext.INSTANCE), explainVerbosity),
                                                      new BsonDocumentCodec());
    }

    private CommandTransformer<BsonDocument, Long> transformer() {
        return new CommandTransformer<BsonDocument, Long>() {
            @Override
            public Long apply(final BsonDocument result, final ServerAddress serverAddress) {
                return (result.getNumber("n")).longValue();
            }
        };
    }

    private BsonDocument getCommand(final SessionContext sessionContext) {
        BsonDocument document = new BsonDocument("count", new BsonString(namespace.getCollectionName()));

        appendReadConcernToCommand(readConcern, sessionContext, document);

        putIfNotNull(document, "query", filter);
        putIfNotZero(document, "limit", limit);
        putIfNotZero(document, "skip", skip);
        putIfNotNull(document, "hint", hint);
        putIfNotZero(document, "maxTimeMS", maxTimeMS);

        if (collation != null) {
            document.put("collation", collation.asDocument());
        }
        return document;
    }
}
