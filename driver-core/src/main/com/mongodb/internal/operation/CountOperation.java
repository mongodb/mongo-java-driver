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

import com.mongodb.ExplainVerbosity;
import com.mongodb.MongoNamespace;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.client.model.Collation;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.binding.AsyncConnectionSource;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.client.model.CountStrategy;
import com.mongodb.internal.connection.NoOpSessionContext;
import com.mongodb.internal.operation.CommandOperationHelper.CommandReadTransformer;
import com.mongodb.internal.operation.CommandOperationHelper.CommandReadTransformerAsync;
import com.mongodb.session.SessionContext;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.operation.CommandOperationHelper.CommandCreator;
import static com.mongodb.internal.operation.CommandOperationHelper.executeCommand;
import static com.mongodb.internal.operation.CommandOperationHelper.executeCommandAsync;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotNull;
import static com.mongodb.internal.operation.DocumentHelper.putIfNotZero;
import static com.mongodb.internal.operation.ExplainHelper.asExplainCommand;
import static com.mongodb.internal.operation.OperationHelper.validateReadConcernAndCollation;
import static com.mongodb.internal.operation.OperationReadConcernHelper.appendReadConcernToCommand;

/**
 * An operation that executes a count.
 *
 * @since 3.0
 */
public class CountOperation implements AsyncReadOperation<Long>, ReadOperation<Long> {
    private static final Decoder<BsonDocument> DECODER = new BsonDocumentCodec();
    private final MongoNamespace namespace;
    private final CountStrategy countStrategy;
    private boolean retryReads;
    private BsonDocument filter;
    private BsonValue hint;
    private long skip;
    private long limit;
    private long maxTimeMS;
    private Collation collation;

    /**
     * Construct a new instance.
     *
     * @param namespace the database and collection namespace for the operation.
     */
    public CountOperation(final MongoNamespace namespace) {
        this(namespace, CountStrategy.COMMAND);
    }

    /**
     * Construct a new instance.
     *
     * @param namespace     the database and collection namespace for the operation.
     * @param countStrategy the strategy to use for calculating the count.
     */
    public CountOperation(final MongoNamespace namespace, final CountStrategy countStrategy) {
        this.namespace = notNull("namespace", namespace);
        this.countStrategy = notNull("countStrategy", countStrategy);
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
     * Enables retryable reads if a read fails due to a network error.
     *
     * @param retryReads true if reads should be retried
     * @return this
     * @since 3.11
     */
    public CountOperation retryReads(final boolean retryReads) {
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
        if (countStrategy.equals(CountStrategy.COMMAND)) {
            return executeCommand(binding, namespace.getDatabaseName(),
                    getCommandCreator(binding.getSessionContext()), DECODER, transformer(), retryReads);
        } else {
            BatchCursor<BsonDocument> cursor = getAggregateOperation().execute(binding);
            return cursor.hasNext() ? getCountFromAggregateResults(cursor.next()) : 0;
        }
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<Long> callback) {
        if (countStrategy.equals(CountStrategy.COMMAND)) {
            executeCommandAsync(binding, namespace.getDatabaseName(), getCommandCreator(binding.getSessionContext()),
                    DECODER, asyncTransformer(), retryReads, callback);
        } else {
            getAggregateOperation().executeAsync(binding, new SingleResultCallback<AsyncBatchCursor<BsonDocument>>(){
                @Override
                public void onResult(final AsyncBatchCursor<BsonDocument> result, final Throwable t) {
                    if (t != null) {
                        callback.onResult(null, t);
                    } else {
                        result.next(new SingleResultCallback<List<BsonDocument>>() {
                            @Override
                            public void onResult(final List<BsonDocument> result, final Throwable t) {
                                if (t != null) {
                                    callback.onResult(null, t);
                                } else {
                                    callback.onResult(getCountFromAggregateResults(result), null);
                                }
                            }
                        });
                    }
                }
            });
        }
    }

    /**
     * Gets an operation whose execution explains this operation.
     *
     * @param explainVerbosity the explain verbosity
     * @return a read operation that when executed will explain this operation
     */
    public ReadOperation<BsonDocument> asExplainableOperation(final ExplainVerbosity explainVerbosity) {
        if (countStrategy.equals(CountStrategy.COMMAND)) {
            return createExplainableOperation(explainVerbosity);
        } else {
            return getAggregateOperation().asExplainableOperation(explainVerbosity);
        }
    }

    /**
     * Gets an operation whose execution explains this operation.
     *
     * @param explainVerbosity the explain verbosity
     * @return a read operation that when executed will explain this operation
     */
    public AsyncReadOperation<BsonDocument> asExplainableOperationAsync(final ExplainVerbosity explainVerbosity) {
        if (countStrategy.equals(CountStrategy.COMMAND)) {
            return createExplainableOperation(explainVerbosity);
        } else {
            return getAggregateOperation().asExplainableOperationAsync(explainVerbosity);
        }
    }

    private CommandReadOperation<BsonDocument> createExplainableOperation(final ExplainVerbosity explainVerbosity) {
        return new CommandReadOperation<BsonDocument>(namespace.getDatabaseName(),
                asExplainCommand(getCommand(NoOpSessionContext.INSTANCE), explainVerbosity),
                new BsonDocumentCodec());
    }

    private CommandReadTransformer<BsonDocument, Long> transformer() {
        return new CommandReadTransformer<BsonDocument, Long>() {
            @Override
            public Long apply(final BsonDocument result, final ConnectionSource source, final Connection connection) {
                return (result.getNumber("n")).longValue();
            }
        };
    }

    private CommandReadTransformerAsync<BsonDocument, Long> asyncTransformer() {
        return new CommandReadTransformerAsync<BsonDocument, Long>() {
            @Override
            public Long apply(final BsonDocument result, final AsyncConnectionSource source, final AsyncConnection connection) {
                return (result.getNumber("n")).longValue();
            }
        };
    }

    private CommandCreator getCommandCreator(final SessionContext sessionContext) {
        return new CommandCreator() {
            @Override
            public BsonDocument create(final ServerDescription serverDescription, final ConnectionDescription connectionDescription) {
                validateReadConcernAndCollation(connectionDescription, sessionContext.getReadConcern(), collation);
                return getCommand(sessionContext);
            }
        };
    }

    private BsonDocument getCommand(final SessionContext sessionContext) {
        BsonDocument document = new BsonDocument("count", new BsonString(namespace.getCollectionName()));

        appendReadConcernToCommand(sessionContext, document);

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

    private AggregateOperation<BsonDocument> getAggregateOperation() {
        return new AggregateOperation<BsonDocument>(namespace, getPipeline(), DECODER)
                .retryReads(retryReads)
                .collation(collation)
                .hint(hint)
                .maxTime(maxTimeMS, TimeUnit.MILLISECONDS);
    }

    private List<BsonDocument> getPipeline() {
        ArrayList<BsonDocument> pipeline = new ArrayList<BsonDocument>();
        pipeline.add(new BsonDocument("$match", filter != null ? filter : new BsonDocument()));
        if (skip > 0) {
            pipeline.add(new BsonDocument("$skip", new BsonInt64(skip)));
        }
        if (limit > 0) {
            pipeline.add(new BsonDocument("$limit", new BsonInt64(limit)));
        }
        pipeline.add(new BsonDocument("$group", new BsonDocument("_id", new BsonInt32(1))
                .append("n", new BsonDocument("$sum", new BsonInt32(1)))));
        return pipeline;
    }

    private Long getCountFromAggregateResults(final List<BsonDocument> results) {
        if (results == null || results.isEmpty()) {
            return 0L;
        } else {
            return results.get(0).getNumber("n").longValue();
        }
    }
}
