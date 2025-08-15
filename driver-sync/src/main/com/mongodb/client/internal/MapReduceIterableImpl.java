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

package com.mongodb.client.internal;

import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.client.model.Collation;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.client.model.FindOptions;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.operation.BatchCursor;
import com.mongodb.internal.operation.MapReduceStatistics;
import com.mongodb.internal.operation.Operations;
import com.mongodb.internal.operation.ReadOperationCursor;
import com.mongodb.internal.operation.ReadOperationMapReduceCursor;
import com.mongodb.internal.operation.WriteOperation;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.notNull;

@SuppressWarnings("deprecation")
class MapReduceIterableImpl<TDocument, TResult> extends MongoIterableImpl<TResult> implements com.mongodb.client.MapReduceIterable<TResult> {
    private final Operations<TDocument> operations;
    private final MongoNamespace namespace;
    private final Class<TResult> resultClass;
    private final String mapFunction;
    private final String reduceFunction;

    private boolean inline = true;
    private String collectionName;
    private String finalizeFunction;
    private Bson scope;
    private Bson filter;
    private Bson sort;
    private int limit;
    private boolean jsMode;
    private boolean verbose = true;
    private long maxTimeMS;
    private com.mongodb.client.model.MapReduceAction action = com.mongodb.client.model.MapReduceAction.REPLACE;
    private String databaseName;
    private Boolean bypassDocumentValidation;
    private Collation collation;

    MapReduceIterableImpl(@Nullable final ClientSession clientSession, final MongoNamespace namespace, final Class<TDocument> documentClass,
                          final Class<TResult> resultClass, final CodecRegistry codecRegistry, final ReadPreference readPreference,
                          final ReadConcern readConcern, final WriteConcern writeConcern, final OperationExecutor executor,
                          final String mapFunction, final String reduceFunction, final TimeoutSettings timeoutSettings) {
        super(clientSession, executor, readConcern, readPreference, false, timeoutSettings);
        this.operations = new Operations<>(namespace, documentClass, readPreference, codecRegistry, readConcern, writeConcern,
                false, false, timeoutSettings);
        this.namespace = notNull("namespace", namespace);
        this.resultClass = notNull("resultClass", resultClass);
        this.mapFunction = notNull("mapFunction", mapFunction);
        this.reduceFunction = notNull("reduceFunction", reduceFunction);
    }

    @Override
    public void toCollection() {
        if (inline) {
            throw new IllegalStateException("The options must specify a non-inline result");
        }

        getExecutor().execute(createMapReduceToCollectionOperation(), getReadConcern(), getClientSession());
    }

    @Override
    public com.mongodb.client.MapReduceIterable<TResult> collectionName(final String collectionName) {
        this.collectionName = notNull("collectionName", collectionName);
        this.inline = false;
        return this;
    }

    @Override
    public com.mongodb.client.MapReduceIterable<TResult> finalizeFunction(@Nullable final String finalizeFunction) {
        this.finalizeFunction = finalizeFunction;
        return this;
    }

    @Override
    public com.mongodb.client.MapReduceIterable<TResult> scope(@Nullable final Bson scope) {
        this.scope = scope;
        return this;
    }

    @Override
    public com.mongodb.client.MapReduceIterable<TResult> sort(@Nullable final Bson sort) {
        this.sort = sort;
        return this;
    }

    @Override
    public com.mongodb.client.MapReduceIterable<TResult> filter(@Nullable final Bson filter) {
        this.filter = filter;
        return this;
    }

    @Override
    public com.mongodb.client.MapReduceIterable<TResult> limit(final int limit) {
        this.limit = limit;
        return this;
    }

    @Override
    public com.mongodb.client.MapReduceIterable<TResult> jsMode(final boolean jsMode) {
        this.jsMode = jsMode;
        return this;
    }

    @Override
    public com.mongodb.client.MapReduceIterable<TResult> verbose(final boolean verbose) {
        this.verbose = verbose;
        return this;
    }

    @Override
    public com.mongodb.client.MapReduceIterable<TResult> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    @Override
    public com.mongodb.client.MapReduceIterable<TResult> action(final com.mongodb.client.model.MapReduceAction action) {
        this.action = action;
        return this;
    }

    @Override
    public com.mongodb.client.MapReduceIterable<TResult> databaseName(@Nullable final String databaseName) {
        this.databaseName = databaseName;
        return this;
    }

    @Override
    public com.mongodb.client.MapReduceIterable<TResult> batchSize(final int batchSize) {
        super.batchSize(batchSize);
        return this;
    }

    @Override
    public com.mongodb.client.MapReduceIterable<TResult> timeoutMode(final TimeoutMode timeoutMode) {
        super.timeoutMode(timeoutMode);
        return this;
    }

    @Override
    public com.mongodb.client.MapReduceIterable<TResult> bypassDocumentValidation(@Nullable final Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    @Override
    public com.mongodb.client.MapReduceIterable<TResult> collation(@Nullable final Collation collation) {
        this.collation = collation;
        return this;
    }

    @Override
    ReadPreference getReadPreference() {
        if (inline) {
            return super.getReadPreference();
        } else {
            return primary();
        }
    }


    protected OperationExecutor getExecutor() {
        return getExecutor(operations.createTimeoutSettings(maxTimeMS));
    }

    @Override
    public ReadOperationCursor<TResult> asReadOperation() {
        if (inline) {
            ReadOperationMapReduceCursor<TResult> operation = operations.mapReduce(mapFunction, reduceFunction, finalizeFunction,
                    resultClass, filter, limit, jsMode, scope, sort, verbose, collation);
            return new WrappedMapReduceReadOperation<>(operation);
        } else {
            getExecutor().execute(createMapReduceToCollectionOperation(), getReadConcern(), getClientSession());

            String dbName = databaseName != null ? databaseName : namespace.getDatabaseName();

            FindOptions findOptions = new FindOptions().collation(collation);
            Integer batchSize = getBatchSize();
            if (batchSize != null) {
                findOptions.batchSize(batchSize);
            }
            return operations.find(new MongoNamespace(dbName, collectionName), new BsonDocument(), resultClass, findOptions);
        }

    }

    private WriteOperation<MapReduceStatistics> createMapReduceToCollectionOperation() {
        return operations.mapReduceToCollection(databaseName, collectionName, mapFunction, reduceFunction, finalizeFunction, filter,
                limit, jsMode, scope, sort, verbose, action, bypassDocumentValidation, collation
        );
    }

    // this could be inlined, but giving it a name so that it's unit-testable
    static class WrappedMapReduceReadOperation<TResult> implements ReadOperationCursor<TResult> {
        private final ReadOperationMapReduceCursor<TResult> operation;

        ReadOperationMapReduceCursor<TResult> getOperation() {
            return operation;
        }

        WrappedMapReduceReadOperation(final ReadOperationMapReduceCursor<TResult> operation) {
            this.operation = operation;
        }

        @Override
        public String getCommandName() {
            return operation.getCommandName();
        }

        @Override
        public BatchCursor<TResult> execute(final ReadBinding binding, final OperationContext operationContext) {
            return operation.execute(binding, operationContext);
        }

        @Override
        public void executeAsync(final AsyncReadBinding binding, final OperationContext operationContext, final SingleResultCallback<AsyncBatchCursor<TResult>> callback) {
            throw new UnsupportedOperationException("This operation is sync only");
        }
    }
}
