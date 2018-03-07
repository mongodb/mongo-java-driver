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
import com.mongodb.binding.ReadBinding;
import com.mongodb.client.MapReduceIterable;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.FindOptions;
import com.mongodb.client.model.MapReduceAction;
import com.mongodb.internal.operation.SyncOperations;
import com.mongodb.lang.Nullable;
import com.mongodb.operation.BatchCursor;
import com.mongodb.operation.MapReduceBatchCursor;
import com.mongodb.operation.MapReduceStatistics;
import com.mongodb.operation.ReadOperation;
import com.mongodb.operation.WriteOperation;
import com.mongodb.session.ClientSession;
import org.bson.BsonDocument;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.notNull;

class MapReduceIterableImpl<TDocument, TResult> extends MongoIterableImpl<TResult> implements MapReduceIterable<TResult> {
    private final SyncOperations<TDocument> operations;
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
    private MapReduceAction action = MapReduceAction.REPLACE;
    private String databaseName;
    private boolean sharded;
    private boolean nonAtomic;
    private Boolean bypassDocumentValidation;
    private Collation collation;

    MapReduceIterableImpl(@Nullable final ClientSession clientSession, final MongoNamespace namespace, final Class<TDocument> documentClass,
                          final Class<TResult> resultClass, final CodecRegistry codecRegistry, final ReadPreference readPreference,
                          final ReadConcern readConcern, final WriteConcern writeConcern, final OperationExecutor executor,
                          final String mapFunction, final String reduceFunction) {
        super(clientSession, executor, readConcern, readPreference);
        this.operations = new SyncOperations<TDocument>(namespace, documentClass, readPreference, codecRegistry, writeConcern, false,
                readConcern);
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

        getExecutor().execute(createMapReduceToCollectionOperation(), getClientSession());
    }

    @Override
    public MapReduceIterable<TResult> collectionName(final String collectionName) {
        this.collectionName = notNull("collectionName", collectionName);
        this.inline = false;
        return this;
    }

    @Override
    public MapReduceIterable<TResult> finalizeFunction(@Nullable final String finalizeFunction) {
        this.finalizeFunction = finalizeFunction;
        return this;
    }

    @Override
    public MapReduceIterable<TResult> scope(@Nullable final Bson scope) {
        this.scope = scope;
        return this;
    }

    @Override
    public MapReduceIterable<TResult> sort(@Nullable final Bson sort) {
        this.sort = sort;
        return this;
    }

    @Override
    public MapReduceIterable<TResult> filter(@Nullable final Bson filter) {
        this.filter = filter;
        return this;
    }

    @Override
    public MapReduceIterable<TResult> limit(final int limit) {
        this.limit = limit;
        return this;
    }

    @Override
    public MapReduceIterable<TResult> jsMode(final boolean jsMode) {
        this.jsMode = jsMode;
        return this;
    }

    @Override
    public MapReduceIterable<TResult> verbose(final boolean verbose) {
        this.verbose = verbose;
        return this;
    }

    @Override
    public MapReduceIterable<TResult> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    @Override
    public MapReduceIterable<TResult> action(final MapReduceAction action) {
        this.action = action;
        return this;
    }

    @Override
    public MapReduceIterable<TResult> databaseName(@Nullable final String databaseName) {
        this.databaseName = databaseName;
        return this;
    }

    @Override
    public MapReduceIterable<TResult> sharded(final boolean sharded) {
        this.sharded = sharded;
        return this;
    }

    @Override
    public MapReduceIterable<TResult> nonAtomic(final boolean nonAtomic) {
        this.nonAtomic = nonAtomic;
        return this;
    }

    @Override
    public MapReduceIterable<TResult> batchSize(final int batchSize) {
        super.batchSize(batchSize);
        return this;
    }

    @Override
    public MapReduceIterable<TResult> bypassDocumentValidation(@Nullable final Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    @Override
    public MapReduceIterable<TResult> collation(@Nullable final Collation collation) {
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

    @Override
    public ReadOperation<BatchCursor<TResult>> asReadOperation() {
        if (inline) {
            ReadOperation<MapReduceBatchCursor<TResult>> operation = operations.mapReduce(mapFunction, reduceFunction, finalizeFunction,
                    resultClass, filter, limit, maxTimeMS, jsMode, scope, sort, verbose, collation);
            return new WrappedMapReduceReadOperation<TResult>(operation);
        } else {
            getExecutor().execute(createMapReduceToCollectionOperation(), getClientSession());

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
                limit, maxTimeMS, jsMode, scope, sort, verbose, action, nonAtomic, sharded, bypassDocumentValidation, collation
        );
    }

    // this could be inlined, but giving it a name so that it's unit-testable
    static class WrappedMapReduceReadOperation<TResult> implements ReadOperation<BatchCursor<TResult>> {
        private final ReadOperation<MapReduceBatchCursor<TResult>> operation;

        ReadOperation<MapReduceBatchCursor<TResult>> getOperation() {
            return operation;
        }

        WrappedMapReduceReadOperation(final ReadOperation<MapReduceBatchCursor<TResult>> operation) {
            this.operation = operation;
        }

        @Override
        public BatchCursor<TResult> execute(final ReadBinding binding) {
            return operation.execute(binding);
        }
    }
}
