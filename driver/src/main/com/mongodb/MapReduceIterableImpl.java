/*
 * Copyright 2015-2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb;

import com.mongodb.binding.ReadBinding;
import com.mongodb.client.MapReduceIterable;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.MapReduceAction;
import com.mongodb.operation.BatchCursor;
import com.mongodb.operation.FindOperation;
import com.mongodb.operation.MapReduceBatchCursor;
import com.mongodb.operation.MapReduceToCollectionOperation;
import com.mongodb.operation.MapReduceWithInlineResultsOperation;
import com.mongodb.operation.ReadOperation;
import org.bson.BsonDocument;
import org.bson.BsonJavaScript;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class MapReduceIterableImpl<TDocument, TResult> extends MongoIterableImpl<TResult> implements MapReduceIterable<TResult> {
    private final MongoNamespace namespace;
    private final Class<TDocument> documentClass;
    private final Class<TResult> resultClass;
    private final CodecRegistry codecRegistry;
    private final WriteConcern writeConcern;
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

    MapReduceIterableImpl(final MongoNamespace namespace, final Class<TDocument> documentClass, final Class<TResult> resultClass,
                          final CodecRegistry codecRegistry, final ReadPreference readPreference, final ReadConcern readConcern,
                          final WriteConcern writeConcern, final OperationExecutor executor, final String mapFunction,
                          final String reduceFunction) {
        super(executor, readConcern, readPreference);
        this.namespace = notNull("namespace", namespace);
        this.documentClass = notNull("documentClass", documentClass);
        this.resultClass = notNull("resultClass", resultClass);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.writeConcern = notNull("writeConcern", writeConcern);
        this.mapFunction = notNull("mapFunction", mapFunction);
        this.reduceFunction = notNull("reduceFunction", reduceFunction);
    }

    @Override
    public void toCollection() {
        if (inline) {
            throw new IllegalStateException("The options must specify a non-inline result");
        }

        getExecutor().execute(createMapReduceToCollectionOperation());
    }

    @Override
    public MapReduceIterable<TResult> collectionName(final String collectionName) {
        this.collectionName = notNull("collectionName", collectionName);
        this.inline = false;
        return this;
    }

    @Override
    public MapReduceIterable<TResult> finalizeFunction(final String finalizeFunction) {
        this.finalizeFunction = finalizeFunction;
        return this;
    }

    @Override
    public MapReduceIterable<TResult> scope(final Bson scope) {
        this.scope = scope;
        return this;
    }

    @Override
    public MapReduceIterable<TResult> sort(final Bson sort) {
        this.sort = sort;
        return this;
    }

    @Override
    public MapReduceIterable<TResult> filter(final Bson filter) {
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
    public MapReduceIterable<TResult> databaseName(final String databaseName) {
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
    public MapReduceIterable<TResult> bypassDocumentValidation(final Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    @Override
    public MapReduceIterable<TResult> collation(final Collation collation) {
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
    ReadOperation<BatchCursor<TResult>> asReadOperation() {
        if (inline) {
            final MapReduceWithInlineResultsOperation<TResult> operation =
                    new MapReduceWithInlineResultsOperation<TResult>(namespace,
                                                                            new BsonJavaScript(mapFunction),
                                                                            new BsonJavaScript(reduceFunction),
                                                                            codecRegistry.get(resultClass))
                            .filter(toBsonDocument(filter))
                            .limit(limit)
                            .maxTime(maxTimeMS, MILLISECONDS)
                            .jsMode(jsMode)
                            .scope(toBsonDocument(scope))
                            .sort(toBsonDocument(sort))
                            .verbose(verbose)
                            .readConcern(getReadConcern())
                            .collation(collation);
            if (finalizeFunction != null) {
                operation.finalizeFunction(new BsonJavaScript(finalizeFunction));
            }
            return new WrappedMapReduceReadOperation<TResult>(operation);
        } else {
            getExecutor().execute(createMapReduceToCollectionOperation());

            String dbName = databaseName != null ? databaseName : namespace.getDatabaseName();
            return new FindOperation<TResult>(new MongoNamespace(dbName, collectionName), codecRegistry.get(resultClass))
                    .collation(collation)
                    .batchSize(getBatchSize() == null ? 0 : getBatchSize());
        }

    }

    private MapReduceToCollectionOperation createMapReduceToCollectionOperation() {
        MapReduceToCollectionOperation operation =
                new MapReduceToCollectionOperation(namespace, new BsonJavaScript(mapFunction), new BsonJavaScript(reduceFunction),
                        collectionName, writeConcern)
                        .filter(toBsonDocument(filter))
                        .limit(limit)
                        .maxTime(maxTimeMS, MILLISECONDS)
                        .jsMode(jsMode)
                        .scope(toBsonDocument(scope))
                        .sort(toBsonDocument(sort))
                        .verbose(verbose)
                        .action(action.getValue())
                        .nonAtomic(nonAtomic)
                        .sharded(sharded)
                        .databaseName(databaseName)
                        .bypassDocumentValidation(bypassDocumentValidation)
                        .collation(collation);

        if (finalizeFunction != null) {
            operation.finalizeFunction(new BsonJavaScript(finalizeFunction));
        }
        return operation;
    }

    private BsonDocument toBsonDocument(final Bson document) {
        return document == null ? null : document.toBsonDocument(documentClass, codecRegistry);
    }

    // this could be inlined, but giving it a name so that it's unit-testable
    static class WrappedMapReduceReadOperation<TResult> implements ReadOperation<BatchCursor<TResult>> {
        private final ReadOperation<MapReduceBatchCursor<TResult>> operation;

        ReadOperation<MapReduceBatchCursor<TResult>> getOperation() {
            return operation;
        }

        WrappedMapReduceReadOperation(final MapReduceWithInlineResultsOperation<TResult> operation) {
            this.operation = operation;
        }

        @Override
        public BatchCursor<TResult> execute(final ReadBinding binding) {
            return operation.execute(binding);
        }
    }
}
