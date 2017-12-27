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
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.FindOptions;
import com.mongodb.internal.operation.SyncOperations;
import com.mongodb.lang.Nullable;
import com.mongodb.operation.BatchCursor;
import com.mongodb.operation.ReadOperation;
import com.mongodb.session.ClientSession;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;

class AggregateIterableImpl<TDocument, TResult> extends MongoIterableImpl<TResult> implements AggregateIterable<TResult> {
    private final SyncOperations<TDocument> operations;
    private final MongoNamespace namespace;
    private final Class<TDocument> documentClass;
    private final Class<TResult> resultClass;
    private final CodecRegistry codecRegistry;
    private final List<? extends Bson> pipeline;

    private Boolean allowDiskUse;
    private long maxTimeMS;
    private long maxAwaitTimeMS;
    private Boolean useCursor;
    private Boolean bypassDocumentValidation;
    private Collation collation;
    private String comment;
    private Bson hint;

    AggregateIterableImpl(final ClientSession clientSession, final MongoNamespace namespace, final Class<TDocument> documentClass,
                          final Class<TResult> resultClass, final CodecRegistry codecRegistry, final ReadPreference readPreference,
                          final ReadConcern readConcern, final WriteConcern writeConcern, final OperationExecutor executor,
                          final List<? extends Bson> pipeline) {
        super(clientSession, executor, readConcern, readPreference);
        this.operations = new SyncOperations<TDocument>(namespace, documentClass, readPreference, codecRegistry, writeConcern, false,
                readConcern);
        this.namespace = notNull("namespace", namespace);
        this.documentClass = notNull("documentClass", documentClass);
        this.resultClass = notNull("resultClass", resultClass);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.pipeline = notNull("pipeline", pipeline);
    }

    @Override
    public void toCollection() {

        if (getOutCollection() == null) {
            throw new IllegalStateException("The last stage of the aggregation pipeline must be $out");
        }

        getExecutor().execute(operations.aggregateToCollection(pipeline, maxTimeMS, allowDiskUse, bypassDocumentValidation, collation, hint,
                comment), getClientSession());
    }

    @Override
    public AggregateIterable<TResult> allowDiskUse(@Nullable final Boolean allowDiskUse) {
        this.allowDiskUse = allowDiskUse;
        return this;
    }

    @Override
    public AggregateIterable<TResult> batchSize(final int batchSize) {
        super.batchSize(batchSize);
        return this;
    }

    @Override
    public AggregateIterable<TResult> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    @Override
    @Deprecated
    public AggregateIterable<TResult> useCursor(@Nullable final Boolean useCursor) {
        this.useCursor = useCursor;
        return this;
    }

    @Override
    public AggregateIterable<TResult> maxAwaitTime(final long maxAwaitTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxAwaitTimeMS = TimeUnit.MILLISECONDS.convert(maxAwaitTime, timeUnit);
        return this;
    }

    @Override
    public AggregateIterable<TResult> bypassDocumentValidation(final Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    @Override
    public AggregateIterable<TResult> collation(final Collation collation) {
        this.collation = collation;
        return this;
    }

    @Override
    public AggregateIterable<TResult> comment(@Nullable final String comment) {
        this.comment = comment;
        return this;
    }

    @Override
    public AggregateIterable<TResult> hint(@Nullable final Bson hint) {
        this.hint = hint;
        return this;
    }

    @Override
    @SuppressWarnings("deprecation")
    public ReadOperation<BatchCursor<TResult>> asReadOperation() {
        BsonValue outCollection = getOutCollection();

        if (outCollection != null) {
            getExecutor().execute(operations.aggregateToCollection(pipeline, maxTimeMS, allowDiskUse, bypassDocumentValidation, collation,
                    hint, comment), getClientSession());

            FindOptions findOptions = new FindOptions().collation(collation);
            if (getBatchSize() != null) {
                findOptions.batchSize(getBatchSize());
            }
            return operations.find(new MongoNamespace(namespace.getDatabaseName(), outCollection.asString().getValue()), new BsonDocument(),
                    resultClass, findOptions);
        } else {
            return operations.aggregate(pipeline, resultClass, maxTimeMS, maxAwaitTimeMS, getBatchSize(), collation,
                    hint, comment, allowDiskUse, useCursor);
        }
    }

    private BsonValue getOutCollection() {
        if (pipeline.size() == 0) {
            return null;
        }

        Bson lastStage = notNull("last stage", pipeline.get(pipeline.size() - 1));
        return lastStage.toBsonDocument(documentClass, codecRegistry).get("$out");
    }
}
