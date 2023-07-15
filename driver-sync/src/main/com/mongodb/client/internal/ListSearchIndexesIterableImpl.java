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

import com.mongodb.ExplainVerbosity;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.ListSearchIndexesIterable;
import com.mongodb.client.model.Collation;
import com.mongodb.internal.operation.BatchCursor;
import com.mongodb.internal.operation.ExplainableReadOperation;
import com.mongodb.internal.operation.ReadOperation;
import com.mongodb.internal.operation.SyncOperations;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;

final class ListSearchIndexesIterableImpl<TResult> extends MongoIterableImpl<TResult> implements ListSearchIndexesIterable<TResult> {
    private final SyncOperations<BsonDocument> operations;
    private final Class<TResult> resultClass;
    @Nullable
    private Boolean allowDiskUse;
    @Nullable
    private long maxTimeMS;
    @Nullable
    private Collation collation;
    @Nullable
    private BsonValue comment;
    @Nullable
    private String indexName;
    private final CodecRegistry codecRegistry;

    ListSearchIndexesIterableImpl(final MongoNamespace namespace, final OperationExecutor executor,
                                         final ReadConcern readConcern, final Class<TResult> resultClass,
                                         final CodecRegistry codecRegistry, final ReadPreference readPreference,
                                         final boolean retryReads) {
        super(null, executor, readConcern, readPreference, retryReads);

        this.resultClass = resultClass;
        this.operations = new SyncOperations<>(namespace, BsonDocument.class, readPreference, codecRegistry, retryReads);
        this.codecRegistry = codecRegistry;
    }

    @Override
    public ReadOperation<BatchCursor<TResult>> asReadOperation() {
        return asAggregateOperation();
    }


    @Override
    public ListSearchIndexesIterable<TResult> allowDiskUse(@Nullable final Boolean allowDiskUse) {
        this.allowDiskUse = allowDiskUse;
        return this;
    }

    @Override
    public ListSearchIndexesIterable<TResult> batchSize(final int batchSize) {
        super.batchSize(batchSize);
        return this;
    }

    @Override
    public ListSearchIndexesIterable<TResult> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    @Override
    public ListSearchIndexesIterable<TResult> collation(@Nullable final Collation collation) {
        this.collation = collation;
        return this;
    }

    @Override
    public ListSearchIndexesIterable<TResult> comment(@Nullable final String comment) {
        this.comment = comment == null ? null : new BsonString(comment);
        return this;
    }

    @Override
    public ListSearchIndexesIterable<TResult> comment(@Nullable final BsonValue comment) {
        this.comment = comment;
        return this;
    }

    @Override
    public ListSearchIndexesIterable<TResult> name(@Nullable final String indexName) {
        this.indexName = notNull("indexName", indexName);
        return this;
    }

    @Override
    public Document explain() {
        return executeExplain(Document.class, null);
    }

    @Override
    public Document explain(final ExplainVerbosity verbosity) {
        notNull("verbosity", verbosity);
        return executeExplain(Document.class, verbosity);
    }

    @Override
    public <E> E explain(final Class<E> explainResultClass) {
        notNull("explainResultClass", explainResultClass);
        return executeExplain(explainResultClass, null);
    }

    @Override
    public <E> E explain(final Class<E> explainResultClass, final ExplainVerbosity verbosity) {
        notNull("verbosity", verbosity);
        return executeExplain(explainResultClass, verbosity);
    }

    private <E> E executeExplain(final Class<E> explainResultClass, @Nullable final ExplainVerbosity verbosity) {
        return getExecutor().execute(asAggregateOperation().asExplainableOperation(verbosity, codecRegistry.get(explainResultClass)),
                getReadPreference(), getReadConcern(), getClientSession());
    }

    private ExplainableReadOperation<BatchCursor<TResult>> asAggregateOperation() {
        return operations.listSearchIndexes(resultClass, maxTimeMS, indexName, getBatchSize(), collation, comment,
                allowDiskUse);
    }
}
