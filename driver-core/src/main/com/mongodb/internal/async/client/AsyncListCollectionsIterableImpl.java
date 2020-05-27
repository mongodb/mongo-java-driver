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

package com.mongodb.internal.async.client;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.operation.AsyncOperations;
import com.mongodb.internal.operation.AsyncReadOperation;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

final class AsyncListCollectionsIterableImpl<TResult> extends AsyncMongoIterableImpl<TResult>
        implements AsyncListCollectionsIterable<TResult> {
    private AsyncOperations<BsonDocument> operations;
    private final String databaseName;
    private final Class<TResult> resultClass;

    private Bson filter;
    private boolean collectionNamesOnly;
    private long maxTimeMS;

    AsyncListCollectionsIterableImpl(@Nullable final AsyncClientSession clientSession, final String databaseName,
                                     final boolean collectionNamesOnly, final Class<TResult> resultClass, final CodecRegistry codecRegistry,
                                     final ReadPreference readPreference, final OperationExecutor executor, final boolean retryReads) {
        super(clientSession, executor, ReadConcern.DEFAULT, readPreference, retryReads); // TODO: read concern?
        this.collectionNamesOnly = collectionNamesOnly;
        this.operations = new AsyncOperations<BsonDocument>(BsonDocument.class, readPreference, codecRegistry, retryReads);
        this.databaseName = notNull("databaseName", databaseName);
        this.resultClass = notNull("resultClass", resultClass);
    }

    @Override
    public AsyncListCollectionsIterable<TResult> filter(@Nullable final Bson filter) {
        this.filter = filter;
        return this;
    }

    @Override
    public AsyncListCollectionsIterable<TResult> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    @Override
    public AsyncListCollectionsIterable<TResult> batchSize(final int batchSize) {
        super.batchSize(batchSize);
        return this;
    }

    @Override
    AsyncReadOperation<AsyncBatchCursor<TResult>> asAsyncReadOperation() {
        return operations.listCollections(databaseName, resultClass, filter, collectionNamesOnly, getBatchSize(), maxTimeMS);
    }

}
