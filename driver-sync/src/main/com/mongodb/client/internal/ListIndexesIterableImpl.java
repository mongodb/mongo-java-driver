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
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.internal.operation.SyncOperations;
import com.mongodb.lang.Nullable;
import com.mongodb.operation.BatchCursor;
import com.mongodb.operation.ReadOperation;
import com.mongodb.client.ClientSession;
import org.bson.BsonDocument;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class ListIndexesIterableImpl<TResult> extends MongoIterableImpl<TResult> implements ListIndexesIterable<TResult> {
    private final SyncOperations<BsonDocument> operations;
    private final Class<TResult> resultClass;
    private long maxTimeMS;

    ListIndexesIterableImpl(@Nullable final ClientSession clientSession, final MongoNamespace namespace, final Class<TResult> resultClass,
                            final CodecRegistry codecRegistry, final ReadPreference readPreference, final OperationExecutor executor) {
        this(clientSession, namespace, resultClass, codecRegistry, readPreference, executor, true);
    }

    ListIndexesIterableImpl(@Nullable final ClientSession clientSession, final MongoNamespace namespace, final Class<TResult> resultClass,
                            final CodecRegistry codecRegistry, final ReadPreference readPreference, final OperationExecutor executor,
                            final boolean retryReads) {
        super(clientSession, executor, ReadConcern.DEFAULT, readPreference, retryReads);
        this.operations = new SyncOperations<BsonDocument>(namespace, BsonDocument.class, readPreference, codecRegistry, retryReads);
        this.resultClass = notNull("resultClass", resultClass);
    }

    @Override
    public ListIndexesIterable<TResult> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    @Override
    public ListIndexesIterable<TResult> batchSize(final int batchSize) {
        super.batchSize(batchSize);
        return this;
    }

    @Override
    public ReadOperation<BatchCursor<TResult>> asReadOperation() {
        return operations.listIndexes(resultClass, getBatchSize(), maxTimeMS);
    }
}
