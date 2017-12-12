/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import com.mongodb.operation.BatchCursor;
import com.mongodb.operation.ListIndexesOperation;
import com.mongodb.operation.ReadOperation;
import com.mongodb.session.ClientSession;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

final class ListIndexesIterableImpl<TResult> extends MongoIterableImpl<TResult> implements ListIndexesIterable<TResult> {
    private final MongoNamespace namespace;
    private final Class<TResult> resultClass;
    private final CodecRegistry codecRegistry;
    private long maxTimeMS;

    ListIndexesIterableImpl(final ClientSession clientSession, final MongoNamespace namespace, final Class<TResult> resultClass,
                            final CodecRegistry codecRegistry, final ReadPreference readPreference, final OperationExecutor executor) {
        super(clientSession, executor, ReadConcern.DEFAULT, readPreference);
        this.namespace = notNull("namespace", namespace);
        this.resultClass = notNull("resultClass", resultClass);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
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
        return new ListIndexesOperation<TResult>(namespace, codecRegistry.get(resultClass))
                       .batchSize(getBatchSize() == null ? 0 : getBatchSize())
                       .maxTime(maxTimeMS, MILLISECONDS);
    }
}
