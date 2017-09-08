/*
 * Copyright 2015 MongoDB, Inc.
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

package com.mongodb;

import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.operation.BatchCursor;
import com.mongodb.operation.ListCollectionsOperation;
import com.mongodb.operation.ReadOperation;
import org.bson.BsonDocument;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

final class ListCollectionsIterableImpl<TResult> extends MongoIterableImpl<TResult> implements ListCollectionsIterable<TResult> {
    private final String databaseName;
    private final Class<TResult> resultClass;
    private final CodecRegistry codecRegistry;

    private Bson filter;
    private long maxTimeMS;

    ListCollectionsIterableImpl(final String databaseName, final Class<TResult> resultClass, final CodecRegistry codecRegistry,
                                final ReadPreference readPreference, final OperationExecutor executor) {
        super(executor, ReadConcern.DEFAULT, readPreference); // TODO: read concern?
        this.databaseName = notNull("databaseName", databaseName);
        this.resultClass = notNull("resultClass", resultClass);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
    }

    @Override
    public ListCollectionsIterable<TResult> filter(final Bson filter) {
        notNull("filter", filter);
        this.filter = filter;
        return this;
    }

    @Override
    public ListCollectionsIterable<TResult> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    @Override
    public ListCollectionsIterable<TResult> batchSize(final int batchSize) {
        super.batchSize(batchSize);
        return this;
    }

    @Override
    ReadOperation<BatchCursor<TResult>> asReadOperation() {
        return new ListCollectionsOperation<TResult>(databaseName, codecRegistry.get(resultClass))
                       .filter(toBsonDocument(filter))
                       .batchSize(getBatchSize() == null ? 0 : getBatchSize())
                       .maxTime(maxTimeMS, MILLISECONDS);
    }

    private BsonDocument toBsonDocument(final Bson document) {
        return document == null ? null : document.toBsonDocument(BsonDocument.class, codecRegistry);
    }
}
