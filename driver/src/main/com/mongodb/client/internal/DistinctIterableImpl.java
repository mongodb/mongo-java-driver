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
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.model.Collation;
import com.mongodb.operation.BatchCursor;
import com.mongodb.operation.DistinctOperation;
import com.mongodb.operation.ReadOperation;
import com.mongodb.session.ClientSession;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class DistinctIterableImpl<TDocument, TResult> extends MongoIterableImpl<TResult> implements DistinctIterable<TResult> {
    private final MongoNamespace namespace;
    private final Class<TDocument> documentClass;
    private final Class<TResult> resultClass;
    private final CodecRegistry codecRegistry;
    private final String fieldName;

    private Bson filter;
    private long maxTimeMS;
    private Collation collation;


    DistinctIterableImpl(final ClientSession clientSession, final MongoNamespace namespace, final Class<TDocument> documentClass,
                         final Class<TResult> resultClass, final CodecRegistry codecRegistry, final ReadPreference readPreference,
                         final ReadConcern readConcern, final OperationExecutor executor, final String fieldName, final Bson filter) {
        super(clientSession, executor, readConcern, readPreference);
        this.namespace = notNull("namespace", namespace);
        this.documentClass = notNull("documentClass", documentClass);
        this.resultClass = notNull("resultClass", resultClass);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.fieldName = notNull("mapFunction", fieldName);
        this.filter = filter;
    }

    @Override
    public DistinctIterable<TResult> filter(final Bson filter) {
        this.filter = filter;
        return this;
    }

    @Override
    public DistinctIterable<TResult> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    @Override
    public DistinctIterable<TResult> batchSize(final int batchSize) {
        super.batchSize(batchSize);
        return this;
    }

    @Override
    public DistinctIterable<TResult> collation(final Collation collation) {
        this.collation = collation;
        return this;
    }

    @Override
    public ReadOperation<BatchCursor<TResult>> asReadOperation() {
        return new DistinctOperation<TResult>(namespace, fieldName, codecRegistry.get(resultClass))
                       .filter(filter == null ? null : filter.toBsonDocument(documentClass, codecRegistry))
                       .maxTime(maxTimeMS, MILLISECONDS)
                       .readConcern(getReadConcern())
                       .collation(collation);
    }
}
