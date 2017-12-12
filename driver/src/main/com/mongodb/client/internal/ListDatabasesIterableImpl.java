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

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.ListDatabasesIterable;
import com.mongodb.operation.BatchCursor;
import com.mongodb.operation.ListDatabasesOperation;
import com.mongodb.operation.ReadOperation;
import com.mongodb.session.ClientSession;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;


public final class ListDatabasesIterableImpl<TResult> extends MongoIterableImpl<TResult> implements ListDatabasesIterable<TResult> {
    private final Class<TResult> resultClass;
    private final CodecRegistry codecRegistry;

    private long maxTimeMS;
    private Bson filter;
    private Boolean nameOnly;

    public ListDatabasesIterableImpl(final ClientSession clientSession, final Class<TResult> resultClass, final CodecRegistry codecRegistry,
                              final ReadPreference readPreference, final OperationExecutor executor) {
        super(clientSession, executor, ReadConcern.DEFAULT, readPreference); // TODO: read concern?
        this.resultClass = notNull("clazz", resultClass);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
    }

    @Override
    public ListDatabasesIterableImpl<TResult> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    @Override
    public ListDatabasesIterable<TResult> batchSize(final int batchSize) {
        super.batchSize(batchSize);
        return this;
    }

    @Override
    public ListDatabasesIterable<TResult> filter(final Bson filter) {
        this.filter = filter;
        return this;
    }

    @Override
    public ListDatabasesIterable<TResult> nameOnly(final Boolean nameOnly) {
        this.nameOnly = nameOnly;
        return this;
    }

    @Override
    public ReadOperation<BatchCursor<TResult>> asReadOperation() {
        return new ListDatabasesOperation<TResult>(codecRegistry.get(resultClass)).maxTime(maxTimeMS, MILLISECONDS)
                .filter(toBsonDocumentOrNull(filter, codecRegistry)).nameOnly(nameOnly);
    }
}
