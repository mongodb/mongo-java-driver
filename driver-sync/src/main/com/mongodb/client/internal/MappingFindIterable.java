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

import com.mongodb.CursorType;
import com.mongodb.Function;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Collation;
import com.mongodb.lang.Nullable;
import org.bson.conversions.Bson;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

class MappingFindIterable<U, V> implements FindIterable<V> {

    private final FindIterable<U> iterable;
    private final Function<U, V> mapper;

    MappingFindIterable(final FindIterable<U> iterable, final Function<U, V> mapper) {
        this.iterable = iterable;
        this.mapper = mapper;
    }

    @Override
    public MongoCursor<V> iterator() {
        return new MongoMappingCursor<U, V>(iterable.iterator(), mapper);
    }

    @Override
    public MongoCursor<V> cursor() {
        return iterator();
    }

    @Nullable
    @Override
    public V first() {
        U first = iterable.first();
        if (first == null) {
            return null;
        }
        return mapper.apply(first);
    }

    @Override
    public void forEach(final Consumer<? super V> block) {
        iterable.forEach(new Consumer<U>() {
            @Override
            public void accept(final U document) {
                block.accept(mapper.apply(document));
            }
        });
    }

    @Override
    public <A extends Collection<? super V>> A into(final A target) {
        forEach(new Consumer<V>() {
            @Override
            public void accept(final V v) {
                target.add(v);
            }
        });
        return target;
    }

    @Override
    public MappingFindIterable<U, V> batchSize(final int batchSize) {
        iterable.batchSize(batchSize);
        return this;
    }

    @Override
    public FindIterable<V> filter(@Nullable final Bson filter) {
        iterable.filter(filter);
        return this;
    }

    @Override
    public FindIterable<V> limit(final int limit) {
        iterable.limit(limit);
        return this;
    }

    @Override
    public FindIterable<V> skip(final int skip) {
        iterable.skip(skip);
        return this;
    }

    @Override
    public FindIterable<V> maxTime(final long maxTime, final TimeUnit timeUnit) {
        iterable.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public FindIterable<V> maxAwaitTime(final long maxAwaitTime, final TimeUnit timeUnit) {
        iterable.maxAwaitTime(maxAwaitTime, timeUnit);
        return this;
    }

    @Override
    public FindIterable<V> projection(@Nullable final Bson projection) {
        iterable.projection(projection);
        return this;
    }

    @Override
    public FindIterable<V> sort(@Nullable final Bson sort) {
        iterable.sort(sort);
        return this;
    }

    @Override
    public FindIterable<V> noCursorTimeout(final boolean noCursorTimeout) {
        iterable.noCursorTimeout(noCursorTimeout);
        return this;
    }

    @Override
    @Deprecated
    public FindIterable<V> oplogReplay(final boolean oplogReplay) {
        iterable.oplogReplay(oplogReplay);
        return this;
    }

    @Override
    public FindIterable<V> partial(final boolean partial) {
        iterable.partial(partial);
        return this;
    }

    @Override
    public FindIterable<V> cursorType(final CursorType cursorType) {
        iterable.cursorType(cursorType);
        return this;
    }

    @Override
    public FindIterable<V> collation(@Nullable final Collation collation) {
        iterable.collation(collation);
        return this;
    }

    @Override
    public FindIterable<V> comment(@Nullable final String comment) {
        iterable.comment(comment);
        return this;
    }

    @Override
    public FindIterable<V> hint(@Nullable final Bson hint) {
        iterable.hint(hint);
        return this;
    }

    @Override
    public FindIterable<V> hintString(@Nullable final String hint) {
        iterable.hintString(hint);
        return this;
    }

    @Override
    public FindIterable<V> max(@Nullable final Bson max) {
        iterable.max(max);
        return this;
    }

    @Override
    public FindIterable<V> min(@Nullable final Bson min) {
        iterable.min(min);
        return this;
    }

    @Override
    public FindIterable<V> returnKey(final boolean returnKey) {
        iterable.returnKey(returnKey);
        return this;
    }

    @Override
    public FindIterable<V> showRecordId(final boolean showRecordId) {
        iterable.showRecordId(showRecordId);
        return this;
    }

    @Override
    public FindIterable<V> allowDiskUse(@Nullable final Boolean allowDiskUse) {
        iterable.allowDiskUse(allowDiskUse);
        return this;
    }

    @Override
    public <W> MongoIterable<W> map(final Function<V, W> newMap) {
        return new MappingFindIterable<V, W>(this, newMap);
    }

    MongoIterable<U> getMapped() {
        return iterable;
    }
}
