/**
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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
 *
 */

package org.mongodb.impl;

import org.mongodb.MongoCollection;
import org.mongodb.MongoCollectionName;
import org.mongodb.MongoCursor;
import org.mongodb.MongoDocument;
import org.mongodb.MongoQuery;
import org.mongodb.MongoQueryFilter;
import org.mongodb.ReadPreference;

public class MongoQueryImpl<T> implements MongoQuery<T> {
    private final MongoCollection<T> collection;
    private final MongoQueryFilter filter;
    private final Class<T> clazz;

    public MongoQueryImpl(final MongoCollection<T> collection, final MongoQueryFilter filter, Class<T> clazz) {
        this.collection = collection;
        this.filter = filter;
        this.clazz = clazz;
    }

    @Override
    public MongoQuery order(final String condition) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MongoQuery limit(final int value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MongoQuery batchSize(final int value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MongoQuery offset(final int value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MongoQuery hintIndex(final String idxName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MongoQuery retrievedFields(final boolean include, final String... fields) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MongoQuery enableSnapshotMode() {
        throw new UnsupportedOperationException();
    }

    @Override
    public MongoQuery disableSnapshotMode() {
        throw new UnsupportedOperationException();
    }

    @Override
    public MongoQuery readPreference(final ReadPreference readPreference) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MongoQuery disableTimeout() {
        throw new UnsupportedOperationException();
    }

    @Override
    public MongoQuery enableTimeout() {
        throw new UnsupportedOperationException();
    }

    @Override
    public MongoCursor<T> entries() {
        return new MongoCursor<T>(collection.getDatabase().getClient(),
                new MongoCollectionName(collection.getDatabase().getName(), collection.getName()),
                filter.asDocument(), new MongoDocument(), clazz);
    }
}
