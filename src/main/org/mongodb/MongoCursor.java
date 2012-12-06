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

package org.mongodb;

import java.io.Closeable;
import java.util.Iterator;

public class MongoCursor<T> implements Iterator<T>, Closeable {
    private final MongoClient mongoClient;
    private final MongoCollectionName namespace;
    private final MongoDocument query;
    private final MongoDocument fields;
    private final Class<T> clazz;
    QueryResult<T> currentResult;
    Iterator<T> currentIterator;

    public MongoCursor(final MongoClient mongoClient, final MongoCollectionName namespace, final MongoDocument query,
                       final MongoDocument fields, Class<T> clazz) {
        this.mongoClient = mongoClient;
        this.namespace = namespace;
        this.query = query;
        this.fields = fields;
        this.clazz = clazz;
        currentResult = mongoClient.getOperations().query(namespace, query, clazz);
        currentIterator = currentResult.getResults().iterator();
    }

    @Override
    public void close() {
        if (currentResult != null && currentResult.getCursorId() != 0) {
            mongoClient.getOperations().killCursors(currentResult.getCursorId());
        }
    }

    @Override
    public boolean hasNext() {
        // TODO: Handle getMore
        return currentIterator.hasNext();
    }

    @Override
    public T next() {
        // TODO: Handle getMore
        return currentIterator.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("MongoCursor does not support remove");
    }
}
