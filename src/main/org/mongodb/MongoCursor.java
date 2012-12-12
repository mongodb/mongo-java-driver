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

import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoKillCursor;
import org.mongodb.result.QueryResult;
import org.mongodb.serialization.serializers.MongoDocumentSerializer;

import java.io.Closeable;
import java.util.Iterator;

// TODO: Handle getmore
public class MongoCursor<T> implements Iterator<T>, Closeable {
    private final MongoCollection<T> collection;
    private final MongoFind find;
    private QueryResult<T> currentResult;
    private Iterator<T> currentIterator;

    public MongoCursor(final MongoCollection<T> collection, final MongoFind find) {
        this.collection = collection;
        this.find = find;
        currentResult = collection.getClient().getOperations().query(collection.getNamespace(), find,
                new MongoDocumentSerializer(collection.getBasePrimitiveSerializers()),
                        collection.getSerializer());
        currentIterator = currentResult.getResults().iterator();
    }

    @Override
    public void close() {
        if (currentResult != null && currentResult.getCursorId() != 0) {
            collection.getClient().getOperations().killCursors(new MongoKillCursor(currentResult.getCursorId()));
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
