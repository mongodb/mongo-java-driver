/*
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
 */

package com.mongodb;

import org.mongodb.FieldSelectorDocument;
import org.mongodb.annotations.NotThreadSafe;
import org.mongodb.MongoCollection;
import org.mongodb.MongoCursor;
import org.mongodb.QueryFilterDocument;
import org.mongodb.operation.MongoFind;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;

@NotThreadSafe
public class DBCursor implements Iterator<DBObject>, Iterable<DBObject>, Closeable {
    private final MongoCollection<DBObject> collection;
    private MongoCursor<DBObject> cursor;
    private final MongoFind find = new MongoFind();

    public DBCursor(final MongoCollection<DBObject> collection, final QueryFilterDocument filter,
                    final FieldSelectorDocument selector, final ReadPreference readPreference) {
        this.collection = collection;
        find.where(filter).select(selector).readPreference(readPreference.toNew());
    }

    public DBCursor limit(final int limit) {
        find.limit(limit);
        return this;
    }

    public DBCursor batchSize(final int batchSize) {
        find.batchSize(batchSize);
        return this;
    }

    public DBCursor skip(final int skip) {
        find.skip(skip);
        return this;
    }

    public boolean hasNext() {
        if (cursor == null) {
            getCursor();
        }
        return cursor.hasNext();
    }

    private void getCursor() {
        try {
            cursor = collection.filter(find.getFilter()).select(find.getFields()).sort(find.getOrder())
                    .skip(find.getSkip()).limit(find.getLimit()).batchSize(find.getBatchSize()).readPreference(find.getReadPreference()).find();
        } catch (org.mongodb.MongoException e) {
            throw new MongoException(e);
        }
    }

    public DBObject next() {
        if (cursor == null) {
            getCursor();
        }
        return cursor.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }


    /**
     * adds a query option - see Bytes.QUERYOPTION_* for list
     * @param option
     * @return
     */
    public DBCursor addOption(final int option) {
        throw new UnsupportedOperationException();    // TODO
    }

    public DBCursor snapshot() {
        return this;
    }

    public DBCursor sort(final DBObject sort) {
        find.order(DBObjects.toSortCriteriaDocument(sort));
        return this;
    }

    /**
     * @param hint
     */
    public DBCursor hint(final String hint) {
        throw new UnsupportedOperationException();  // TODO
    }

    public void setReadPreference(final ReadPreference readPreference) {
        throw new UnsupportedOperationException();      // TODO
    }

    public DBObject getQuery() {
        return DBObjects.toDBObject(find.getFilter().toDocument());
    }

    @Override
    public void close() {
        if (cursor != null) {
            cursor.close();
            cursor = null;
        }
    }

    @Override
    public Iterator<DBObject> iterator() {
        throw new UnsupportedOperationException();
    }

    /**
     * Converts this cursor to an array.
     *
     * @return an array of elements
     * @throws MongoException
     */
    public List<DBObject> toArray() {
        return toArray(Integer.MAX_VALUE);
    }

    /**
     * Converts this cursor to an array.
     *
     * @param max the maximum number of objects to return
     * @return an array of objects
     * @throws MongoException
     */
    public List<DBObject> toArray(int max) {
        throw new UnsupportedOperationException();
    }

    /**
     * Counts the number of objects matching the query
     * This does not take limit/skip into consideration
     * @see DBCursor#size
     * @return the number of objects
     * @throws MongoException
     */
    public int count() {
        return (int) collection.filter(find.getFilter()).readPreference(find.getReadPreference()).count();  // TODO: dangerous cast.  Throw exception instead?
    }
}
