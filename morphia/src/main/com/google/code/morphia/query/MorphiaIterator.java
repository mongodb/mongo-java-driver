/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package com.google.code.morphia.query;

import com.google.code.morphia.mapping.Mapper;
import com.google.code.morphia.mapping.cache.EntityCache;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Scott Hernandez
 */
@SuppressWarnings("unchecked")
public class MorphiaIterator<T, V> implements Iterable<V>, Iterator<V> {
    protected final Iterator<DBObject> wrapped;
    protected final Mapper m;
    protected final Class<T> clazz;
    protected final String kind;
    protected final EntityCache cache;
    protected long driverTime = 0;
    protected long mapperTime = 0;

    public MorphiaIterator(final Iterator<DBObject> it, final Mapper m, final Class<T> clazz, final String kind,
                           final EntityCache cache) {
        this.wrapped = it;
        this.m = m;
        this.clazz = clazz;
        this.kind = kind;
        this.cache = cache;
    }

    public Iterator<V> iterator() {
        return this;
    }

    public boolean hasNext() {
        if (wrapped == null) {
            return false;
        }
        final long start = System.currentTimeMillis();
        final boolean ret = wrapped.hasNext();
        driverTime += System.currentTimeMillis() - start;
        return ret;
    }

    public V next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        final DBObject dbObj = getNext();
        return processItem(dbObj);
    }

    protected V processItem(final DBObject dbObj) {
        final long start = System.currentTimeMillis();
        final V item = convertItem(dbObj);
        mapperTime += System.currentTimeMillis() - start;
        return item;
    }

    protected DBObject getNext() {
        final long start = System.currentTimeMillis();
        final DBObject dbObj = wrapped.next();
        driverTime += System.currentTimeMillis() - start;
        return dbObj;
    }

    protected V convertItem(final DBObject dbObj) {
        return (V) m.fromDBObject(clazz, dbObj, cache);
    }

    public void remove() {
        final long start = System.currentTimeMillis();
        wrapped.remove();
        driverTime += System.currentTimeMillis() - start;
    }

    /**
     * Returns the time spent calling the driver in ms
     */
    public long getDriverTime() {
        return driverTime;
    }

    /**
     * Returns the time spent calling the mapper in ms
     */
    public long getMapperTime() {
        return mapperTime;
    }

    public DBCursor getCursor() {
        return (DBCursor) wrapped;
    }

    public void close() {
        if (wrapped != null && wrapped instanceof DBCursor) {
            ((DBCursor) wrapped).close();
        }
    }
}