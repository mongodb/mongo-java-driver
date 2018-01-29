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

package org.bson;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * A {@code LazyBSONObject} representing a BSON array.
 */
@SuppressWarnings("rawtypes")
public class LazyBSONList extends LazyBSONObject implements List {

    /**
     * Construct an instance with the given raw bytes and offset.
     *
     * @param bytes the raw BSON bytes
     * @param callback the callback to use to create nested values
     */
    public LazyBSONList(final byte[] bytes, final LazyBSONCallback callback) {
        super(bytes, callback);
    }

    /**
     * Construct an instance with the given raw bytes and offset.
     *
     * @param bytes the raw BSON bytes
     * @param offset the offset into the raw bytes
     * @param callback the callback to use to create nested values
     */
    public LazyBSONList(final byte[] bytes, final int offset, final LazyBSONCallback callback) {
        super(bytes, offset, callback);
    }

    @Override
    public int size() {
        return keySet().size();
    }

    @Override
    public boolean contains(final Object o) {
        return indexOf(o) > -1;
    }

    @Override
    public Iterator iterator() {
        return new LazyBSONListIterator();
    }

    @Override
    public boolean containsAll(final Collection collection) {
        Set<Object> values = new HashSet<Object>();
        for (final Object o : this) {
            values.add(o);
        }
        return values.containsAll(collection);
    }

    @Override
    public Object get(final int index) {
        return get(String.valueOf(index));
    }

    @Override
    public int indexOf(final Object o) {
        Iterator it = iterator();
        for (int pos = 0; it.hasNext(); pos++) {
            if (o.equals(it.next())) {
                return pos;
            }
        }
        return -1;
    }

    @Override
    public int lastIndexOf(final Object o) {
        int lastFound = -1;
        Iterator it = iterator();

        for (int pos = 0; it.hasNext(); pos++) {
            if (o.equals(it.next())) {
                lastFound = pos;
            }
        }

        return lastFound;
    }

    /**
     * An iterator over the values in a LazyBsonList.
     */
    public class LazyBSONListIterator implements Iterator {
        private final BsonBinaryReader reader;
        private BsonType cachedBsonType;

        /**
         * Construct an instance
         */
        public LazyBSONListIterator() {
            reader = getBsonReader();
            reader.readStartDocument();
        }

        @Override
        public boolean hasNext() {
            if (cachedBsonType == null) {
                cachedBsonType = reader.readBsonType();
            }
            return cachedBsonType != BsonType.END_OF_DOCUMENT;
        }

        @Override
        public Object next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            } else {
                cachedBsonType = null;
                reader.readName();
                return readValue(reader);
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Operation is not supported");
        }

    }

    /* ----------------- Unsupported operations --------------------- */

    @Override
    public ListIterator listIterator() {
        throw new UnsupportedOperationException("Operation is not supported instance of this type");
    }

    @Override
    public ListIterator listIterator(final int index) {
        throw new UnsupportedOperationException("Operation is not supported instance of this type");
    }

    @Override
    public boolean add(final Object o) {
        throw new UnsupportedOperationException("Object is read only");
    }

    @Override
    public boolean remove(final Object o) {
        throw new UnsupportedOperationException("Object is read only");
    }

    @Override
    public boolean addAll(final Collection c) {
        throw new UnsupportedOperationException("Object is read only");
    }

    @Override
    public boolean addAll(final int index, final Collection c) {
        throw new UnsupportedOperationException("Object is read only");
    }

    @Override
    public boolean removeAll(final Collection c) {
        throw new UnsupportedOperationException("Object is read only");
    }

    @Override
    public boolean retainAll(final Collection c) {
        throw new UnsupportedOperationException("Object is read only");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("Object is read only");
    }

    @Override
    public Object set(final int index, final Object element) {
        throw new UnsupportedOperationException("Object is read only");
    }

    @Override
    public void add(final int index, final Object element) {
        throw new UnsupportedOperationException("Object is read only");
    }

    @Override
    public Object remove(final int index) {
        throw new UnsupportedOperationException("Object is read only");
    }

    @Override
    public List subList(final int fromIndex, final int toIndex) {
        throw new UnsupportedOperationException("Operation is not supported");
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException("Operation is not supported");
    }

    @Override
    public Object[] toArray(final Object[] a) {
        throw new UnsupportedOperationException("Operation is not supported");
    }
}
