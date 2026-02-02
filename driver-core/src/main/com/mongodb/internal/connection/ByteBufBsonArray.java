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

package com.mongodb.internal.connection;

import org.bson.BsonArray;
import org.bson.BsonBinaryReader;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.ByteBuf;
import org.bson.io.ByteBufferBsonInput;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Objects;

import static com.mongodb.internal.connection.ByteBufBsonHelper.readBsonValue;

final class ByteBufBsonArray extends BsonArray implements Closeable {
    private final ByteBuf byteBuf;

    /**
     * List of resources that need to be closed when this array is closed.
     * Tracks the main ByteBuf and iterator duplicates. Iterator buffers are automatically
     * removed and released when iteration completes normally to prevent memory accumulation.
     */
    private final List<Closeable> trackedResources = new ArrayList<>();
    private boolean closed;

    ByteBufBsonArray(final ByteBuf byteBuf) {
        this.byteBuf = byteBuf;
        trackedResources.add(byteBuf::release);
    }

    @Override
    public Iterator<BsonValue> iterator() {
        ensureOpen();
        return new ByteBufBsonArrayIterator();
    }

    @Override
    public List<BsonValue> getValues() {
        ensureOpen();
        List<BsonValue> values = new ArrayList<>();
        for (BsonValue cur: this) {
            //noinspection UseBulkOperation
            values.add(cur);
        }
        return values;
    }

    private static final String READ_ONLY_MESSAGE = "This BsonArray instance is read-only";

    @Override
    public int size() {
        ensureOpen();
        int size = 0;
        for (BsonValue ignored : this) {
            size++;
        }
        return size;
    }

    @Override
    public boolean isEmpty() {
        ensureOpen();
        return !iterator().hasNext();
    }

    @Override
    public boolean equals(final Object o) {
        ensureOpen();
        if (o == this) {
            return true;
        }
        if (!(o instanceof List)) {
            return false;
        }
        Iterator<BsonValue> e1 = iterator();
        Iterator<?> e2 = ((List<?>) o).iterator();
        while (e1.hasNext() && e2.hasNext()) {
            if (!(Objects.equals(e1.next(), e2.next()))) {
                return false;
            }
        }
        return !(e1.hasNext() || e2.hasNext());
    }

    @Override
    public int hashCode() {
        ensureOpen();
        int hashCode = 1;
        for (BsonValue cur : this) {
            hashCode = 31 * hashCode + (cur == null ? 0 : cur.hashCode());
        }
        return hashCode;
    }

    @Override
    public boolean contains(final Object o) {
        ensureOpen();
        for (BsonValue cur : this) {
            if (Objects.equals(o, cur)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public Object[] toArray() {
        ensureOpen();
        Object[] retVal = new Object[size()];
        Iterator<BsonValue> it = iterator();
        for (int i = 0; i < retVal.length; i++) {
            retVal[i] = it.next();
        }
        return retVal;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T[] toArray(final T[] a) {
        ensureOpen();
        int size = size();
        T[] retVal = a.length >= size ? a : (T[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), size);
        Iterator<BsonValue> it = iterator();
        for (int i = 0; i < retVal.length; i++) {
            retVal[i] = (T) it.next();
        }
        return retVal;
    }

    @Override
    public boolean containsAll(final Collection<?> c) {
        ensureOpen();
        for (Object e : c) {
            if (!contains(e)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public BsonValue get(final int index) {
        ensureOpen();
        if (index < 0) {
            throw new IndexOutOfBoundsException("Index out of range: " + index);
        }

        int i = 0;
        for (BsonValue cur : this) {
            if (i++ == index) {
                return cur;
            }
        }

        throw new IndexOutOfBoundsException("Index out of range: " + index);
    }

    @Override
    public int indexOf(final Object o) {
        ensureOpen();
        int i = 0;
        for (BsonValue cur : this) {
            if (Objects.equals(o, cur)) {
                return i;
            }
            i++;
        }

        return -1;
    }

    @Override
    public int lastIndexOf(final Object o) {
        ensureOpen();
        ListIterator<BsonValue> listIterator = listIterator(size());
        while (listIterator.hasPrevious()) {
            if (Objects.equals(o, listIterator.previous())) {
                return listIterator.nextIndex();
            }
        }
        return -1;
    }

    @Override
    public ListIterator<BsonValue> listIterator() {
        ensureOpen();
        return listIterator(0);
    }

    @Override
    public ListIterator<BsonValue> listIterator(final int index) {
        ensureOpen();
        // Not the most efficient way to do this, but unlikely anyone will notice in practice
        return new ArrayList<>(this).listIterator(index);
    }

    @Override
    public List<BsonValue> subList(final int fromIndex, final int toIndex) {
        ensureOpen();
        if (fromIndex < 0) {
            throw new IndexOutOfBoundsException("fromIndex = " + fromIndex);
        }
        if (fromIndex > toIndex) {
            throw new IllegalArgumentException("fromIndex(" + fromIndex + ") > toIndex(" + toIndex + ")");
        }
        List<BsonValue> subList = new ArrayList<>();
        int i = 0;
        for (BsonValue cur: this) {
            if (i == toIndex) {
                break;
            }
            if (i >= fromIndex) {
                subList.add(cur);
            }
            i++;
        }
        if (toIndex > i) {
            throw new IndexOutOfBoundsException("toIndex = " + toIndex);
        }
        return subList;
    }

    @Override
    public boolean add(final BsonValue bsonValue) {
        throw new UnsupportedOperationException(READ_ONLY_MESSAGE);
    }

    @Override
    public boolean remove(final Object o) {
        throw new UnsupportedOperationException(READ_ONLY_MESSAGE);
    }

    @Override
    public boolean addAll(final Collection<? extends BsonValue> c) {
        throw new UnsupportedOperationException(READ_ONLY_MESSAGE);
    }

    @Override
    public boolean addAll(final int index, final Collection<? extends BsonValue> c) {
        ensureOpen();
        throw new UnsupportedOperationException(READ_ONLY_MESSAGE);
    }

    @Override
    public boolean removeAll(final Collection<?> c) {
        throw new UnsupportedOperationException(READ_ONLY_MESSAGE);
    }

    @Override
    public boolean retainAll(final Collection<?> c) {
        throw new UnsupportedOperationException(READ_ONLY_MESSAGE);
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException(READ_ONLY_MESSAGE);
    }

    @Override
    public BsonValue set(final int index, final BsonValue element) {
        throw new UnsupportedOperationException(READ_ONLY_MESSAGE);
    }

    @Override
    public void add(final int index, final BsonValue element) {
        throw new UnsupportedOperationException(READ_ONLY_MESSAGE);
    }

    @Override
    public BsonValue remove(final int index) {
        throw new UnsupportedOperationException(READ_ONLY_MESSAGE);
    }

    @Override
    public void close(){
        if (!closed) {
            for (Closeable closeable : trackedResources) {
                try {
                    closeable.close();
                } catch (Exception e) {
                    // Log and continue closing other resources
                }
            }
            trackedResources.clear();
            closed = true;
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("The BsonArray resources have been released.");
        }
    }

    private class ByteBufBsonArrayIterator implements Iterator<BsonValue> {
        private ByteBuf duplicatedByteBuf;
        private BsonBinaryReader bsonReader;
        private Closeable resourceHandle;
        private boolean finished;

        {
            ensureOpen();
            duplicatedByteBuf = byteBuf.duplicate();
            resourceHandle = () -> {
                if (duplicatedByteBuf != null) {
                    duplicatedByteBuf.release();
                    duplicatedByteBuf = null;
                }
            };
            trackedResources.add(resourceHandle);
            bsonReader = new BsonBinaryReader(new ByteBufferBsonInput(duplicatedByteBuf));
            // While one might expect that this would be a call to BsonReader#readStartArray that doesn't work because BsonBinaryReader
            // expects to be positioned at the start at the beginning of a document, not an array.  Fortunately, a BSON array has exactly
            // the same structure as a BSON document (the keys are just the array indices converted to a strings).  So it works fine to
            // call BsonReader#readStartDocument here, and just skip all the names via BsonReader#skipName below.
            bsonReader.readStartDocument();
            bsonReader.readBsonType();
        }

        @Override
        public boolean hasNext() {
            boolean hasNext = bsonReader.getCurrentBsonType() != BsonType.END_OF_DOCUMENT;
            if (!hasNext) {
                cleanup();
            }
            return hasNext;
        }

        @Override
        public BsonValue next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            bsonReader.skipName();
            BsonValue value = readBsonValue(duplicatedByteBuf, bsonReader, trackedResources);
            bsonReader.readBsonType();
            return value;
        }

        private void cleanup() {
            if (!finished) {
                finished = true;
                // Remove from tracked resources since we're cleaning up immediately
                trackedResources.remove(resourceHandle);
                try {
                    resourceHandle.close();
                } catch (Exception e) {
                    // Ignore
                }
            }
        }
    }
}
