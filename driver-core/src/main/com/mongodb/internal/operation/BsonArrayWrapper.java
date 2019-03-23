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

package com.mongodb.internal.operation;

import org.bson.BsonArray;
import org.bson.BsonValue;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import static org.bson.assertions.Assertions.notNull;


class BsonArrayWrapper<T> extends BsonArray {

    private final List<T> wrappedArray;

    BsonArrayWrapper(final List<T> wrappedArray) {
        this.wrappedArray = notNull("wrappedArray", wrappedArray);
    }

    /**
     * Get the wrapped array.
     *
     * @return the wrapped array
     */
    public List<T> getWrappedArray() {
        return wrappedArray;
    }

    @Override
    public List<BsonValue> getValues() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean contains(final Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<BsonValue> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T[] toArray(final T[] a) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(final BsonValue bsonValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(final Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(final Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(final Collection<? extends BsonValue> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(final int index, final Collection<? extends BsonValue> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(final Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(final Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public BsonValue get(final int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BsonValue set(final int index, final BsonValue element) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void add(final int index, final BsonValue element) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BsonValue remove(final int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int indexOf(final Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int lastIndexOf(final Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListIterator<BsonValue> listIterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListIterator<BsonValue> listIterator(final int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<BsonValue> subList(final int fromIndex, final int toIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BsonArrayWrapper<?> that = (BsonArrayWrapper<?>) o;
        if (!wrappedArray.equals(that.wrappedArray)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + wrappedArray.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "BsonArrayWrapper{"
                + "wrappedArray=" + wrappedArray
                + '}';
    }

    @Override
    public BsonArray clone() {
        throw new UnsupportedOperationException("This should never be called on an instance of this type");
    }
}
