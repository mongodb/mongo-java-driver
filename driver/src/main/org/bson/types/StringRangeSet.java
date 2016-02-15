/*
 * Copyright 2008-2016 MongoDB, Inc.
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

package org.bson.types;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import static com.mongodb.assertions.Assertions.isTrue;

class StringRangeSet implements Set<String> {

    private static final String[] STRINGS = new String[1024];

    static {
        for (int i = 0; i < STRINGS.length; ++i) {
            STRINGS[i] = String.valueOf(i);
        }
    }

    private final int size;

    StringRangeSet(final int size) {
        isTrue("size >= 0", size >= 0);
        this.size = size;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean contains(final Object o) {
        if (!(o instanceof String)) {
            return false;
        }
        int i = Integer.parseInt((String) o);
        return i >= 0 && i < size();
    }

    @Override
    public Iterator<String> iterator() {
        return new Iterator<String>() {
            private int cur = 0;

            @Override
            public boolean hasNext() {
                return cur < size;
            }

            @Override
            public String next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return intToString(cur++);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public Object[] toArray() {
        Object[] retVal = new Object[size()];
        for (int i = 0; i < size(); i++) {
            retVal[i] = intToString(i);
        }
        return retVal;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T[] toArray(final T[] a) {
        T[] retVal = a.length >= size()
                ? a
                : (T[]) java.lang.reflect.Array
                .newInstance(a.getClass().getComponentType(), size);
        for (int i = 0; i < size(); i++) {
            retVal[i] = (T) (intToString(i));
        }
        if (a.length > size()) {
            a[size] = null;
        }
        return retVal;
    }

    @Override
    public boolean add(final String integer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(final Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(final Collection<?> c) {
        for (Object e : c) {
            if (!contains(e)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean addAll(final Collection<? extends String> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(final Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(final Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    private String intToString(final int i) {
        return i < STRINGS.length
                ? STRINGS[i]
                : Integer.toString(i);
    }
}
