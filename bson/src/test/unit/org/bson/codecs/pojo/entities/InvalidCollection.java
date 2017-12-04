/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bson.codecs.pojo.entities;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

@SuppressWarnings("rawtypes")
public class InvalidCollection implements Collection {
    private final List<Integer> wrapped;

    public InvalidCollection(final List<Integer> wrapped) {
        this.wrapped = new ArrayList<Integer>(wrapped);
    }

    @Override
    public int size() {
        return wrapped.size();
    }

    @Override
    public boolean isEmpty() {
        return wrapped.isEmpty();
    }

    @Override
    public boolean contains(final Object o) {
        return wrapped.contains(o);
    }

    @Override
    public Iterator iterator() {
        return wrapped.iterator();
    }

    @Override
    public Object[] toArray() {
        return wrapped.toArray();
    }

    @Override
    public boolean add(final Object o) {
        return false;
    }

    @Override
    public boolean remove(final Object o) {
        return false;
    }

    @Override
    public boolean addAll(final Collection c) {
        return false;
    }

    @Override
    public void clear() {
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InvalidCollection that = (InvalidCollection) o;
        return wrapped.equals(that.wrapped);
    }

    @Override
    public int hashCode() {
        return wrapped.hashCode();
    }

    @Override
    public boolean retainAll(final Collection c) {
        return false;
    }

    @Override
    public boolean removeAll(final Collection c) {
        return false;
    }

    @Override
    public boolean containsAll(final Collection c) {
        return wrapped.containsAll(c);
    }

    @Override
    public Object[] toArray(final Object[] a) {
        return wrapped.toArray(a);
    }
}
