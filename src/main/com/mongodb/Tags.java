/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.bson.util.Assertions.isTrue;

/**
 * A map of String keys to String values, intended for use in specifying server tags.
 *
 * @mongodb.driver.manual tutorial/configure-replica-set-tag-sets Tag Sets
 */
class Tags implements Map<String, String> {
    private final boolean frozen;
    private final Map<String, String> wrapped;

    /**
     * Returns an unmodifiable view of the specified tags.
     *
     * @param tags the tags to freeze
     * @return an unmodifiable copy of the tags
     */
    public static Tags freeze(final Tags tags) {
        return new Tags(tags);
    }

    public Tags() {
        wrapped = new HashMap<String, String>();
        frozen = false;
    }

    public Tags(final String key, final String value) {
        wrapped = new HashMap<String, String>();
        wrapped.put(key, value);
        frozen = false;
    }

    Tags(final Map<String, String> wrapped) {
        this.wrapped = new HashMap<String, String>(wrapped);
        frozen = true;
    }

    public Tags append(final String key, final String value) {
        isTrue("not frozen", !frozen);

        wrapped.put(key, value);
        return this;
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
    public boolean containsKey(final Object key) {
        return wrapped.containsKey(key);
    }

    @Override
    public boolean containsValue(final Object value) {
        return wrapped.containsValue(value);
    }

    @Override
    public String get(final Object key) {
        return wrapped.get(key);
    }

    @Override
    public String put(final String key, final String value) {
        isTrue("not frozen", !frozen);

        return wrapped.put(key, value);
    }

    @Override
    public String remove(final Object key) {
        isTrue("not frozen", !frozen);

        return wrapped.remove(key);
    }

    @Override
    public void putAll(final Map<? extends String, ? extends String> m) {
        isTrue("not frozen", !frozen);

        wrapped.putAll(m);
    }

    @Override
    public void clear() {
        isTrue("not frozen", !frozen);

        wrapped.clear();
    }

    @Override
    public Set<String> keySet() {
        return wrapped.keySet();
    }

    @Override
    public Collection<String> values() {
        return wrapped.values();
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
        return wrapped.entrySet();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final Tags tags = (Tags) o;

        if (!wrapped.equals(tags.wrapped)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return wrapped.hashCode();
    }

    @Override
    public String toString() {
        return wrapped.toString();
    }
}

