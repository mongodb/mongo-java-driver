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

package org.bson.util;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static org.bson.assertions.Assertions.notNull;

final class ComputingMap<K, V> implements Map<K, V>, Function<K, V> {

    public static <K, V> Map<K, V> create(final Function<K, V> function) {
        return new ComputingMap<K, V>(CopyOnWriteMap.<K, V>newHashMap(), function);
    }

    private final ConcurrentMap<K, V> map;
    private final Function<K, V> function;

    ComputingMap(final ConcurrentMap<K, V> map, final Function<K, V> function) {
        this.map = notNull("map", map);
        this.function = notNull("function", function);
    }

    public V get(final Object key) {
        while (true) {
            V v = map.get(key);
            if (v != null) {
                return v;
            }
            @SuppressWarnings("unchecked")
            K k = (K) key;
            V value = function.apply(k);
            if (value == null) {
                return null;
            }
            map.putIfAbsent(k, value);
        }
    }

    public V apply(final K k) {
        return get(k);
    }

    public V putIfAbsent(final K key, final V value) {
        return map.putIfAbsent(key, value);
    }

    public boolean remove(final Object key, final Object value) {
        return map.remove(key, value);
    }

    public boolean replace(final K key, final V oldValue, final V newValue) {
        return map.replace(key, oldValue, newValue);
    }

    public V replace(final K key, final V value) {
        return map.replace(key, value);
    }

    public int size() {
        return map.size();
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    public boolean containsKey(final Object key) {
        return map.containsKey(key);
    }

    public boolean containsValue(final Object value) {
        return map.containsValue(value);
    }

    public V put(final K key, final V value) {
        return map.put(key, value);
    }

    public V remove(final Object key) {
        return map.remove(key);
    }

    public void putAll(final Map<? extends K, ? extends V> m) {
        map.putAll(m);
    }

    public void clear() {
        map.clear();
    }

    public Set<K> keySet() {
        return map.keySet();
    }

    public Collection<V> values() {
        return map.values();
    }

    public Set<java.util.Map.Entry<K, V>> entrySet() {
        return map.entrySet();
    }

    public boolean equals(final Object o) {
        return map.equals(o);
    }

    public int hashCode() {
        return map.hashCode();
    }
}
