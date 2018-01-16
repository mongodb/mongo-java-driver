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

import java.util.List;
import java.util.Map;

/**
 * <p>Maps Class objects to values. A ClassMap is different from a regular Map in that {@code get(clazz)} does not only look to see if
 * {@code clazz} is a key in the Map, but also walks the up superclass and interface graph of {@code clazz} to find matches. Derived matches
 * of this sort are then cached in the registry so that matches are faster on future gets.</p>
 *
 * <p>This is a very useful class for Class based registries.</p>
 *
 * <p>Example:</p>
 * <pre>{@code
 * ClassMap<String> m = new ClassMap<String>();
 * m.put(Animal.class, "Animal");
 * m.put(Fox.class, "Fox");
 * m.get(Fox.class) --> "Fox"
 * m.get(Dog.class) --> "Animal"
 * } </pre>
 *
 * (assuming Dog.class &lt; Animal.class)
 *
 * @param <T> the type of the value in this map
 */
public class ClassMap<T> {
    /**
     * Helper method that walks superclass and interface graph, superclasses first, then interfaces, to compute an ancestry list. Super
     * types are visited left to right. Duplicates are removed such that no Class will appear in the list before one of its subtypes.
     *
     * @param clazz the class to get the ancestors for
     * @param <T>   the type of the class modeled by this {@code Class} object.
     * @return a list of all the super classes of {@code clazz}, starting with the class, and ending with {@code java.lang.Object}.
     */
    public static <T> List<Class<?>> getAncestry(final Class<T> clazz) {
        return ClassAncestry.getAncestry(clazz);
    }

    private final class ComputeFunction implements Function<Class<?>, T> {
        @Override
        public T apply(final Class<?> a) {
            for (final Class<?> cls : getAncestry(a)) {
                T result = map.get(cls);
                if (result != null) {
                    return result;
                }
            }
            return null;
        }
    }

    private final Map<Class<?>, T> map = CopyOnWriteMap.newHashMap();
    private final Map<Class<?>, T> cache = ComputingMap.create(new ComputeFunction());

    /**
     * Gets the value associated with either this Class or a superclass of this class.  If fetching for a super class, it fetches the value
     * for the closest superclass. Returns null if the given class and none of its superclasses are in the map.
     *
     * @param key a {@code Class} to get the value for
     * @return the value for either this class or its nearest superclass
     */
    public T get(final Object key) {
        return cache.get(key);
    }

    /**
     * As per {@code java.util.Map}, associates the specified value with the specified key in this map.  If the map previously contained a
     * mapping for the key, the old value is replaced by the specified value.
     *
     * @param key   a {@code Class} key
     * @param value the value for this class
     * @return the previous value associated with {@code key}, or null if there was no mapping for key.
     * @see java.util.Map#put(Object, Object)
     */
    public T put(final Class<?> key, final T value) {
        try {
            return map.put(key, value);
        } finally {
            cache.clear();
        }
    }

    /**
     * As per {@code java.util.Map}, removes the mapping for a key from this map if it is present
     *
     * @param key a {@code Class} key
     * @return the previous value associated with {@code key}, or null if there was no mapping for key.
     * @see java.util.Map#remove(Object)
     */
    public T remove(final Object key) {
        try {
            return map.remove(key);
        } finally {
            cache.clear();
        }
    }

    /**
     * As per {@code java.util.Map}, removes all of the mappings from this map (optional operation).
     *
     * @see java.util.Map#clear()
     */
    public void clear() {
        map.clear();
        cache.clear();
    }

    /**
     * As per {@code java.util.Map}, returns the number of key-value mappings in this map.  This will only return the number of keys
     * explicitly added to the map, not any cached hierarchy keys.
     *
     * @return the size of this map
     * @see java.util.Map#size()
     */
    public int size() {
        return map.size();
    }

    /**
     * As per {@code java.util.Map}, returns {@code true} if this map contains no key-value mappings.
     *
     * @return true if there are no values in the map
     * @see java.util.Map#isEmpty()
     */
    public boolean isEmpty() {
        return map.isEmpty();
    }
}
