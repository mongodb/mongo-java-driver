// ClassMap.java

/**
 *      Copyright (C) 2008 10gen Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.bson.util;

import java.util.List;
import java.util.Map;

/**
 * Maps Class objects to values. A ClassMap is different from a regular Map in
 * that get(c) does not only look to see if 'c' is a key in the Map, but also
 * walks the up superclass and interface graph of 'c' to find matches. Derived
 * matches of this sort are then "cached" in the registry so that matches are
 * faster on future gets.
 * 
 * This is a very useful class for Class based registries.
 * 
 * Example:
 * 
 * ClassMap<String> m = new ClassMap<String>(); m.put(Animal.class, "Animal");
 * m.put(Fox.class, "Fox"); m.Fox.class) --> "Fox" m.get(Dog.class) --> "Animal"
 * 
 * (assuming Dog.class &lt; Animal.class)
 */
public class ClassMap<T>  {
    /**
     * Walks superclass and interface graph, superclasses first, then
     * interfaces, to compute an ancestry list. Supertypes are visited left to
     * right. Duplicates are removed such that no Class will appear in the list
     * before one of its subtypes.
     */
    public static <T> List<Class<?>> getAncestry(Class<T> c) {
        return ClassAncestry.getAncestry(c);
    }

    private final class ComputeFunction implements Function<Class<?>, T> {
        @Override
        public T apply(Class<?> a) {
            for (Class<?> cls : getAncestry(a)) {
                T result = map.get(cls);
                if (result != null) {
                    return result;
                }
            }
            return null;
        }
    };

    private final Map<Class<?>, T> map = CopyOnWriteMap.newHashMap();
    private final Map<Class<?>, T> cache = ComputingMap.create(new ComputeFunction());


    public T get(Object key) {
        return cache.get(key);
    }

    public T put(Class<?> key, T value) {
        try {
            return map.put(key, value);
        } finally {
            cache.clear();
        }
    }

    public T remove(Object key) {
        try {
            return map.remove(key);
        } finally {
            cache.clear();
        }
    }

    public void clear() {
        map.clear();
        cache.clear();
    }

    public int size() {
        return map.size();
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }
}
