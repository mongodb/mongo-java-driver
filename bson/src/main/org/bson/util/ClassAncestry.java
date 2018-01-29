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

package org.bson.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import static java.util.Collections.unmodifiableList;
import static org.bson.util.CopyOnWriteMap.newHashMap;

class ClassAncestry {

    /**
     * <p>Walks superclass and interface graph, superclasses first, then interfaces, to compute an ancestry list. Supertypes are visited
     * left
     * to right. Duplicates are removed such that no Class will appear in the list before one of its subtypes.</p>
     *
     * <p>Does not need to be synchronized, races are harmless as the Class graph does not change at runtime.</p>
     */
    public static <T> List<Class<?>> getAncestry(final Class<T> c) {
        ConcurrentMap<Class<?>, List<Class<?>>> cache = getClassAncestryCache();
        while (true) {
            List<Class<?>> cachedResult = cache.get(c);
            if (cachedResult != null) {
                return cachedResult;
            }
            cache.putIfAbsent(c, computeAncestry(c));
        }
    }

    /**
     * Starting with children and going back to parents
     */
    private static List<Class<?>> computeAncestry(final Class<?> c) {
        List<Class<?>> result = new ArrayList<Class<?>>();
        result.add(Object.class);
        computeAncestry(c, result);
        Collections.reverse(result);
        return unmodifiableList(new ArrayList<Class<?>>(result));
    }

    private static <T> void computeAncestry(final Class<T> c, final List<Class<?>> result) {
        if ((c == null) || (c == Object.class)) {
            return;
        }

        // first interfaces (looks backwards but is not)
        Class<?>[] interfaces = c.getInterfaces();
        for (int i = interfaces.length - 1; i >= 0; i--) {
            computeAncestry(interfaces[i], result);
        }

        // next superclass
        computeAncestry(c.getSuperclass(), result);

        if (!result.contains(c)) {
            result.add(c);
        }
    }

    /**
     * classAncestryCache
     */
    private static ConcurrentMap<Class<?>, List<Class<?>>> getClassAncestryCache() {
        return (_ancestryCache);
    }

    private static final ConcurrentMap<Class<?>, List<Class<?>>> _ancestryCache = newHashMap();
}

