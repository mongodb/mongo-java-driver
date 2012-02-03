package org.bson.util;

import static java.util.Collections.unmodifiableList;
import static org.bson.util.CopyOnWriteMap.newHashMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

class ClassAncestry {

    /**
     * getAncestry
     * 
     * Walks superclass and interface graph, superclasses first, then
     * interfaces, to compute an ancestry list. Supertypes are visited left to
     * right. Duplicates are removed such that no Class will appear in the list
     * before one of its subtypes.
     * 
     * Does not need to be synchronized, races are harmless as the Class graph
     * does not change at runtime.
     */
    public static <T> List<Class<?>> getAncestry(Class<T> c) {
        final ConcurrentMap<Class<?>, List<Class<?>>> cache = getClassAncestryCache();
        while (true) {
            List<Class<?>> cachedResult = cache.get(c);
            if (cachedResult != null) {
                return cachedResult;
            }
            cache.putIfAbsent(c, computeAncestry(c));
        }
    }

    /**
     * computeAncestry, starting with children and going back to parents
     */
    private static List<Class<?>> computeAncestry(Class<?> c) {
        final List<Class<?>> result = new ArrayList<Class<?>>();
        result.add(Object.class);
        computeAncestry(c, result);
        Collections.reverse(result);
        return unmodifiableList(new ArrayList<Class<?>>(result));
    }

    private static <T> void computeAncestry(Class<T> c, List<Class<?>> result) {
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

        if (!result.contains(c))
            result.add(c);
    }

    /**
     * classAncestryCache
     */
    private static ConcurrentMap<Class<?>, List<Class<?>>> getClassAncestryCache() {
        return (_ancestryCache);
    }

    private static final ConcurrentMap<Class<?>, List<Class<?>>> _ancestryCache = newHashMap();
}
