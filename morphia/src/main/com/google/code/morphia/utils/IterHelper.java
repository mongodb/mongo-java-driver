/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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

package com.google.code.morphia.utils;

import org.bson.BSONObject;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Helper to allow for optimizations for different types of Map/Collections
 *
 * @param <T> The key type of the map
 * @param <V> The value type of the map/collection
 * @author Scott Hernandez
 */
public class IterHelper<T, V> {
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void loopMap(final Object x, final MapIterCallback<T, V> iter) {
        if (x == null) {
            return;
        }

        if (x instanceof Collection) {
            throw new IllegalArgumentException("call loop instead");
        }

        if (x instanceof HashMap<?, ?>) {
            if (((HashMap) x).size() == 0) {
                return;
            }

            final HashMap<?, ?> hm = (HashMap<?, ?>) x;
            for (final Entry<?, ?> e : hm.entrySet()) {
                iter.eval((T) e.getKey(), (V) e.getValue());
            }
            return;
        }
        if (x instanceof Map<?, ?>) {
            final Map<?, ?> m = (Map<?, ?>) x;
            for (final Object k : m.keySet()) {
                iter.eval((T) k, (V) m.get(k));
            }
            return;
        }
        if (x instanceof BSONObject) {
            final BSONObject m = (BSONObject) x;
            for (final String k : m.keySet()) {
                iter.eval((T) k, (V) m.get(k));
            }
        }

    }

    @SuppressWarnings("unchecked")
    public void loop(final Object x, final IterCallback<V> iter) {
        if (x == null) {
            return;
        }

        if (x instanceof Map) {
            throw new IllegalArgumentException("call loopMap instead");
        }

        if (x instanceof List<?>) {
            final List<?> l = (List<?>) x;
            for (final Object o : l) {
                iter.eval((V) o);
            }
        }
    }

    /**
     * Calls eval for each entry found, or just once if the "x" isn't iterable/collection/list/etc. with "x"
     *
     * @param x
     * @param iter
     */
    @SuppressWarnings("unchecked")
    public void loopOrSingle(final Object x, final IterCallback<V> iter) {
        if (x == null) {
            return;
        }

        //A collection
        if (x instanceof Collection<?>) {
            final Collection<?> l = (Collection<?>) x;
            for (final Object o : l) {
                iter.eval((V) o);
            }
            return;
        }

        //An array of Object[]
        if (x.getClass().isArray()) {
            for (final Object o : (Object[]) x) {
                iter.eval((V) o);
            }
            return;
        }

        iter.eval((V) x);
    }

    public abstract static class MapIterCallback<T, V> {
        public abstract void eval(T t, V v);
    }

    public abstract static class IterCallback<V> {
        public abstract void eval(V v);
    }
}
