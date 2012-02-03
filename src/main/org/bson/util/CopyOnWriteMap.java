/**
 * Copyright 2008 Atlassian Pty Ltd 
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

package org.bson.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.WeakHashMap;

import org.bson.util.annotations.GuardedBy;
import org.bson.util.annotations.ThreadSafe;

import org.bson.util.AbstractCopyOnWriteMap.View.Type;

/**
 * A thread-safe variant of {@link Map} in which all mutative operations (the
 * "destructive" operations described by {@link Map} put, remove and so on) are
 * implemented by making a fresh copy of the underlying map.
 * <p>
 * This is ordinarily too costly, but may be <em>more</em> efficient than
 * alternatives when traversal operations vastly out-number mutations, and is
 * useful when you cannot or don't want to synchronize traversals, yet need to
 * preclude interference among concurrent threads. The "snapshot" style
 * iterators on the collections returned by {@link #entrySet()},
 * {@link #keySet()} and {@link #values()} use a reference to the internal map
 * at the point that the iterator was created. This map never changes during the
 * lifetime of the iterator, so interference is impossible and the iterator is
 * guaranteed not to throw <tt>ConcurrentModificationException</tt>. The
 * iterators will not reflect additions, removals, or changes to the list since
 * the iterator was created. Removing elements via these iterators is not
 * supported. The mutable operations on these collections (remove, retain etc.)
 * are supported but as with the {@link Map} interface, add and addAll are not
 * and throw {@link UnsupportedOperationException}.
 * <p>
 * The actual copy is performed by an abstract {@link #copy(Map)} method. The
 * method is responsible for the underlying Map implementation (for instance a
 * {@link HashMap}, {@link TreeMap}, {@link LinkedHashMap} etc.) and therefore
 * the semantics of what this map will cope with as far as null keys and values,
 * iteration ordering etc. See the note below about suitable candidates for
 * underlying Map implementations
 * <p>
 * There are supplied implementations for the common j.u.c {@link Map}
 * implementations via the {@link CopyOnWriteMap} static {@link Builder}.
 * <p>
 * Collection views of the keys, values and entries are optionally
 * {@link View.Type.LIVE live} or {@link View.Type.STABLE stable}. Live views
 * are modifiable will cause a copy if a modifying method is called on them.
 * Methods on these will reflect the current state of the collection, although
 * iterators will be snapshot style. If the collection views are stable they are
 * unmodifiable, and will be a snapshot of the state of the map at the time the
 * collection was asked for.
 * <p>
 * <strong>Please note</strong> that the thread-safety guarantees are limited to
 * the thread-safety of the non-mutative (non-destructive) operations of the
 * underlying map implementation. For instance some implementations such as
 * {@link WeakHashMap} and {@link LinkedHashMap} with access ordering are
 * actually structurally modified by the {@link #get(Object)} method and are
 * therefore not suitable candidates as delegates for this class.
 * 
 * @param <K> the key type
 * @param <V> the value type
 * @author Jed Wesley-Smith
 */
@ThreadSafe
abstract class CopyOnWriteMap<K, V> extends AbstractCopyOnWriteMap<K, V, Map<K, V>> {
    private static final long serialVersionUID = 7935514534647505917L;

    /**
     * Get a {@link Builder} for a {@link CopyOnWriteMap} instance.
     * 
     * @param <K> key type
     * @param <V> value type
     * @return a fresh builder
     */
    public static <K, V> Builder<K, V> builder() {
        return new Builder<K, V>();
    }

    /**
     * Build a {@link CopyOnWriteMap} and specify all the options.
     * 
     * @param <K> key type
     * @param <V> value type
     */
    public static class Builder<K, V> {
        private View.Type viewType = View.Type.STABLE;
        private final Map<K, V> initialValues = new HashMap<K, V>();

        Builder() {}

        /**
         * Views are stable (fixed in time) and unmodifiable.
         */
        public Builder<K, V> stableViews() {
            viewType = View.Type.STABLE;
            return this;
        }

        /**
         * Views are live (reflecting concurrent updates) and mutator methods
         * are supported.
         */
        public Builder<K, V> addAll(final Map<? extends K, ? extends V> values) {
            initialValues.putAll(values);
            return this;
        }

        /**
         * Views are live (reflecting concurrent updates) and mutator methods
         * are supported.
         */
        public Builder<K, V> liveViews() {
            viewType = View.Type.LIVE;
            return this;
        }

        public CopyOnWriteMap<K, V> newHashMap() {
            return new Hash<K, V>(initialValues, viewType);
        }

        public CopyOnWriteMap<K, V> newLinkedMap() {
            return new Linked<K, V>(initialValues, viewType);
        }
    }

    /**
     * Creates a new {@link CopyOnWriteMap} with an underlying {@link HashMap}.
     * <p>
     * This map has {@link View.Type.STABLE stable} views.
     */
    public static <K, V> CopyOnWriteMap<K, V> newHashMap() {
        final Builder<K, V> builder = builder();
        return builder.newHashMap();
    }

    /**
     * Creates a new {@link CopyOnWriteMap} with an underlying {@link HashMap}
     * using the supplied map as the initial values.
     * <p>
     * This map has {@link View.Type.STABLE stable} views.
     */
    public static <K, V> CopyOnWriteMap<K, V> newHashMap(final Map<? extends K, ? extends V> map) {
        final Builder<K, V> builder = builder();
        return builder.addAll(map).newHashMap();
    }

    /**
     * Creates a new {@link CopyOnWriteMap} with an underlying
     * {@link LinkedHashMap}. Iterators for this map will be return elements in
     * insertion order.
     * <p>
     * This map has {@link View.Type.STABLE stable} views.
     */
    public static <K, V> CopyOnWriteMap<K, V> newLinkedMap() {
        final Builder<K, V> builder = builder();
        return builder.newLinkedMap();
    }

    /**
     * Creates a new {@link CopyOnWriteMap} with an underlying
     * {@link LinkedHashMap} using the supplied map as the initial values.
     * Iterators for this map will be return elements in insertion order.
     * <p>
     * This map has {@link View.Type.STABLE stable} views.
     */
    public static <K, V> CopyOnWriteMap<K, V> newLinkedMap(final Map<? extends K, ? extends V> map) {
        final Builder<K, V> builder = builder();
        return builder.addAll(map).newLinkedMap();
    }

    //
    // constructors
    //

    /**
     * Create a new {@link CopyOnWriteMap} with the supplied {@link Map} to
     * initialize the values.
     * 
     * @param map the initial map to initialize with
     * @deprecated since 0.0.12 use the versions that explicitly specify
     * View.Type
     */
    @Deprecated
    protected CopyOnWriteMap(final Map<? extends K, ? extends V> map) {
        this(map, View.Type.LIVE);
    }

    /**
     * Create a new empty {@link CopyOnWriteMap}.
     * 
     * @deprecated since 0.0.12 use the versions that explicitly specify
     * View.Type
     */
    @Deprecated
    protected CopyOnWriteMap() {
        this(Collections.<K, V> emptyMap(), View.Type.LIVE);
    }

    /**
     * Create a new {@link CopyOnWriteMap} with the supplied {@link Map} to
     * initialize the values. This map may be optionally modified using any of
     * the key, entry or value views
     * 
     * @param map the initial map to initialize with
     */
    protected CopyOnWriteMap(final Map<? extends K, ? extends V> map, final View.Type viewType) {
        super(map, viewType);
    }

    /**
     * Create a new empty {@link CopyOnWriteMap}. This map may be optionally
     * modified using any of the key, entry or value views
     */
    protected CopyOnWriteMap(final View.Type viewType) {
        super(Collections.<K, V> emptyMap(), viewType);
    }

    @Override
    @GuardedBy("internal-lock")
    protected abstract <N extends Map<? extends K, ? extends V>> Map<K, V> copy(N map);

    //
    // inner classes
    //

    /**
     * Uses {@link HashMap} instances as its internal storage.
     */
    static class Hash<K, V> extends CopyOnWriteMap<K, V> {
        private static final long serialVersionUID = 5221824943734164497L;

        Hash(final Map<? extends K, ? extends V> map, final Type viewType) {
            super(map, viewType);
        }

        @Override
        public <N extends Map<? extends K, ? extends V>> Map<K, V> copy(final N map) {
            return new HashMap<K, V>(map);
        }
    }

    /**
     * Uses {@link LinkedHashMap} instances as its internal storage.
     */
    static class Linked<K, V> extends CopyOnWriteMap<K, V> {
        private static final long serialVersionUID = -8659999465009072124L;

        Linked(final Map<? extends K, ? extends V> map, final Type viewType) {
            super(map, viewType);
        }

        @Override
        public <N extends Map<? extends K, ? extends V>> Map<K, V> copy(final N map) {
            return new LinkedHashMap<K, V>(map);
        }
    }
}
