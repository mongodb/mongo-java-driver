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

import static org.bson.util.Assertions.notNull;
import static java.util.Collections.unmodifiableCollection;
import static java.util.Collections.unmodifiableSet;
import org.bson.util.annotations.GuardedBy;
import org.bson.util.annotations.ThreadSafe;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Abstract base class for COW {@link Map} implementations that delegate to an
 * internal map.
 * 
 * @param <K> The key type
 * @param <V> The value type
 * @param <M> the internal {@link Map} or extension for things like sorted and
 * navigable maps.
 */
@ThreadSafe
abstract class AbstractCopyOnWriteMap<K, V, M extends Map<K, V>> implements ConcurrentMap<K, V>, Serializable {
    private static final long serialVersionUID = 4508989182041753878L;

    @GuardedBy("lock")
    private volatile M delegate;

    // import edu.umd.cs.findbugs.annotations.@SuppressWarnings
    private final transient Lock lock = new ReentrantLock();

    // private final transient EntrySet entrySet = new EntrySet();
    // private final transient KeySet keySet = new KeySet();
    // private final transient Values values = new Values();
    // private final View.Type viewType;
    private final View<K, V> view;

    /**
     * Create a new {@link CopyOnWriteMap} with the supplied {@link Map} to
     * initialize the values.
     * 
     * @param map the initial map to initialize with
     * @param viewType for writable or read-only key, value and entrySet views
     */
    protected <N extends Map<? extends K, ? extends V>> AbstractCopyOnWriteMap(final N map, final View.Type viewType) {
        this.delegate = notNull("delegate", copy(notNull("map", map)));
        this.view = notNull("viewType", viewType).get(this);
    }

    /**
     * Copy function, implemented by sub-classes.
     * 
     * @param <N> the map to copy and return.
     * @param map the initial values of the newly created map.
     * @return a new map. Will never be modified after construction.
     */
    @GuardedBy("lock")
    abstract <N extends Map<? extends K, ? extends V>> M copy(N map);

    //
    // mutable operations
    //

    public final void clear() {
        lock.lock();
        try {
            set(copy(Collections.<K, V> emptyMap()));
        } finally {
            lock.unlock();
        }
    }

    public final V remove(final Object key) {
        lock.lock();
        try {
            // short circuit if key doesn't exist
            if (!delegate.containsKey(key)) {
                return null;
            }
            final M map = copy();
            try {
                return map.remove(key);
            } finally {
                set(map);
            }
        } finally {
            lock.unlock();
        }
    }

    public boolean remove(final Object key, final Object value) {
        lock.lock();
        try {
            if (delegate.containsKey(key) && equals(value, delegate.get(key))) {
                final M map = copy();
                map.remove(key);
                set(map);
                return true;
            } else {
                return false;
            }
        } finally {
            lock.unlock();
        }
    }

    public boolean replace(final K key, final V oldValue, final V newValue) {
        lock.lock();
        try {
            if (!delegate.containsKey(key) || !equals(oldValue, delegate.get(key))) {
                return false;
            }
            final M map = copy();
            map.put(key, newValue);
            set(map);
            return true;
        } finally {
            lock.unlock();
        }
    }

    public V replace(final K key, final V value) {
        lock.lock();
        try {
            if (!delegate.containsKey(key)) {
                return null;
            }
            final M map = copy();
            try {
                return map.put(key, value);
            } finally {
                set(map);
            }
        } finally {
            lock.unlock();
        }
    }

    public final V put(final K key, final V value) {
        lock.lock();
        try {
            final M map = copy();
            try {
                return map.put(key, value);
            } finally {
                set(map);
            }
        } finally {
            lock.unlock();
        }
    }

    public V putIfAbsent(final K key, final V value) {
        lock.lock();
        try {
            if (!delegate.containsKey(key)) {
                final M map = copy();
                try {
                    return map.put(key, value);
                } finally {
                    set(map);
                }
            }
            return delegate.get(key);
        } finally {
            lock.unlock();
        }
    }

    public final void putAll(final Map<? extends K, ? extends V> t) {
        lock.lock();
        try {
            final M map = copy();
            map.putAll(t);
            set(map);
        } finally {
            lock.unlock();
        }
    }

    protected M copy() {
        lock.lock();
        try {
            return copy(delegate);
        } finally {
            lock.unlock();
        }
    }

    @GuardedBy("lock")
    protected void set(final M map) {
        delegate = map;
    }

    //
    // Collection views
    //

    public final Set<Map.Entry<K, V>> entrySet() {
        return view.entrySet();
    }

    public final Set<K> keySet() {
        return view.keySet();
    }

    public final Collection<V> values() {
        return view.values();
    }

    //
    // delegate operations
    //

    public final boolean containsKey(final Object key) {
        return delegate.containsKey(key);
    }

    public final boolean containsValue(final Object value) {
        return delegate.containsValue(value);
    }

    public final V get(final Object key) {
        return delegate.get(key);
    }

    public final boolean isEmpty() {
        return delegate.isEmpty();
    }

    public final int size() {
        return delegate.size();
    }

    @Override
    public final boolean equals(final Object o) {
        return delegate.equals(o);
    }

    @Override
    public final int hashCode() {
        return delegate.hashCode();
    }

    protected final M getDelegate() {
        return delegate;
    }

    @Override
    public String toString() {
        return delegate.toString();
    }

    //
    // inner classes
    //

    private class KeySet extends CollectionView<K> implements Set<K> {

        @Override
        Collection<K> getDelegate() {
            return delegate.keySet();
        }

        //
        // mutable operations
        //

        public void clear() {
            lock.lock();
            try {
                final M map = copy();
                map.keySet().clear();
                set(map);
            } finally {
                lock.unlock();
            }
        }

        public boolean remove(final Object o) {
            return AbstractCopyOnWriteMap.this.remove(o) != null;
        }

        public boolean removeAll(final Collection<?> c) {
            lock.lock();
            try {
                final M map = copy();
                try {
                    return map.keySet().removeAll(c);
                } finally {
                    set(map);
                }
            } finally {
                lock.unlock();
            }
        }

        public boolean retainAll(final Collection<?> c) {
            lock.lock();
            try {
                final M map = copy();
                try {
                    return map.keySet().retainAll(c);
                } finally {
                    set(map);
                }
            } finally {
                lock.unlock();
            }
        }
    }

    private final class Values extends CollectionView<V> {

        @Override
        Collection<V> getDelegate() {
            return delegate.values();
        }

        public void clear() {
            lock.lock();
            try {
                final M map = copy();
                map.values().clear();
                set(map);
            } finally {
                lock.unlock();
            }
        }

        public boolean remove(final Object o) {
            lock.lock();
            try {
                if (!contains(o)) {
                    return false;
                }
                final M map = copy();
                try {
                    return map.values().remove(o);
                } finally {
                    set(map);
                }
            } finally {
                lock.unlock();
            }
        }

        public boolean removeAll(final Collection<?> c) {
            lock.lock();
            try {
                final M map = copy();
                try {
                    return map.values().removeAll(c);
                } finally {
                    set(map);
                }
            } finally {
                lock.unlock();
            }
        }

        public boolean retainAll(final Collection<?> c) {
            lock.lock();
            try {
                final M map = copy();
                try {
                    return map.values().retainAll(c);
                } finally {
                    set(map);
                }
            } finally {
                lock.unlock();
            }
        }
    }

    private class EntrySet extends CollectionView<Entry<K, V>> implements Set<Map.Entry<K, V>> {

        @Override
        Collection<java.util.Map.Entry<K, V>> getDelegate() {
            return delegate.entrySet();
        }

        public void clear() {
            lock.lock();
            try {
                final M map = copy();
                map.entrySet().clear();
                set(map);
            } finally {
                lock.unlock();
            }
        }

        public boolean remove(final Object o) {
            lock.lock();
            try {
                if (!contains(o)) {
                    return false;
                }
                final M map = copy();
                try {
                    return map.entrySet().remove(o);
                } finally {
                    set(map);
                }
            } finally {
                lock.unlock();
            }
        }

        public boolean removeAll(final Collection<?> c) {
            lock.lock();
            try {
                final M map = copy();
                try {
                    return map.entrySet().removeAll(c);
                } finally {
                    set(map);
                }
            } finally {
                lock.unlock();
            }
        }

        public boolean retainAll(final Collection<?> c) {
            lock.lock();
            try {
                final M map = copy();
                try {
                    return map.entrySet().retainAll(c);
                } finally {
                    set(map);
                }
            } finally {
                lock.unlock();
            }
        }
    }

    private static class UnmodifiableIterator<T> implements Iterator<T> {
        private final Iterator<T> delegate;

        public UnmodifiableIterator(final Iterator<T> delegate) {
            this.delegate = delegate;
        }

        public boolean hasNext() {
            return delegate.hasNext();
        }

        public T next() {
            return delegate.next();
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    protected static abstract class CollectionView<E> implements Collection<E> {

        abstract Collection<E> getDelegate();

        //
        // delegate operations
        //

        public final boolean contains(final Object o) {
            return getDelegate().contains(o);
        }

        public final boolean containsAll(final Collection<?> c) {
            return getDelegate().containsAll(c);
        }

        public final Iterator<E> iterator() {
            return new UnmodifiableIterator<E>(getDelegate().iterator());
        }

        public final boolean isEmpty() {
            return getDelegate().isEmpty();
        }

        public final int size() {
            return getDelegate().size();
        }

        public final Object[] toArray() {
            return getDelegate().toArray();
        }

        public final <T> T[] toArray(final T[] a) {
            return getDelegate().toArray(a);
        }

        @Override
        public int hashCode() {
            return getDelegate().hashCode();
        }

        @Override
        public boolean equals(final Object obj) {
            return getDelegate().equals(obj);
        }

        @Override
        public String toString() {
            return getDelegate().toString();
        }

        //
        // unsupported operations
        //

        public final boolean add(final E o) {
            throw new UnsupportedOperationException();
        }

        public final boolean addAll(final Collection<? extends E> c) {
            throw new UnsupportedOperationException();
        }
    }

    private boolean equals(final Object o1, final Object o2) {
        if (o1 == null) {
            return o2 == null;
        }
        return o1.equals(o2);
    }

    /**
     * Provides access to the views of the underlying key, value and entry
     * collections.
     */
    public static abstract class View<K, V> {
        View() {}

        abstract Set<K> keySet();

        abstract Set<Entry<K, V>> entrySet();

        abstract Collection<V> values();

        /**
         * The different types of {@link View} available
         */
        public enum Type {
            STABLE {
                @Override
                <K, V, M extends Map<K, V>> View<K, V> get(final AbstractCopyOnWriteMap<K, V, M> host) {
                    return host.new Immutable();
                }
            },
            LIVE {
                @Override
                <K, V, M extends Map<K, V>> View<K, V> get(final AbstractCopyOnWriteMap<K, V, M> host) {
                    return host.new Mutable();
                }
            };
            abstract <K, V, M extends Map<K, V>> View<K, V> get(AbstractCopyOnWriteMap<K, V, M> host);
        }
    }

    final class Immutable extends View<K, V> implements Serializable {

        private static final long serialVersionUID = -4158727180429303818L;

        @Override
        public Set<K> keySet() {
            return unmodifiableSet(delegate.keySet());
        }

        @Override
        public Set<Entry<K, V>> entrySet() {
            return unmodifiableSet(delegate.entrySet());
        }

        @Override
        public Collection<V> values() {
            return unmodifiableCollection(delegate.values());
        }
    }

    final class Mutable extends View<K, V> implements Serializable {

        private static final long serialVersionUID = 1624520291194797634L;

        private final transient KeySet keySet = new KeySet();
        private final transient EntrySet entrySet = new EntrySet();
        private final transient Values values = new Values();

        @Override
        public Set<K> keySet() {
            return keySet;
        }

        @Override
        public Set<Entry<K, V>> entrySet() {
            return entrySet;
        }

        @Override
        public Collection<V> values() {
            return values;
        }
    }
}
