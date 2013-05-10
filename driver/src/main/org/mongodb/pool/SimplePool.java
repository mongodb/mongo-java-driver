/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.pool;

import org.mongodb.MongoInterruptedException;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * This class is NOT part of the public API.  Be prepared for non-binary compatible changes in minor releases.
 */
public abstract class SimplePool<T> implements Pool<T> {

    private final String name;
    private final int size;

    private final List<T> available = new ArrayList<T>();
    private final Map<T, Boolean> out = new IdentityHashMap<T, Boolean>();
    private final Semaphore semaphore;
    private volatile boolean closed;

    /**
     * Initializes a new pool of objects.
     *
     * @param name name for the pool
     * @param size max to hold to at any given time. if < 0 then no limit
     */
    public SimplePool(final String name, final int size) {
        this.name = name;
        this.size = size;
        semaphore = new Semaphore(size);
    }

    /**
     * Creates a new object of this pool's type.  Implementations should throw a runtime exception if unable to create.
     *
     * @return the new object.
     */
    protected abstract T createNew();

    /**
     * Override this if you need to do any cleanup of items when the pool is closed.
     */
    protected void cleanup(final T t) {
    }

    /**
     * call done when you are done with an object from the pool if there is room and the object is ok will get added
     *
     * @param t item to return to the pool
     */
    public void done(final T t) {
        done(t, false);
    }

    /**
     * call done when you are done with an object from the pool if there is room and the object is ok will get added
     *
     * @param t       item to return to the pool
     * @param discard true if the item should be discarded, false if it should be back in the pool
     */
    public void done(final T t, final boolean discard) {
        if (t == null) {
            throw new IllegalArgumentException("Can not return a null item to the pool");
        }
        synchronized (this) {
            if (closed) {
                cleanup(t);
                return;
            }

            assertConditions();

            if (!out.remove(t)) {
                return;
            }

            if (!discard) {
                available.add(t);
            }
        }
        semaphore.release();
    }

    private void assertConditions() {
        assert getTotal() <= getMaxSize();
    }

    /**
     * Gets an object from the pool - will block if none are available
     *
     * @return An object from the pool
     */
    @Override
    public T get() {
        return get(-1, TimeUnit.MILLISECONDS);
    }

    /**
     * Gets an object from the pool - will block if none are available
     *
     * @param timeout  negative - forever 0        - return immediately no matter what positive ms to wait
     * @param timeUnit
     * @return An object from the pool, or null if can't get one in the given waitTime
     */
    @Override
    public T get(final long timeout, final TimeUnit timeUnit) {
        if (closed) {
            throw new IllegalStateException("The pool is closed");
        }

        if (!permitAcquired(timeout, timeUnit)) {
            return null;
        }

        synchronized (this) {
            assertConditions();

            final int toTake = available.size() - 1;
            final T t;
            if (toTake >= 0) {
                t = available.remove(toTake);
            }
            else {
                t = createNewAndReleasePermitIfFailure();
            }
            out.put(t, true);

            return t;
        }
    }

    private T createNewAndReleasePermitIfFailure() {
        try {
            final T newMember = createNew();
            if (newMember == null) {
                throw new IllegalStateException("null pool members are not allowed");
            }
            return newMember;
        } catch (RuntimeException e) {
            semaphore.release();
            throw e;
        } catch (Error e) {
            semaphore.release();
            throw e;
        }
    }

    private boolean permitAcquired(final long timeout, final TimeUnit timeUnit) {
        try {
            if (timeout > 0) {
                return semaphore.tryAcquire(timeout, timeUnit);
            }
            else if (timeout < 0) {
                semaphore.acquire();
                return true;
            }
            else {
                return semaphore.tryAcquire();
            }
        } catch (InterruptedException e) {
            throw new MongoInterruptedException("Interrupted in pool " + getName(), e);
        }
    }

    /**
     * Clears the pool of all objects.
     */
    @Override
    public synchronized void close() {
        closed = true;
        clear();
    }

    public void clear() {
        for (final T t : available) {
            cleanup(t);
        }
        available.clear();
        out.clear();
    }

    public String getName() {
        return name;
    }

    public synchronized int getTotal() {
        return available.size() + out.size();
    }

    public synchronized int getInUse() {
        return out.size();
    }

    public synchronized int getNumberAvailable() {
        return available.size();
    }

    public int getMaxSize() {
        return size;
    }

    public synchronized String toString() {
        final StringBuilder buf = new StringBuilder();
        buf.append("pool: ").append(name)
                .append(" maxToKeep: ").append(size)
                .append(" avail ").append(available.size())
                .append(" out ").append(out.size());
        return buf.toString();
    }

    protected int getSize() {
        return size;
    }
}
