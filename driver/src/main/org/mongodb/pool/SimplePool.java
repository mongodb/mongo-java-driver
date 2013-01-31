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
public abstract class SimplePool<T> {

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
     * override this if you need to do any cleanup
     */
    public void cleanup(final T t) {
    }

    /**
     * Pick a member of {@code available}.  This method is called with a lock held on {@code available}, so it may be used
     * safely.
     *
     * @param recommended the recommended member to choose.
     * @param couldCreate true if there is room in the pool to create a new object
     * @return >= 0 the one to use, -1 create a new one
     */
    protected int pick(final int recommended, final boolean couldCreate) {
        return recommended;
    }

    /**
     * call done when you are done with an object form the pool if there is room and the object is ok will get added
     *
     * @param t Object to add
     */
    public void done(final T t) {
        synchronized (this) {
            if (closed) {
                cleanup(t);
                return;
            }

            assertConditions();

            if (!out.remove(t)) {
                throw new RuntimeException("trying to put something back in the pool wasn't checked out");
            }

            available.add(t);

        }
        semaphore.release();
    }

    private void assertConditions() {
        assert getTotal() <= getMaxSize();
    }

    public void remove(final T t) {
        done(t);
    }

    /**
     * Gets an object from the pool - will block if none are available
     *
     * @return An object from the pool
     */
    public T get() {
        return get(-1);
    }

    /**
     * Gets an object from the pool - will block if none are available
     *
     * @param waitTime negative - forever 0        - return immediately no matter what positive ms to wait
     * @return An object from the pool, or null if can't get one in the given waitTime
     */
    public T get(final long waitTime) {
        if (closed) {
            throw new IllegalStateException("The pool is closed");
        }
        if (!permitAcquired(waitTime)) {
            return null;
        }

        synchronized (this) {
            assertConditions();

            final int toTake = pick(available.size() - 1, getTotal() < getMaxSize());
            final T t;
            if (toTake >= 0) {
                t = available.remove(toTake);
            } else {
                t = createNewAndReleasePermitIfFailure();
            }
            out.put(t, true);

            if (out.size() > 1000) {
                System.out.println("oops");
            }

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

    private boolean permitAcquired(final long waitTime) {
        try {
            if (waitTime > 0) {
                return semaphore.tryAcquire(waitTime, TimeUnit.MILLISECONDS);
            } else if (waitTime < 0) {
                semaphore.acquire();
                return true;
            } else {
                return semaphore.tryAcquire();
            }
        } catch (InterruptedException e) {
            throw new MongoInterruptedException("Interrupted in pool " + getName(), e);
        }
    }

    /**
     * Clears the pool of all objects.
     */
    public synchronized void close() {
        closed = true;
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
