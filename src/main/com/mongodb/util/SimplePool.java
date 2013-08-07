/**
 *      Copyright (C) 2008-2012 10gen Inc.
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

package com.mongodb.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * @deprecated This class is NOT a part of public API and will be dropped in 3.x versions.
 */
@Deprecated
public abstract class SimplePool<T> {

    /** Initializes a new pool of objects.
     * @param name name for the pool
     * @param size max to hold to at any given time. if < 0 then no limit
     */
    public SimplePool(String name, int size){
        _name = name;
        _size = size;
        _sem = new Semaphore(size);
    }

    /** Creates a new object of this pool's type.  Implementations should throw a runtime exception if unable to create.
     * @return the new object.
     */
    protected abstract T createNew();

    /**
     * override this if you need to do any cleanup
     */
    public void cleanup( T t ) {
    }

    /**
     * Pick a member of {@code _avail}.  This method is called with a lock held on {@code _avail}, so it may be used safely.
     *
     * @param recommended the recommended member to choose.
     * @param couldCreate  true if there is room in the pool to create a new object
     * @return >= 0 the one to use, -1 create a new one
     */
    protected int pick( int recommended , boolean couldCreate ){
        return recommended;
    }

    /**
     * call done when you are done with an object form the pool
     * if there is room and the object is ok will get added
     * @param t Object to add
     */
    public void done( T t ){
        synchronized ( this ) {
            if (_closed) {
                cleanup(t);
                return;
            }

            assertConditions();

            if (!_out.remove(t)) {
                throw new RuntimeException("trying to put something back in the pool wasn't checked out");
            }

            _avail.add(t);

        }
        _sem.release();
    }

    private void assertConditions() {
        assert getTotal() <= getMaxSize();
    }

    public void remove( T t ) {
        done(t);
    }

    /** Gets an object from the pool - will block if none are available
     * @return An object from the pool
     */
    public T get() throws InterruptedException {
	return get(-1);
    }
    
    /** Gets an object from the pool - will block if none are available
     * @param waitTime 
     *        negative - forever
     *        0        - return immediately no matter what
     *        positive ms to wait
     * @return An object from the pool, or null if can't get one in the given waitTime
     */
    public T get(long waitTime) throws InterruptedException {
        if (!permitAcquired(waitTime)) {
            return null;
        }

        synchronized (this) {
            assertConditions();

            int toTake = pick(_avail.size() - 1, getTotal() < getMaxSize());
            T t;
            if (toTake >= 0) {
                t = _avail.remove(toTake);
            } else {
                t = createNewAndReleasePermitIfFailure();
            }
            _out.add(t);

            return t;
        }
    }

    private T createNewAndReleasePermitIfFailure() {
        try {
            T newMember = createNew();
            if (newMember == null) {
                throw new IllegalStateException("null pool members are not allowed");
            }
            return newMember;
        } catch (RuntimeException e) {
            _sem.release();
            throw e;
        } catch (Error e) {
            _sem.release();
            throw e;
        }
    }

    private boolean permitAcquired(final long waitTime) throws InterruptedException {
        if (waitTime > 0) {
            return _sem.tryAcquire(waitTime, TimeUnit.MILLISECONDS);
        } else if (waitTime < 0) {
            _sem.acquire();
            return true;
        } else {
            return _sem.tryAcquire();
        }
    }

    /** Clears the pool of all objects. */
    protected synchronized void close(){
        _closed = true;
        for (T t : _avail)
            cleanup(t);
        _avail.clear();
        _out.clear();
    }

    public String getName() {
        return _name;
    }

    public synchronized int getTotal(){
        return _avail.size() + _out.size();
    }
    
    public synchronized int getInUse(){
        return _out.size();
    }

    public synchronized int getAvailable(){
        return _avail.size();
    }

    public int getMaxSize(){
        return _size;
    }

    public synchronized String toString(){
        StringBuilder buf = new StringBuilder();
        buf.append("pool: ").append(_name)
            .append(" maxToKeep: ").append(_size)
            .append(" avail ").append(_avail.size())
            .append(" out ").append(_out.size())
            ;
        return buf.toString();
    }

    protected final String _name;
    protected final int _size;

    protected final List<T> _avail = new ArrayList<T>();
    protected final Set<T> _out = new HashSet<T>();
    private final Semaphore _sem;
    private boolean _closed;
}
