// SimplePool.java

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

package com.mongodb.util;

import java.util.*;
import java.util.concurrent.*;

public abstract class SimplePool<T> {

    static final boolean TRACK_LEAKS = Boolean.getBoolean( "MONGO-TRACKLEAKS" );
    static final long _sleepTime = 2;
    
    /** 
     * See full constructor docs
     */
    public SimplePool( String name , int maxToKeep , int maxTotal ){
        this( name , maxToKeep , maxTotal , false , false );
    } 
    
    /** Initializes a new pool of objects.
     * @param name name for the pool
     * @param maxToKeep max to hold to at any given time. if < 0 then no limit
     * @param maxTotal max to have allocated at any point.  if there are no more, get() will block
     * @param trackLeaks if leaks should be tracked
     */
    public SimplePool( String name , int maxToKeep , int maxTotal , boolean trackLeaks , boolean debug ){
        _name = name;
        _maxToKeep = maxToKeep;
        _maxTotal = maxTotal;
        _trackLeaks = trackLeaks || TRACK_LEAKS;
        _debug = debug;
        
    }

    /** Creates a new object of this pool's type.
     * @return the new object.
     */
    protected abstract T createNew();

    /** 
     * callback to determine if an object is ok to be added back to the pool or used
     * will be called when something is put back into the queue and when it comes out
     * @return true iff the object is ok to be added back to pool
     */
    public boolean ok( T t ){
        return true;
    }

    /** 
     * call done when you are done with an object form the pool
     * if there is room and the object is ok will get added
     * @param t Object to add
     */
    public void done( T t ){
        if ( _trackLeaks ){
            synchronized ( _where ){
                _where.remove( _hash( t ) );
            }
        }
        
        if ( ! ok( t ) ){
            synchronized ( _avail ){
                _all.remove( t );
            }
            return;
        }

        synchronized ( _avail ){
            if ( _maxToKeep < 0 || _avail.size() < _maxToKeep ){
                for ( int i=0; i<_avail.size(); i++ )
                    if ( _avail.get( i ) == t )
                        throw new RuntimeException( "trying to put something back in the pool that's already there" );
                
                // if all doesn't contain it, it probably means this was cleared, so we don't want it
                if ( _all.contains( t ) ){
                    _avail.add( t );
                    _waiting.release();
                }
            }
        }
    }

    /** Gets an object from the pool - will block if none are available
     * @return An object from the pool
     */
    public T get(){
	return get(-1);
    }
    
    /** Gets an object from the pool - will block if none are available
     * @param waitTime 
     *        negative - forever
     *        0        - return immediately no matter what
     *        positive ms to wait
     * @return An object from the pool
     */
    public T get( long waitTime ){
        final T t = _get( waitTime );
        if ( t != null ){
            _consecutiveSleeps = 0;
            if ( _trackLeaks ){
                Throwable stack = new Throwable();
                stack.fillInStackTrace();
                synchronized ( _where ){
                    _where.put( _hash( t ) , stack );
                }
            }
        }
        return t;
    }

    private int _hash( T t ){
        return System.identityHashCode( t );
    }
    
    private T _get( long waitTime ){
	long totalSlept = 0;
        while ( true ){
            synchronized ( _avail ){

                while ( _avail.size() > 0 ){
                    T t = _avail.remove( _avail.size() - 1 );
                    if ( ok( t ) ){
                        _debug( "got an old one" );
                        return t;
                    }
                    _debug( "old one was not ok" );
                    _all.remove( t );
                    continue;
                }

                if ( _maxTotal <= 0 || _all.size() < _maxTotal ){
                    _everCreated++;
                    T t = createNew();
                    _all.add( t );
                    return t;
                }
		
                if ( _trackLeaks && _trackPrintCount++ % 200 == 0 ){
                    _wherePrint();
                    _trackPrintCount = 1;
                }
            }
	    
	    if ( waitTime == 0 )
		return null;

	    if ( waitTime > 0 && totalSlept >= waitTime )
		return null;
	    
            if ( _consecutiveSleeps > 100 && totalSlept > _sleepTime * 2 )
                _gcIfNeeded();

            _consecutiveSleeps++;
	    totalSlept += _sleepTime;
            
            try {
                _waiting.tryAcquire( _sleepTime , TimeUnit.MILLISECONDS );
            }
            catch ( InterruptedException ie ){
            }

        }
    }

    private void _wherePrint(){
        StringBuilder buf = new StringBuilder( toString() ).append( " waiting \n" );
        synchronized ( _where ){
            for ( Throwable t : _where.values() ){
                buf.append( "--\n" );
                final StackTraceElement[] st = t.getStackTrace();
                for ( int i=0; i<st.length; i++ )
                    buf.append( "  " ).append( st[i] ).append( "\n" );
                buf.append( "----\n" );
            }
        }
        System.out.println( buf );
    }

    /** Clears the pool of all objects. */
    protected void clear(){
        _avail.clear();
        _all.clear();
        synchronized ( _where ){
            _where.clear(); // is this correct
        }
    }

    public int total(){
        return _all.size();
    }
    
    public int inUse(){
        return _all.size() - _avail.size();
    }

    public Iterator<T> getAll(){
        return _all.getAll().iterator();
    }

    public int available(){
        if ( _maxTotal <= 0 )
            throw new IllegalStateException( "this pool has an infinite number of things available" );
        return _maxTotal - inUse();
    }

    public int everCreated(){
        return _everCreated;
    }

    private void _debug( String msg ){
        if( _debug )
            System.out.println( "SimplePool [" + _name + "] : " + msg );
    }

    public int maxToKeep(){
        return _maxToKeep;
    }

    public String toString(){
        StringBuilder buf = new StringBuilder();
        buf.append( "pool: " ).append( _name )
            .append( " maxToKeep: " ).append( _maxToKeep )
            .append( " maxTotal: " ).append( _maxToKeep )
            .append( " where " ).append( _where.size() )
            .append( " avail " ).append( _avail.size() )
            .append( " all " ).append( _all.size() )
            ;
        return buf.toString();
    }

    protected final String _name;
    protected final int _maxToKeep;
    protected final int _maxTotal;
    protected final boolean _trackLeaks;
    protected final boolean _debug;

    private final List<T> _avail = new ArrayList<T>();
    private final WeakBag<T> _all = new WeakBag<T>();
    private final Map<Integer,Throwable> _where = new HashMap<Integer,Throwable>();

    private final Semaphore _waiting = new Semaphore(0);

    private int _everCreated = 0;
    private int _trackPrintCount = 0;
    private int _consecutiveSleeps = 0;


    private static void _gcIfNeeded(){
        final long now = System.currentTimeMillis();
        if ( now < _nextGC )
            return;

        _nextGC = now + 5000;
        System.gc();
    }
    private static long _nextGC = 0;
    
}
