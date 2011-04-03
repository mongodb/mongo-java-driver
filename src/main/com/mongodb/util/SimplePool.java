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
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import javax.management.*;

public abstract class SimplePool<T> implements DynamicMBean {

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
        _mbeanInfo = new MBeanInfo( this.getClass().getName() , _name , 
                new MBeanAttributeInfo[]{
                    new MBeanAttributeInfo( "name" , "java.lang.String" , "name of pool" , true , false , false ) , 
                    new MBeanAttributeInfo( "size" , "java.lang.Integer" , "total size of pool" , true , false , false ) , 
                    new MBeanAttributeInfo( "available" , "java.lang.Integer" , "total connections available" , true , false , false ) , 
                    new MBeanAttributeInfo( "inUse" , "java.lang.Integer" , "number connections in use right now" , true , false , false ) , 
                    new MBeanAttributeInfo( "everCreated" , "java.lang.Integer" , "number connections ever created" , true , false , false ) 
                } , null , null , null );
        
    }

    /** Creates a new object of this pool's type.
     * @return the new object.
     */
    protected abstract T createNew();

    /** 
     * callback to determine if an object is ok to be added back to the pool or used
     * will be called when something is put back into the queue and when it comes out
     * @return true if the object is ok to be added back to pool
     */
    public boolean ok( T t ){
        return true;
    }

    /**
     * override this if you need to do any cleanup
     */
    public void cleanup( T t ){}

    /**
     * @return >= 0 the one to use, -1 don't use any
     */
    protected int pick( int iThink , boolean couldCreate ){
        return iThink;
    }

    /** 
     * call done when you are done with an object form the pool
     * if there is room and the object is ok will get added
     * @param t Object to add
     */
    public void done( T t ){
        done( t , ok( t ) );
    }

    void done( T t , boolean ok ){
        if ( _trackLeaks ){
            synchronized ( _where ){
                _where.remove( _hash( t ) );
            }
        }
        
        if ( ! ok ){
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
            else {
                cleanup( t );
            }
        }
    }

    public void remove( T t ){
        done( t , false );
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
                
                boolean couldCreate = _maxTotal <= 0 || _all.size() < _maxTotal;

                while ( _avail.size() > 0 ){
                    int toTake = _avail.size() - 1;
                    toTake = pick( toTake, couldCreate );
                    if ( toTake >= 0 ){
                        T t = _avail.remove( toTake );
                        if ( ok( t ) ){
                            _debug( "got an old one" );
                            return t;
                        }
                        _debug( "old one was not ok" );
                        _all.remove( t );
                        continue;
                    }
                    else if ( ! couldCreate ) {
                        throw new IllegalStateException( "can't pick nothing if can't create" );
                    }
                    break;
                }
                
                if ( couldCreate ){
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
	    
            long start = System.currentTimeMillis();
            try {
                _waiting.tryAcquire( _sleepTime , TimeUnit.MILLISECONDS );
            }
            catch ( InterruptedException ie ){
            }

	    totalSlept += ( System.currentTimeMillis() - start );

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
        synchronized( _avail ){
            for ( T t : _avail )
                cleanup( t );
            _avail.clear();
            _all.clear();
            synchronized ( _where ){
                _where.clear(); // is this correct
            }
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

    public Object getAttribute(String attribute){
        if ( attribute.equals( "name" ) )
            return _name;
        if ( attribute.equals( "size" ) )
            return _maxToKeep;
        if ( attribute.equals( "available" ) )
            return available();
        if ( attribute.equals( "inUse" ) )
            return inUse();
        if ( attribute.equals( "everCreated" ) )
            return _everCreated;
        
        System.err.println( "com.mongo.util.SimplePool unknown attribute: " + attribute );
        throw new RuntimeException( "unknown attribute: " + attribute );
    }
    
    public AttributeList getAttributes(String[] attributes){
        AttributeList l = new AttributeList();
        for ( int i=0; i<attributes.length; i++ ){
            String name = attributes[i];
            l.add( new Attribute( name , getAttribute( name ) ) );
        }
        return l;
    }

    public MBeanInfo getMBeanInfo(){
        return _mbeanInfo;
    }

    public Object invoke(String actionName, Object[] params, String[] signature){
        throw new RuntimeException( "not allowed to invoke anything" );
    }

    public void setAttribute(Attribute attribute){
        throw new RuntimeException( "not allowed to set anything" );
    }
    
    public AttributeList setAttributes(AttributeList attributes){
        throw new RuntimeException( "not allowed to set anything" );
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
    protected final MBeanInfo _mbeanInfo;

    private final List<T> _avail = new ArrayList<T>();
    protected final List<T> _availSafe = Collections.unmodifiableList( _avail );
    private final WeakBag<T> _all = new WeakBag<T>();
    private final Map<Integer,Throwable> _where = new HashMap<Integer,Throwable>();

    private final Semaphore _waiting = new Semaphore(0);

    private int _everCreated = 0;
    private int _trackPrintCount = 0;
    
}
