// SimplePool.java

package org.bson.util;

import java.util.*;
import java.util.concurrent.*;

public abstract class SimplePool<T> {

    public SimplePool( int max ){
        _max = max;
    }

    public SimplePool(){
        _max = 1000;
    }
    
    protected abstract T createNew();

    protected boolean ok( T t ){
        return true;
    }
    
    public T get(){
        T t = _stored.poll();
        if ( t != null )
            return t;
        return createNew();
    }

    public void done( T t ){
        if ( ! ok( t ) )
            return;
        
        if ( _stored.size() > _max )
            return;
        _stored.add( t );
    }
    
    final int _max;
    private Queue<T> _stored = new ConcurrentLinkedQueue<T>();
}
