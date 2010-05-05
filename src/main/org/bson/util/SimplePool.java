// SimplePool.java

package org.bson.util;

import java.util.*;

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
        synchronized ( _stored ){
            if ( _stored.size() > 0 ) 
                return _stored.removeFirst();
        }
        return createNew();
    }

    public void done( T t ){
        if ( ! ok( t ) )
            return;
        synchronized ( _stored ){
            if ( _stored.size() > _max )
                return;
            _stored.addFirst( t );
        }
    }
    
    final int _max;
    private LinkedList<T> _stored = new LinkedList<T>();
}
