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

package org.bson.util;

import java.util.*;
import java.util.concurrent.*;

/**
 * @deprecated This class is NOT a part of public API and will be dropped in 3.x versions.
 */
@Deprecated
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
