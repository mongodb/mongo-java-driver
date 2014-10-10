/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

// SimplePool.java

package org.bson.util;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @deprecated This class is NOT a part of public API and will be dropped in 3.x versions.
 */
@Deprecated
@SuppressWarnings("JavaDoc")
public abstract class SimplePool<T> {

    @SuppressWarnings("JavaDoc")
    public SimplePool( int max ){
        _max = max;
    }

    @SuppressWarnings("JavaDoc")
    public SimplePool(){
        _max = 1000;
    }

    @SuppressWarnings("JavaDoc")
    protected abstract T createNew();

    @SuppressWarnings("JavaDoc")
    protected boolean ok( T t ){
        return true;
    }

    @SuppressWarnings("JavaDoc")
    public T get(){
        T t = _stored.poll();
        if ( t != null )
            return t;
        return createNew();
    }

    @SuppressWarnings("JavaDoc")
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
