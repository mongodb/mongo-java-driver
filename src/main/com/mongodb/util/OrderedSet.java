// OrderedSet.java

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

public class OrderedSet<T> extends AbstractSet<T> {
    
    public OrderedSet( Collection<T> copy ){
        this();
        addAll( copy );
    }

    public OrderedSet(){
        _list = new ArrayList<T>();
    }

    public boolean add(T t) {
        if ( _list.contains( t ) )
            return false;
        _list.add( t );
        return true;
    }

    public int size(){
        return _list.size();
    }

    public Iterator<T> iterator(){
        return _list.iterator();
    }
    
    final List<T> _list;
    
}
