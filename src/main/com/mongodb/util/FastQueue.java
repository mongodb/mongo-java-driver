// FastStack.java

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

public class FastQueue<T>{

    public boolean add( T t ){
        return _data.add( t );
    }
    
    public T peek(){
        if( size() == 0 )
            return null;
        return _data.get( 0 );
    }

    public T poll(){
        if( size() == 0 ) 
            return null;
        return _data.remove( 0 );
    }

    public int size(){
        return _data.size();
    }

    public void clear(){
        _data.clear();
    }

    private final List<T> _data = new LinkedList<T>();
}
