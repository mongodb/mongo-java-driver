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

import java.util.ArrayList;
import java.util.List;

/**
 * @deprecated This class is NOT a part of public API and will be dropped in 3.x versions.
 */
@Deprecated
public class FastStack<T>{

    public void push( T t ){
        _data.add( t );
    }
    
    public T peek(){
        return _data.get( _data.size() - 1 );
    }

    public T pop(){
        return _data.remove( _data.size() - 1 );
    }

    public int size(){
        return _data.size();
    }

    public void clear(){
        _data.clear();
    }

    public T get( int i ){
        return _data.get( i );
    }
    
    public String toString(){
        return _data.toString();
    }

    private final List<T> _data = new ArrayList<T>();
}
