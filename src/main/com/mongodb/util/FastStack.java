// FastStack.java

/**
*    Copyright (C) 2008 10gen Inc.
*  
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*  
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU Affero General Public License for more details.
*  
*    You should have received a copy of the GNU Affero General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package com.mongodb.util;

import java.util.*;

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
