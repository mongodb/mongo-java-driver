// SimpleStack.java

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

package ed.util;

import java.util.*;

public class SimpleStack<T>{

    public void push( T t ){

        if ( _size == 0 ){
            _last = t;
            _size++;
            return;
        }
        
        if ( _extra == null )
            _extra = new LinkedList<T>();
        _extra.addLast( _last );
        _last = t;
        _size++;
    }

    public T peek(){
        return _last;
    }

    public T pop(){
        T ret = _last;
        _size--;
        if ( _size != 0 )
            _last = _extra.removeLast();
        else 
            if ( _extra != null && _extra.size() > 0 )
                throw new RuntimeException( "something is wrong" );
        return ret;
    }

    public int size(){
        return _size;
    }

    public void clear(){
        _size = 0;
        _last = null;
        if ( _extra != null )
            _extra.clear();
    }

    public T get( int i ){
        if ( i < 0 || i >= _size )
            throw new IllegalArgumentException( "out of range : " + i );
        if ( i == _size - 1 )
            return _last;
        return _extra.get(i);
    }

    private int _size = 0;
    private T _last;
    private LinkedList<T> _extra;
}
