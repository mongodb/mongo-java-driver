// CircularList.java

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

public class CircularList<T> {

    /**
     * @param stack if true, this list operates like a stack.  so get(0) will always return that last thing put in
                    if false, it acts like a list, so get(0) will return the oldest entry still in the list
     */
    public CircularList( int capacity , boolean stack ){
        _array = (T[])(new Object[capacity]);
        _pos = 0;
        _size = 0;
        _stack = stack;
    }
    
    public void clear(){
        _size = 0;
        _pos = 0;
        for ( int i=0; i<_array.length; i++ )
            _array[i] = null;
    }
    
    public void add( T t ){
        
        _array[_pos++] = t;
        if ( _pos == _array.length )
            _pos = 0;

        if ( _size < _array.length )
            _size++;
    }

    public T get( int i ){
        int pos = 
            _stack ? 
            ( _pos - ( 1 + i ) ) :
            ( ( _pos - _size ) + i );
        if ( pos < 0 )
            pos += _array.length;
        return _array[pos];
    }
    
    public int size(){
        return _size;
    }

    public int capacity(){
        return _array.length;
    }

    public String toString(){
        StringBuilder buf = new StringBuilder("[" );
        
        boolean first = true;
        for ( int i=0; i<size(); i++ ){
            if ( first )
                first = false;
            else
                buf.append( ", " );
            buf.append( get( i )  );
        }

        buf.append( "]" );
        return buf.toString();
    }

    final T[] _array;
    final boolean _stack;

    private int _pos;
    private int _size;
}
