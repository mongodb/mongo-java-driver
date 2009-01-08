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
