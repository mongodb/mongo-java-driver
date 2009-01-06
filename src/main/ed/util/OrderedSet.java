// OrderedSet.java

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
