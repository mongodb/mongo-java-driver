// CollectionEnumeration.java

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

public class CollectionEnumeration<E> implements Enumeration<E> {

    public CollectionEnumeration( Collection<E> c ){
        _it = c.iterator();
    }

    public boolean hasMoreElements(){
        return _it.hasNext();
    }

    public E nextElement(){
        return _it.next();
    }

    private final Iterator<E> _it;
}
