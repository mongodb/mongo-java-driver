// UniqueList.java

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

public class UniqueList<T> extends ArrayList<T> {

    public boolean add( T t ){
        if ( contains( t ) )
            return false;
        return super.add( t );
    }
    
    public boolean addAll(Collection<? extends T> c) {
        boolean added = false;
        for ( T t : c )
            added = added || add( t );
        return added;
    }

}
