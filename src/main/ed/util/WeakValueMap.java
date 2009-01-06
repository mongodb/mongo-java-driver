// WeakValueMap.java

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

import java.lang.ref.*;
import java.util.*;

/** @expose */
public class WeakValueMap<K,V> /* implements Map<K,V> */ {

    /** Initializes a new values map */
    public WeakValueMap(){
        _map = new HashMap<K,WeakReference<V>>();
    }

    /** Gets an object with a given key from this map
     * @param key name of the object to find
     * @return the object, if found
     */
    public V get( Object key ){
        WeakReference<V> r = _map.get( key );
        if ( r == null )
            return null;

        V v = r.get();
        if ( v == null )
            _map.remove( key );

        return v;
    }

    /** Adds an object to this map
     * @param key key of object to add
     * @param value value of object to add
     * @return value of added object
     */
    public V put( K key , V v ){
        final WeakReference<V> nr = new WeakReference<V>( v );
        final WeakReference<V> or = _map.put( key , nr );
        if ( or == null )
            return null;
        return or.get();
    }

    /** @unexpose */
    final Map<K,WeakReference<V>> _map;
}
