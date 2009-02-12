// WeakValueMap.java

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

import java.lang.ref.*;
import java.util.*;

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
     * @param v value of object to add
     * @return value of added object
     */
    public V put( K key , V v ){
        final WeakReference<V> nr = new WeakReference<V>( v );
        final WeakReference<V> or = _map.put( key , nr );
        if ( or == null )
            return null;
        return or.get();
    }

    final Map<K,WeakReference<V>> _map;
}
