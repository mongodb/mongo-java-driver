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

import java.util.Map;

public class MapEntryImpl<K,V> implements Map.Entry<K, V> {
    
    public MapEntryImpl(K key, V value) {
        _key = key;
        _value = value;
    }
    
    public K getKey() {
        return _key;
    }
    
    public V getValue() {
        return _value;
    }
    
    public V setValue(V value) {
        throw new UnsupportedOperationException();
    }
    
    private final K _key;
    private V _value;
}
