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
