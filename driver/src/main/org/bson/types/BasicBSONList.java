/*
 * Copyright 2008-2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// BasicBSONList.java

package org.bson.types;

import org.bson.BSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * <p>Utility class to allow array {@code DBObject}s to be created. <p> Note: MongoDB will also create arrays from {@code java.util
 * .List}s.</p>
 * <pre>
 * BSONObject obj = new BasicBSONList();
 * obj.put( "0", value1 );
 * obj.put( "4", value2 );
 * obj.put( 2, value3 );
 * </pre>
 * <p>This simulates the array [ value1, null, value3, null, value2 ] by creating the {@code DBObject} {@code { "0" : value1, "1" : null,
 * "2" : value3, "3" : null, "4" : value2 }}. </p>
 *
 * <p>BasicBSONList only supports numeric keys.  Passing strings that cannot be converted to ints
 * will cause an IllegalArgumentException.</p>
 * <pre>
 * BasicBSONList list = new BasicBSONList();
 * list.put("1", "bar"); // ok
 * list.put("1E1", "bar"); // throws exception
 * </pre>
 */
@SuppressWarnings("rawtypes")
public class BasicBSONList extends ArrayList<Object> implements BSONObject {

    private static final long serialVersionUID = -4415279469780082174L;

    /**
     * Puts a value at an index. For interface compatibility.  Must be passed a String that is parsable to an int.
     *
     * @param key the index at which to insert the value
     * @param v   the value to insert
     * @return the value
     * @throws IllegalArgumentException if {@code key} cannot be parsed into an {@code int}
     */
    @Override
    public Object put(final String key, final Object v) {
        return put(_getInt(key), v);
    }

    /**
     * Puts a value at an index. This will fill any unset indexes less than {@code index} with {@code null}.
     *
     * @param key the index at which to insert the value
     * @param value   the value to insert
     * @return the value
     */
    public Object put(final int key, final Object value) {
        while (key >= size()) {
            add(null);
        }
        set(key, value);
        return value;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void putAll(final Map m) {
        for (final Map.Entry entry : (Set<Map.Entry>) m.entrySet()) {
            put(entry.getKey().toString(), entry.getValue());
        }
    }

    @Override
    public void putAll(final BSONObject o) {
        for (final String k : o.keySet()) {
            put(k, o.get(k));
        }
    }

    /**
     * Gets a value at an index. For interface compatibility.  Must be passed a String that is parsable to an int.
     *
     * @param key the index
     * @return the value, if found, or null
     * @throws IllegalArgumentException if {@code key} cannot be parsed into an {@code int}
     */
    public Object get(final String key) {
        int i = _getInt(key);
        if (i < 0) {
            return null;
        }
        if (i >= size()) {
            return null;
        }
        return get(i);
    }

    @Override
    public Object removeField(final String key) {
        int i = _getInt(key);
        if (i < 0) {
            return null;
        }
        if (i >= size()) {
            return null;
        }
        return remove(i);
    }

    @Override
    @Deprecated
    public boolean containsKey(final String key) {
        return containsField(key);
    }

    @Override
    public boolean containsField(final String key) {
        int i = _getInt(key, false);
        if (i < 0) {
            return false;
        }
        return i >= 0 && i < size();
    }

    @Override
    public Set<String> keySet() {
        return new StringRangeSet(size());
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map toMap() {
        Map m = new HashMap();
        Iterator i = this.keySet().iterator();
        while (i.hasNext()) {
            Object s = i.next();
            m.put(s, this.get(String.valueOf(s)));
        }
        return m;
    }

    int _getInt(final String s) {
        return _getInt(s, true);
    }

    int _getInt(final String s, final boolean err) {
        try {
            return Integer.parseInt(s);
        } catch (Exception e) {
            if (err) {
                throw new IllegalArgumentException("BasicBSONList can only work with numeric keys, not: [" + s + "]");
            }
            return -1;
        }
    }
}
