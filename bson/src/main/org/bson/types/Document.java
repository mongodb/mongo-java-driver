/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.bson.types;

import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class Document implements Map<String, Object>, Serializable {
    private static final long serialVersionUID = 6297731997167536582L;

    private final LinkedHashMap<String, Object> documentAsMap = new LinkedHashMap<String, Object>();

    public Document() {
    }

    public Document(final String key, final Object value) {
        documentAsMap.put(key, value);
    }

    public Document append(final String key, final Object value) {
        documentAsMap.put(key, value);
        return this;
    }

    // Vanilla Map methods delegate to map field
    public int size() {
        return documentAsMap.size();
    }

    public boolean isEmpty() {
        return documentAsMap.isEmpty();
    }

    public boolean containsValue(final Object o) {
        return documentAsMap.containsValue(o);
    }

    public boolean containsKey(final Object o) {
        return documentAsMap.containsKey(o);
    }

    public Object get(final Object o) {
        return documentAsMap.get(o);
    }

    public Object put(final String s, final Object o) {
        return documentAsMap.put(s, o);
    }

    public Object remove(final Object o) {
        return documentAsMap.remove(o);
    }

    public void putAll(final Map<? extends String, ?> map) {
        documentAsMap.putAll(map);
    }

    public void clear() {
        documentAsMap.clear();
    }

    public Set<String> keySet() {
        return documentAsMap.keySet();
    }

    public Collection<Object> values() {
        return documentAsMap.values();
    }

    public Set<Map.Entry<String, Object>> entrySet() {
        return documentAsMap.entrySet();
    }

    public boolean equals(final Object o) {
        return documentAsMap.equals(o);
    }

    public int hashCode() {
        return documentAsMap.hashCode();
    }

    public String toString() {
        return documentAsMap.toString();
    }

}
