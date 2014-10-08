/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb;

import org.bson.BSONObject;
import org.bson.LazyBSONCallback;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LazyWriteableDBObject extends LazyDBObject {

    private final Map<String, Object> writable = new HashMap<String, Object>();

    public LazyWriteableDBObject(final byte[] bytes, final LazyBSONCallback callback) {
        super(bytes, callback);
    }

    public LazyWriteableDBObject(final byte[] bytes, final int offset, final LazyBSONCallback callback) {
        super(bytes, offset, callback);
    }

    @Override
    public Object put(final String key, final Object value) {
        return writable.put(key, value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void putAll(final BSONObject document) {
        putAll(document.toMap());
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void putAll(final Map m) {
        writable.putAll(m);
    }

    @Override
    public boolean containsField(final String s) {
        return writable.containsKey(s) || super.containsField(s);
    }

    @Override
    public Object get(final String key) {
        return writable.containsKey(key) ? writable.get(key) : super.get(key);
    }

    @Override
    public Set<String> keySet() {
        Set<String> union = new HashSet<String>(super.keySet());
        union.addAll(writable.keySet());
        return Collections.unmodifiableSet(union);
    }

    @Override
    public boolean isEmpty() {
        return writable.isEmpty() && super.isEmpty();
    }

    @Override
    public Object removeField(final String key) {
        if (writable.containsKey(key)) {
            return writable.remove(key);
        } else {
            throw new UnsupportedOperationException("Key [" + key + "] is not in writable part of the object");
        }
    }
}
