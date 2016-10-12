/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client;

import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public final class ImmutableDocument implements Map<String, Object>, Serializable, Bson {
    private final Map<String, Object> immutableDocument;

    /**
     * Creates a Document instance initialized with the given map.
     *
     * @param map initial map
     */
    public ImmutableDocument(final Map<String, Object> map) {
        immutableDocument = Collections.unmodifiableMap(new LinkedHashMap<String, Object>(map));
    }


    @Override
    public int size() {
        return immutableDocument.size();
    }

    @Override
    public boolean isEmpty() {
        return immutableDocument.isEmpty();
    }

    @Override
    public boolean containsKey(final Object key) {
        return immutableDocument.containsKey(key);
    }

    @Override
    public boolean containsValue(final Object value) {
        return immutableDocument.containsValue(value);
    }

    @Override
    public Object get(final Object key) {
        return immutableDocument.get(key);
    }

    @Override
    public Object put(final String key, final Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object remove(final Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(final Map<? extends String, ?> m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> keySet() {
        return immutableDocument.keySet();
    }

    @Override
    public Collection<Object> values() {
        return immutableDocument.values();
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
        return immutableDocument.entrySet();
    }

    @Override
    public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> tDocumentClass, final CodecRegistry codecRegistry) {
        return new BsonDocumentWrapper<ImmutableDocument>(this, codecRegistry.get(ImmutableDocument.class));
    }
}
