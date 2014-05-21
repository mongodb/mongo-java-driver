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

package org.bson.types;

import org.bson.BSONException;
import org.bson.BSONInvalidOperationException;
import org.bson.BSONType;

import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;

/**
 * A type-safe container for a BSON document.
 *
 * @since 3.0
 */
public class BsonDocument extends BsonValue implements Map<String, BsonValue>, Serializable {
    private static final long serialVersionUID = -8366220692735186027L;

    private final Map<String, BsonValue> map = new LinkedHashMap<String, BsonValue>();

    /**
     * Construct a new instance with the given key value pairs, none of which may be null.
     */
    public BsonDocument(final List<BsonElement> keyValuePairs) {
        for (BsonElement cur : keyValuePairs) {
            put(cur.getName(), cur.getValue());
        }
    }

    /**
     * Construct a new instance with a single key value pair
     *
     * @param key   the key
     * @param value the value
     */
    public BsonDocument(final String key, final BsonValue value) {
        put(key, value);
    }

    /**
     * Construct an empty document.
     */
    public BsonDocument() {
    }

    @Override
    public BSONType getBsonType() {
        return BSONType.DOCUMENT;
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public boolean containsKey(final Object key) {
        return map.containsKey(key);
    }

    @Override
    public boolean containsValue(final Object value) {
        return map.containsValue(value);
    }

    @Override
    public BsonValue get(final Object key) {
        return map.get(key);
    }

    /**
     * Gets the value of the key if it is a BsonDocument, or throws if not.
     *
     * @param key the key
     * @return the value of the key as a BsonDocument
     * @throws org.bson.BSONInvalidOperationException if the document does not contain the key or the value is not a BsonDocument
     */
    public BsonDocument getDocument(final Object key) {
        throwIfKeyAbsent(key);
        return get(key).asDocument();
    }

    /**
     * Gets the value of the key if it is a BsonArray, or throws if not.
     *
     * @param key the key
     * @return the value of the key as a BsonArray
     * @throws org.bson.BSONInvalidOperationException if the document does not contain the key or the value is not of the expected type
     */
    public BsonArray getArray(final Object key) {
        throwIfKeyAbsent(key);

        return get(key).asArray();
    }

    /**
     * Gets the value of the key if it is a BsonNumber, or throws if not.
     *
     * @param key the key
     * @return the value of the key as a BsonNumber
     * @throws org.bson.BSONInvalidOperationException if the document does not contain the key or the value is not of the expected type
     */
    public BsonNumber getNumber(final Object key) {
        throwIfKeyAbsent(key);
        return get(key).asNumber();
    }

    /**
     * Gets the value of the key if it is a BsonInt32, or throws if not.
     *
     * @param key the key
     * @return the value of the key as a BsonInt32
     * @throws org.bson.BSONInvalidOperationException if the document does not contain the key or the value is not of the expected type
     */
    public BsonInt32 getInt32(final Object key) {
        throwIfKeyAbsent(key);
        return get(key).asInt32();
    }

    /**
     * Gets the value of the key if it is a BsonInt64, or throws if not.
     *
     * @param key the key
     * @return the value of the key as a BsonInt64
     * @throws org.bson.BSONInvalidOperationException if the document does not contain the key or the value is not of the expected type
     */
    public BsonInt64 getInt64(final Object key) {
        throwIfKeyAbsent(key);
        return get(key).asInt64();
    }

    /**
     * Gets the value of the key if it is a BsonDouble, or throws if not.
     *
     * @param key the key
     * @return the value of the key as a BsonDouble
     * @throws org.bson.BSONInvalidOperationException if the document does not contain the key or the value is not of the expected type
     */
    public BsonDouble getDouble(final Object key) {
        throwIfKeyAbsent(key);
        return get(key).asDouble();
    }

    /**
     * Gets the value of the key if it is a BsonBoolean, or throws if not.
     *
     * @param key the key
     * @return the value of the key as a BsonBoolean
     * @throws org.bson.BSONInvalidOperationException if the document does not contain the key or the value is not of the expected type
     */
    public BsonBoolean getBoolean(final Object key) {
        throwIfKeyAbsent(key);
        return get(key).asBoolean();
    }

    /**
     * Gets the value of the key if it is a BsonString, or throws if not.
     *
     * @param key the key
     * @return the value of the key as a BsonString
     * @throws org.bson.BSONInvalidOperationException if the document does not contain the key or the value is not of the expected type
     */
    public BsonString getString(final Object key) {
        throwIfKeyAbsent(key);
        return get(key).asString();
    }

    /**
     * If the document does not contain the given key, return the given default value.  Otherwise, gets the value of the key as a
     * BsonDocument.
     *
     * @param key the key
     * @return the value of the key as a BsonDocument
     * @throws org.bson.BSONInvalidOperationException if the document contains the key but the value is not of the expected type
     */
    public BsonDocument getDocument(final Object key, final BsonDocument defaultValue) {
        if (!containsKey(key)) {
            return defaultValue;
        }
        return get(key).asDocument();
    }

    /**
     * If the document does not contain the given key, return the given default value.  Otherwise, gets the value of the key as a
     * BsonArray.
     *
     * @param key the key
     * @return the value of the key as a BsonArray
     * @throws org.bson.BSONInvalidOperationException if the document contains the key but the value is not of the expected type
     */
    public BsonArray getArray(final Object key, final BsonArray defaultValue) {
        if (!containsKey(key)) {
            return defaultValue;
        }
        return get(key).asArray();
    }

    /**
     * If the document does not contain the given key, return the given default value.  Otherwise, gets the value of the key as a
     * BsonInt32.
     *
     * @param key the key
     * @return the value of the key as a BsonInt32
     * @throws org.bson.BSONInvalidOperationException if the document contains the key but the value is not of the expected type
     */
    public BsonInt32 getInt32(final Object key, final BsonInt32 defaultValue) {
        if (!containsKey(key)) {
            return defaultValue;
        }
        return get(key).asInt32();
    }

    /**
     * Gets the value of the key if it is a BsonInt64, or throws if not.
     *
     * @param key the key
     * @return the value of the key as a BsonInt64
     * @throws org.bson.BSONInvalidOperationException if the document contains the key but the value is not of the expected type
     */
    public BsonInt64 getInt64(final Object key, final BsonInt64 defaultValue) {
        if (!containsKey(key)) {
            return defaultValue;
        }
        return get(key).asInt64();
    }

    /**
     * If the document does not contain the given key, return the given default value.  Otherwise, gets the value of the key as a
     * BsonDouble.
     *
     * @param key the key
     * @return the value of the key as a BsonDouble
     * @throws org.bson.BSONInvalidOperationException if the document contains the key but the value is not of the expected type
     */
    public BsonDouble getDouble(final Object key, final BsonDouble defaultValue) {
        if (!containsKey(key)) {
            return defaultValue;
        }
        return get(key).asDouble();
    }

    /**
     * If the document does not contain the given key, return the given default value.  Otherwise, gets the value of the key as a
     * BsonBoolean.
     *
     * @param key the key
     * @return the value of the key as a BsonBoolean
     * @throws org.bson.BSONInvalidOperationException if the document contains the key but the value is not of the expected type
     */
    public BsonBoolean getBoolean(final Object key, final BsonBoolean defaultValue) {
        if (!containsKey(key)) {
            return defaultValue;
        }
        return get(key).asBoolean();
    }

    /**
     * If the document does not contain the given key, return the given default value.  Otherwise, gets the value of the key as a
     * BsonString.
     *
     * @param key the key
     * @return the value of the key as a BsonString
     * @throws org.bson.BSONInvalidOperationException if the document contains the key but the value is not of the expected type
     */
    public BsonString getString(final Object key, final BsonString defaultValue) {
        if (!containsKey(key)) {
            return defaultValue;
        }
        return get(key).asString();
    }

    private void throwIfKeyAbsent(final Object key) {
        if (!containsKey(key)) {
            throw new BSONInvalidOperationException("Document does not contain key " + key);
        }
    }

    @Override
    public BsonValue put(final String key, final BsonValue value) {
        if (value == null) {
            throw new IllegalArgumentException(String.format("The value for key %s can not be null", key));
        }
        if (key.contains("\0")) {
            throw new BSONException(format("BSON cstring '%s' is not valid because it contains a null character at index %d", key,
                                           key.indexOf('\0')));
        }
        return map.put(key, value);
    }

    @Override
    public BsonValue remove(final Object key) {
        return map.remove(key);
    }

    @Override
    public void putAll(final Map<? extends String, ? extends BsonValue> m) {
        for (Map.Entry<? extends String, ? extends BsonValue> cur : m.entrySet()) {
            put(cur.getKey(), cur.getValue());
        }
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public Set<String> keySet() {
        return map.keySet();
    }

    @Override
    public Collection<BsonValue> values() {
        return map.values();
    }

    @Override
    public Set<Entry<String, BsonValue>> entrySet() {
        return map.entrySet();
    }

    /**
     * Put the given key and value into this document, and return the document.
     *
     * @param key   the key
     * @param value the value
     * @return this
     */
    public BsonDocument append(final String key, final BsonValue value) {
        put(key, value);
        return this;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BsonDocument that = (BsonDocument) o;

        if (!map.equals(that.map)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return map.hashCode();
    }

    @Override
    public String toString() {
        return "BsonDocument{"
               + "map=" + map
               + '}';
    }
}
