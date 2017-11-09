/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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

package org.bson;

import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.bson.io.BasicOutputBuffer;
import org.bson.json.JsonReader;
import org.bson.json.JsonWriter;
import org.bson.json.JsonWriterSettings;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;

/**
 * A type-safe container for a BSON document.  This class should NOT be sub-classed by third parties.
 *
 * @since 3.0
 */
public class BsonDocument extends BsonValue implements Map<String, BsonValue>, Cloneable, Bson, Serializable {
    private static final long serialVersionUID = 1L;

    private final Map<String, BsonValue> map = new LinkedHashMap<String, BsonValue>();

    /**
     * Parses a string in MongoDB Extended JSON format to a {@code BsonDocument}
     *
     * @param json the JSON string
     * @return a corresponding {@code BsonDocument} object
     * @see org.bson.json.JsonReader
     * @mongodb.driver.manual reference/mongodb-extended-json/ MongoDB Extended JSON
     */
    public static BsonDocument parse(final String json) {
        return new BsonDocumentCodec().decode(new JsonReader(json), DecoderContext.builder().build());
    }

    /**
     * Construct a new instance with the given list {@code BsonElement}, none of which may be null.
     *
     * @param bsonElements a list of {@code BsonElement}
     */
    public BsonDocument(final List<BsonElement> bsonElements) {
        for (BsonElement cur : bsonElements) {
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
    public <C> BsonDocument toBsonDocument(final Class<C> documentClass, final CodecRegistry codecRegistry) {
        return this;
    }

    @Override
    public BsonType getBsonType() {
        return BsonType.DOCUMENT;
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
     * @throws org.bson.BsonInvalidOperationException if the document does not contain the key or the value is not a BsonDocument
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
     * @throws org.bson.BsonInvalidOperationException if the document does not contain the key or the value is not of the expected type
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
     * @throws org.bson.BsonInvalidOperationException if the document does not contain the key or the value is not of the expected type
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
     * @throws org.bson.BsonInvalidOperationException if the document does not contain the key or the value is not of the expected type
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
     * @throws org.bson.BsonInvalidOperationException if the document does not contain the key or the value is not of the expected type
     */
    public BsonInt64 getInt64(final Object key) {
        throwIfKeyAbsent(key);
        return get(key).asInt64();
    }

    /**
     * Gets the value of the key if it is a BsonDecimal128, or throws if not.
     *
     * @param key the key
     * @return the value of the key as a BsonDecimal128
     * @throws org.bson.BsonInvalidOperationException if the document does not contain the key or the value is not of the expected type
     * @since 3.4
     */
    public BsonDecimal128 getDecimal128(final Object key) {
        throwIfKeyAbsent(key);
        return get(key).asDecimal128();
    }

    /**
     * Gets the value of the key if it is a BsonDouble, or throws if not.
     *
     * @param key the key
     * @return the value of the key as a BsonDouble
     * @throws org.bson.BsonInvalidOperationException if the document does not contain the key or the value is not of the expected type
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
     * @throws org.bson.BsonInvalidOperationException if the document does not contain the key or the value is not of the expected type
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
     * @throws org.bson.BsonInvalidOperationException if the document does not contain the key or the value is not of the expected type
     */
    public BsonString getString(final Object key) {
        throwIfKeyAbsent(key);
        return get(key).asString();
    }

    /**
     * Gets the value of the key if it is a BsonDateTime, or throws if not.
     *
     * @param key the key
     * @return the value of the key as a BsonDateTime
     * @throws org.bson.BsonInvalidOperationException if the document does not contain the key or the value is not of the expected type
     */
    public BsonDateTime getDateTime(final Object key) {
        throwIfKeyAbsent(key);
        return get(key).asDateTime();
    }

    /**
     * Gets the value of the key if it is a BsonTimestamp, or throws if not.
     *
     * @param key the key
     * @return the value of the key as a BsonTimestamp
     * @throws org.bson.BsonInvalidOperationException if the document does not contain the key or the value is not of the expected type
     */
    public BsonTimestamp getTimestamp(final Object key) {
        throwIfKeyAbsent(key);
        return get(key).asTimestamp();
    }

    /**
     * Gets the value of the key if it is a BsonObjectId, or throws if not.
     *
     * @param key the key
     * @return the value of the key as a BsonObjectId
     * @throws org.bson.BsonInvalidOperationException if the document does not contain the key or the value is not of the expected type
     */
    public BsonObjectId getObjectId(final Object key) {
        throwIfKeyAbsent(key);
        return get(key).asObjectId();
    }

    /**
     * Gets the value of the key if it is a BsonRegularExpression, or throws if not.
     *
     * @param key the key
     * @return the value of the key as a BsonRegularExpression
     * @throws org.bson.BsonInvalidOperationException if the document does not contain the key or the value is not of the expected type
     */
    public BsonRegularExpression getRegularExpression(final Object key) {
        throwIfKeyAbsent(key);
        return get(key).asRegularExpression();
    }

    /**
     * Gets the value of the key if it is a BsonBinary, or throws if not.
     *
     * @param key the key
     * @return the value of the key as a BsonBinary
     * @throws org.bson.BsonInvalidOperationException if the document does not contain the key or the value is not of the expected type
     */
    public BsonBinary getBinary(final Object key) {
        throwIfKeyAbsent(key);
        return get(key).asBinary();
    }

    /**
     * Returns true if the value of the key is a BsonNull, returns false if the document does not contain the key.
     *
     * @param key the key
     * @return true if the value of the key is a BsonNull, returns false if the document does not contain the key.
     */
    public boolean isNull(final Object key) {
        if (!containsKey(key)) {
            return false;
        }
        return get(key).isNull();
    }

    /**
     * Returns true if the value of the key is a BsonDocument, returns false if the document does not contain the key.
     *
     * @param key the key
     * @return true if the value of the key is a BsonDocument, returns false if the document does not contain the key.
     */
    public boolean isDocument(final Object key) {
        if (!containsKey(key)) {
            return false;
        }
        return get(key).isDocument();
    }

    /**
     * Returns true if the value of the key is a BsonArray, returns false if the document does not contain the key.
     *
     * @param key the key
     * @return true if the value of the key is a BsonArray, returns false if the document does not contain the key.
     */
    public boolean isArray(final Object key) {
        if (!containsKey(key)) {
            return false;
        }
        return get(key).isArray();
    }

    /**
     * Returns true if the value of the key is a BsonNumber, returns false if the document does not contain the key.
     *
     * @param key the key
     * @return true if the value of the key is a BsonNumber, returns false if the document does not contain the key.
     */
    public boolean isNumber(final Object key) {
        if (!containsKey(key)) {
            return false;
        }
        return get(key).isNumber();
    }

    /**
     * Returns true if the value of the key is a BsonInt32, returns false if the document does not contain the key.
     *
     * @param key the key
     * @return true if the value of the key is a BsonInt32, returns false if the document does not contain the key.
     */
    public boolean isInt32(final Object key) {
        if (!containsKey(key)) {
            return false;
        }
        return get(key).isInt32();
    }

    /**
     * Returns true if the value of the key is a BsonInt64, returns false if the document does not contain the key.
     *
     * @param key the key
     * @return true if the value of the key is a BsonInt64, returns false if the document does not contain the key.
     */
    public boolean isInt64(final Object key) {
        if (!containsKey(key)) {
            return false;
        }
        return get(key).isInt64();
    }

    /**
     * Returns true if the value of the key is a BsonDecimal128, returns false if the document does not contain the key.
     *
     * @param key the key
     * @return true if the value of the key is a BsonDecimal128, returns false if the document does not contain the key.
     * @since 3.4
     */
    public boolean isDecimal128(final Object key) {
        if (!containsKey(key)) {
            return false;
        }
        return get(key).isDecimal128();
    }


    /**
     * Returns true if the value of the key is a BsonDouble, returns false if the document does not contain the key.
     *
     * @param key the key
     * @return true if the value of the key is a BsonDouble, returns false if the document does not contain the key.
     */
    public boolean isDouble(final Object key) {
        if (!containsKey(key)) {
            return false;
        }
        return get(key).isDouble();
    }

    /**
     * Returns true if the value of the key is a BsonBoolean, returns false if the document does not contain the key.
     *
     * @param key the key
     * @return true if the value of the key is a BsonBoolean, returns false if the document does not contain the key.
     */
    public boolean isBoolean(final Object key) {
        if (!containsKey(key)) {
            return false;
        }
        return get(key).isBoolean();
    }

    /**
     * Returns true if the value of the key is a BsonString, returns false if the document does not contain the key.
     *
     * @param key the key
     * @return true if the value of the key is a BsonString, returns false if the document does not contain the key.
     */
    public boolean isString(final Object key) {
        if (!containsKey(key)) {
            return false;
        }
        return get(key).isString();
    }

    /**
     * Returns true if the value of the key is a BsonDateTime, returns false if the document does not contain the key.
     *
     * @param key the key
     * @return true if the value of the key is a BsonDateTime, returns false if the document does not contain the key.
     */
    public boolean isDateTime(final Object key) {
        if (!containsKey(key)) {
            return false;
        }
        return get(key).isDateTime();
    }

    /**
     * Returns true if the value of the key is a BsonTimestamp, returns false if the document does not contain the key.
     *
     * @param key the key
     * @return true if the value of the key is a BsonTimestamp, returns false if the document does not contain the key.
     */
    public boolean isTimestamp(final Object key) {
        if (!containsKey(key)) {
            return false;
        }
        return get(key).isTimestamp();
    }

    /**
     * Returns true if the value of the key is a BsonObjectId, returns false if the document does not contain the key.
     *
     * @param key the key
     * @return true if the value of the key is a BsonObjectId, returns false if the document does not contain the key.
     */
    public boolean isObjectId(final Object key) {
        if (!containsKey(key)) {
            return false;
        }
        return get(key).isObjectId();
    }

    /**
     * Returns true if the value of the key is a BsonBinary, returns false if the document does not contain the key.
     *
     * @param key the key
     * @return true if the value of the key is a BsonBinary, returns false if the document does not contain the key.
     */
    public boolean isBinary(final Object key) {
        if (!containsKey(key)) {
            return false;
        }
        return get(key).isBinary();
    }

    /**
     * If the document does not contain the given key, return the given default value.  Otherwise, gets the value of the key.
     *
     * @param key the key
     * @param defaultValue the default value
     * @return the value of the key as a BsonValue
     */
    public BsonValue get(final Object key, final BsonValue defaultValue) {
        BsonValue value = get(key);
        return value != null ? value : defaultValue;
    }

    /**
     * If the document does not contain the given key, return the given default value.  Otherwise, gets the value of the key as a
     * BsonDocument.
     *
     * @param key the key
     * @param defaultValue the default value
     * @return the value of the key as a BsonDocument
     * @throws org.bson.BsonInvalidOperationException if the document contains the key but the value is not of the expected type
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
     * @param defaultValue the default value
     * @return the value of the key as a BsonArray
     * @throws org.bson.BsonInvalidOperationException if the document contains the key but the value is not of the expected type
     */
    public BsonArray getArray(final Object key, final BsonArray defaultValue) {
        if (!containsKey(key)) {
            return defaultValue;
        }
        return get(key).asArray();
    }

    /**
     * If the document does not contain the given key, return the given default value.  Otherwise, gets the value of the key as a
     * BsonNumber.
     *
     * @param key the key
     * @param defaultValue the default value
     * @return the value of the key as a BsonNumber
     * @throws org.bson.BsonInvalidOperationException if the document contains the key but the value is not of the expected type
     */
    public BsonNumber getNumber(final Object key, final BsonNumber defaultValue) {
        if (!containsKey(key)) {
            return defaultValue;
        }
        return get(key).asNumber();
    }

    /**
     * If the document does not contain the given key, return the given default value.  Otherwise, gets the value of the key as a
     * BsonInt32.
     *
     * @param key the key
     * @param defaultValue the default value
     * @return the value of the key as a BsonInt32
     * @throws org.bson.BsonInvalidOperationException if the document contains the key but the value is not of the expected type
     */
    public BsonInt32 getInt32(final Object key, final BsonInt32 defaultValue) {
        if (!containsKey(key)) {
            return defaultValue;
        }
        return get(key).asInt32();
    }

    /**
     * If the document does not contain the given key, return the given default value.  Otherwise, gets the value of the key as a
     * BsonInt64.
     *
     * @param key the key
     * @param defaultValue the default value
     * @return the value of the key as a BsonInt64
     * @throws org.bson.BsonInvalidOperationException if the document contains the key but the value is not of the expected type
     */
    public BsonInt64 getInt64(final Object key, final BsonInt64 defaultValue) {
        if (!containsKey(key)) {
            return defaultValue;
        }
        return get(key).asInt64();
    }

    /**
     * If the document does not contain the given key, return the given default value.  Otherwise, gets the value of the key as a
     * BsonDecimal128.
     *
     * @param key the key
     * @param defaultValue the default value
     * @return the value of the key as a BsonDecimal128
     * @throws org.bson.BsonInvalidOperationException if the document contains the key but the value is not of the expected type
     * @since 3.4
     */
    public BsonDecimal128 getDecimal128(final Object key, final BsonDecimal128 defaultValue) {
        if (!containsKey(key)) {
            return defaultValue;
        }
        return get(key).asDecimal128();
    }

    /**
     * If the document does not contain the given key, return the given default value.  Otherwise, gets the value of the key as a
     * BsonDouble.
     *
     * @param key the key
     * @param defaultValue the default value
     * @return the value of the key as a BsonDouble
     * @throws org.bson.BsonInvalidOperationException if the document contains the key but the value is not of the expected type
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
     * @param defaultValue the default value
     * @return the value of the key as a BsonBoolean
     * @throws org.bson.BsonInvalidOperationException if the document contains the key but the value is not of the expected type
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
     * @param defaultValue the default value
     * @return the value of the key as a BsonString
     * @throws org.bson.BsonInvalidOperationException if the document contains the key but the value is not of the expected type
     */
    public BsonString getString(final Object key, final BsonString defaultValue) {
        if (!containsKey(key)) {
            return defaultValue;
        }
        return get(key).asString();
    }

    /**
     * If the document does not contain the given key, return the given default value.  Otherwise, gets the value of the key as a
     * BsonDateTime.
     *
     * @param key the key
     * @param defaultValue the default value
     * @return the value of the key as a BsonDateTime
     * @throws org.bson.BsonInvalidOperationException if the document contains the key but the value is not of the expected type
     */
    public BsonDateTime getDateTime(final Object key, final BsonDateTime defaultValue) {
        if (!containsKey(key)) {
            return defaultValue;
        }
        return get(key).asDateTime();
    }

    /**
     * If the document does not contain the given key, return the given default value.  Otherwise, gets the value of the key as a
     * BsonTimestamp.
     *
     * @param key the key
     * @param defaultValue the default value
     * @return the value of the key as a BsonTimestamp
     * @throws org.bson.BsonInvalidOperationException if the document contains the key but the value is not of the expected type
     */
    public BsonTimestamp getTimestamp(final Object key, final BsonTimestamp defaultValue) {
        if (!containsKey(key)) {
            return defaultValue;
        }
        return get(key).asTimestamp();
    }

    /**
     * If the document does not contain the given key, return the given default value.  Otherwise, gets the value of the key as a
     * BsonObjectId.
     *
     * @param key the key
     * @param defaultValue the default value
     * @return the value of the key as a BsonObjectId
     * @throws org.bson.BsonInvalidOperationException if the document contains the key but the value is not of the expected type
     */
    public BsonObjectId getObjectId(final Object key, final BsonObjectId defaultValue) {
        if (!containsKey(key)) {
            return defaultValue;
        }
        return get(key).asObjectId();
    }

    /**
     * If the document does not contain the given key, return the given default value.  Otherwise, gets the value of the key as a
     * BsonBinary.
     *
     * @param key the key
     * @param defaultValue the default value
     * @return the value of the key as a BsonBinary
     * @throws org.bson.BsonInvalidOperationException if the document contains the key but the value is not of the expected type
     */
    public BsonBinary getBinary(final Object key, final BsonBinary defaultValue) {
        if (!containsKey(key)) {
            return defaultValue;
        }
        return get(key).asBinary();
    }

    /**
     * If the document does not contain the given key, return the given default value.  Otherwise, gets the value of the key as a
     * BsonRegularExpression.
     *
     * @param key the key
     * @param defaultValue the default value
     * @return the value of the key as a BsonRegularExpression
     * @throws org.bson.BsonInvalidOperationException if the document contains the key but the value is not of the expected type
     */
    public BsonRegularExpression getRegularExpression(final Object key, final BsonRegularExpression defaultValue) {
        if (!containsKey(key)) {
            return defaultValue;
        }
        return get(key).asRegularExpression();
    }

    @Override
    public BsonValue put(final String key, final BsonValue value) {
        if (value == null) {
            throw new IllegalArgumentException(format("The value for key %s can not be null", key));
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

    /**
     * Gets the first key in the document.
     *
     * @return the first key in the document
     * @throws java.util.NoSuchElementException if the document is empty
     * @since 3.6
     */
    public String getFirstKey() {
        return keySet().iterator().next();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BsonDocument)) {
            return false;
        }

        BsonDocument that = (BsonDocument) o;

        return entrySet().equals(that.entrySet());
    }

    @Override
    public int hashCode() {
        return entrySet().hashCode();
    }

    /**
     * Gets a JSON representation of this document using the {@link org.bson.json.JsonMode#STRICT} output mode, and otherwise the default
     * settings of {@link JsonWriterSettings.Builder}.
     *
     * @return a JSON representation of this document
     * @see #toJson(JsonWriterSettings)
     * @see JsonWriterSettings
     */
    @SuppressWarnings("deprecation")
    public String toJson() {
        return toJson(new JsonWriterSettings());
    }

    /**
     * Gets a JSON representation of this document using the given {@code JsonWriterSettings}.
     * @param settings the JSON writer settings
     * @return a JSON representation of this document
     */
    public String toJson(final JsonWriterSettings settings) {
        StringWriter writer = new StringWriter();
        new BsonDocumentCodec().encode(new JsonWriter(writer, settings), this, EncoderContext.builder().build());
        return writer.toString();
    }

    @Override
    public String toString() {
        return toJson();
    }

    @Override
    public BsonDocument clone() {
        BsonDocument to = new BsonDocument();
        for (Entry<String, BsonValue> cur : entrySet()) {
            switch (cur.getValue().getBsonType()) {
                case DOCUMENT:
                    to.put(cur.getKey(), cur.getValue().asDocument().clone());
                    break;
                case ARRAY:
                    to.put(cur.getKey(), cur.getValue().asArray().clone());
                    break;
                case BINARY:
                    to.put(cur.getKey(), BsonBinary.clone(cur.getValue().asBinary()));
                    break;
                case JAVASCRIPT_WITH_SCOPE:
                    to.put(cur.getKey(), BsonJavaScriptWithScope.clone(cur.getValue().asJavaScriptWithScope()));
                    break;
                default:
                    to.put(cur.getKey(), cur.getValue());
            }
        }
        return to;
    }

    private void throwIfKeyAbsent(final Object key) {
        if (!containsKey(key)) {
            throw new BsonInvalidOperationException("Document does not contain key " + key);
        }
    }

    // see https://docs.oracle.com/javase/6/docs/platform/serialization/spec/output.html
    private Object writeReplace() {
        return new SerializationProxy(this);
    }

    // see https://docs.oracle.com/javase/6/docs/platform/serialization/spec/input.html
    private void readObject(final ObjectInputStream stream) throws InvalidObjectException {
        throw new InvalidObjectException("Proxy required");
    }

    private static class SerializationProxy implements Serializable {
        private static final long serialVersionUID = 1L;

        private final byte[] bytes;

        SerializationProxy(final BsonDocument document) {
            BasicOutputBuffer buffer = new BasicOutputBuffer();
            new BsonDocumentCodec().encode(new BsonBinaryWriter(buffer), document, EncoderContext.builder().build());
            this.bytes = new byte[buffer.size()];
            int curPos = 0;
            for (ByteBuf cur : buffer.getByteBuffers()) {
                System.arraycopy(cur.array(), cur.position(), bytes, curPos, cur.limit());
                curPos += cur.position();
            }
        }

        private Object readResolve() {
            return new BsonDocumentCodec().decode(new BsonBinaryReader(ByteBuffer.wrap(bytes)
                                                                                 .order(ByteOrder.LITTLE_ENDIAN)),
                                                  DecoderContext.builder().build());
        }
    }
}
