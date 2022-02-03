/*
 * Copyright 2008-present MongoDB, Inc.
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

import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.Codec;
import org.bson.codecs.Decoder;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.DocumentCodecProvider;
import org.bson.codecs.Encoder;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.IterableCodecProvider;
import org.bson.codecs.MapCodecProvider;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.bson.json.JsonMode;
import org.bson.json.JsonReader;
import org.bson.json.JsonWriter;
import org.bson.json.JsonWriterSettings;
import org.bson.types.ObjectId;

import java.io.Serializable;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.bson.assertions.Assertions.isTrue;
import static org.bson.assertions.Assertions.notNull;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.withUuidRepresentation;

/**
 * A representation of a document as a {@code Map}.  All iterators will traverse the elements in insertion order, as with {@code
 * LinkedHashMap}.
 *
 * @mongodb.driver.manual core/document document
 * @since 3.0.0
 */
public class Document implements Map<String, Object>, Serializable, Bson {
    private static final Codec<Document> DEFAULT_CODEC =
            withUuidRepresentation(fromProviders(asList(new ValueCodecProvider(), new IterableCodecProvider(),
                    new BsonValueCodecProvider(), new DocumentCodecProvider(), new MapCodecProvider())), UuidRepresentation.STANDARD)
                    .get(Document.class);

    private static final long serialVersionUID = 6297731997167536582L;

    private final LinkedHashMap<String, Object> documentAsMap;

    /**
     * Creates an empty Document instance.
     */
    public Document() {
        documentAsMap = new LinkedHashMap<String, Object>();
    }

    /**
     * Create a Document instance initialized with the given key/value pair.
     *
     * @param key   key
     * @param value value
     */
    public Document(final String key, final Object value) {
        documentAsMap = new LinkedHashMap<String, Object>();
        documentAsMap.put(key, value);
    }

    /**
     * Creates a Document instance initialized with the given map.
     *
     * @param map initial map
     */
    public Document(final Map<String, Object> map) {
        documentAsMap = new LinkedHashMap<String, Object>(map);
    }


    /**
     * Parses a string in MongoDB Extended JSON format to a {@code Document}
     *
     * @param json the JSON string
     * @return a corresponding {@code Document} object
     * @see org.bson.json.JsonReader
     * @mongodb.driver.manual reference/mongodb-extended-json/ MongoDB Extended JSON
     */
    public static Document parse(final String json) {
        return parse(json, DEFAULT_CODEC);
    }

    /**
     * Parses a string in MongoDB Extended JSON format to a {@code Document}
     *
     * @param json the JSON string
     * @param decoder the {@code Decoder} to use to parse the JSON string into a {@code Document}
     * @return a corresponding {@code Document} object
     * @see org.bson.json.JsonReader
     * @mongodb.driver.manual reference/mongodb-extended-json/ MongoDB Extended JSON
     */
    public static Document parse(final String json, final Decoder<Document> decoder) {
        notNull("codec", decoder);
        JsonReader bsonReader = new JsonReader(json);
        return decoder.decode(bsonReader, DecoderContext.builder().build());
    }

    @Override
    public <C> BsonDocument toBsonDocument(final Class<C> documentClass, final CodecRegistry codecRegistry) {
        return new BsonDocumentWrapper<Document>(this, codecRegistry.get(Document.class));
    }

    /**
     * Put the given key/value pair into this Document and return this.  Useful for chaining puts in a single expression, e.g.
     * <pre>
     * doc.append("a", 1).append("b", 2)}
     * </pre>
     * @param key   key
     * @param value value
     * @return this
     */
    public Document append(final String key, final Object value) {
        documentAsMap.put(key, value);
        return this;
    }

    /**
     * Gets the value of the given key, casting it to the given {@code Class<T>}.  This is useful to avoid having casts in client code,
     * though the effect is the same.  So to get the value of a key that is of type String, you would write {@code String name =
     * doc.get("name", String.class)} instead of {@code String name = (String) doc.get("x") }.
     *
     * @param key   the key
     * @param clazz the non-null class to cast the value to
     * @param <T>   the type of the class
     * @return the value of the given key, or null if the instance does not contain this key.
     * @throws ClassCastException if the value of the given key is not of type T
     */
    public <T> T get(final Object key, final Class<T> clazz) {
        notNull("clazz", clazz);
        return clazz.cast(documentAsMap.get(key));
    }

    /**
     * Gets the value of the given key, casting it to {@code Class<T>} or returning the default value if null.
     * This is useful to avoid having casts in client code, though the effect is the same.
     *
     * @param key   the key
     * @param defaultValue what to return if the value is null
     * @param <T>   the type of the class
     * @return the value of the given key, or null if the instance does not contain this key.
     * @throws ClassCastException if the value of the given key is not of type T
     * @since 3.5
     */
    @SuppressWarnings("unchecked")
    public <T> T get(final Object key, final T defaultValue) {
        notNull("defaultValue", defaultValue);
        Object value = documentAsMap.get(key);
        return value == null ? defaultValue : (T) value;
    }

    /**
     * Gets the value in an embedded document, casting it to the given {@code Class<T>}.  The list of keys represents a path to the
     * embedded value, drilling down into an embedded document for each key. This is useful to avoid having casts in
     * client code, though the effect is the same.
     *
     * The generic type of the keys list is {@code ?} to be consistent with the corresponding {@code get} methods, but in practice
     * the actual type of the argument should be {@code List<String>}. So to get the embedded value of a key list that is of type String,
     * you would write {@code String name = doc.getEmbedded(List.of("employee", "manager", "name"), String.class)} instead of
     * {@code String name = (String) doc.get("employee", Document.class).get("manager", Document.class).get("name") }.
     *
     * @param keys  the list of keys
     * @param clazz the non-null class to cast the value to
     * @param <T>   the type of the class
     * @return the value of the given embedded key, or null if the instance does not contain this embedded key.
     * @throws ClassCastException if the value of the given embedded key is not of type T
     * @since 3.10
     */
    public <T> T getEmbedded(final List<?> keys, final Class<T> clazz) {
        notNull("keys", keys);
        isTrue("keys", !keys.isEmpty());
        notNull("clazz", clazz);
        return getEmbeddedValue(keys, clazz, null);
    }

    /**
     * Gets the value in an embedded document, casting it to the given {@code Class<T>} or returning the default value if null.
     * The list of keys represents a path to the embedded value, drilling down into an embedded document for each key.
     * This is useful to avoid having casts in client code, though the effect is the same.
     *
     * The generic type of the keys list is {@code ?} to be consistent with the corresponding {@code get} methods, but in practice
     * the actual type of the argument should be {@code List<String>}. So to get the embedded value of a key list that is of type String,
     * you would write {@code String name = doc.getEmbedded(List.of("employee", "manager", "name"), "John Smith")} instead of
     * {@code String name = doc.get("employee", Document.class).get("manager", Document.class).get("name", "John Smith") }.
     *
     * @param keys  the list of keys
     * @param defaultValue what to return if the value is null
     * @param <T>   the type of the class
     * @return the value of the given key, or null if the instance does not contain this key.
     * @throws ClassCastException if the value of the given key is not of type T
     * @since 3.10
     */
    public <T> T getEmbedded(final List<?> keys, final T defaultValue) {
        notNull("keys", keys);
        isTrue("keys", !keys.isEmpty());
        notNull("defaultValue", defaultValue);
        return getEmbeddedValue(keys, null, defaultValue);
    }


    // Gets the embedded value of the given list of keys, casting it to {@code Class<T>} or returning the default value if null.
    // Throws ClassCastException if any of the intermediate embedded values is not a Document.
    @SuppressWarnings("unchecked")
    private <T> T getEmbeddedValue(final List<?> keys, final Class<T> clazz, final T defaultValue) {
        Object value = this;
        Iterator<?> keyIterator = keys.iterator();
        while (keyIterator.hasNext()) {
            Object key = keyIterator.next();
            value = ((Document) value).get(key);
            if (!(value instanceof Document)) {
                if (value == null) {
                    return defaultValue;
                } else if (keyIterator.hasNext()) {
                    throw new ClassCastException(format("At key %s, the value is not a Document (%s)",
                            key, value.getClass().getName()));
                }
            }
        }
        return clazz != null ? clazz.cast(value) : (T) value;
    }

    /**
     * Gets the value of the given key as an Integer.
     *
     * @param key the key
     * @return the value as an integer, which may be null
     * @throws java.lang.ClassCastException if the value is not an integer
     */
    public Integer getInteger(final Object key) {
        return (Integer) get(key);
    }

    /**
     * Gets the value of the given key as a primitive int.
     *
     * @param key          the key
     * @param defaultValue what to return if the value is null
     * @return the value as an integer, which may be null
     * @throws java.lang.ClassCastException if the value is not an integer
     */
    public int getInteger(final Object key, final int defaultValue) {
        return get(key, defaultValue);
    }

    /**
     * Gets the value of the given key as a Long.
     *
     * @param key the key
     * @return the value as a long, which may be null
     * @throws java.lang.ClassCastException if the value is not an long
     */
    public Long getLong(final Object key) {
        return (Long) get(key);
    }

    /**
     * Gets the value of the given key as a Double.
     *
     * @param key the key
     * @return the value as a double, which may be null
     * @throws java.lang.ClassCastException if the value is not an double
     */
    public Double getDouble(final Object key) {
        return (Double) get(key);
    }

    /**
     * Gets the value of the given key as a String.
     *
     * @param key the key
     * @return the value as a String, which may be null
     * @throws java.lang.ClassCastException if the value is not a String
     */
    public String getString(final Object key) {
        return (String) get(key);
    }

    /**
     * Gets the value of the given key as a Boolean.
     *
     * @param key the key
     * @return the value as a Boolean, which may be null
     * @throws java.lang.ClassCastException if the value is not an boolean
     */
    public Boolean getBoolean(final Object key) {
        return (Boolean) get(key);
    }

    /**
     * Gets the value of the given key as a primitive boolean.
     *
     * @param key          the key
     * @param defaultValue what to return if the value is null
     * @return the value as a primitive boolean
     * @throws java.lang.ClassCastException if the value is not a boolean
     */
    public boolean getBoolean(final Object key, final boolean defaultValue) {
        return get(key, defaultValue);
    }

    /**
     * Gets the value of the given key as an ObjectId.
     *
     * @param key the key
     * @return the value as an ObjectId, which may be null
     * @throws java.lang.ClassCastException if the value is not an ObjectId
     */
    public ObjectId getObjectId(final Object key) {
        return (ObjectId) get(key);
    }

    /**
     * Gets the value of the given key as a Date.
     *
     * @param key the key
     * @return the value as a Date, which may be null
     * @throws java.lang.ClassCastException if the value is not a Date
     */
    public Date getDate(final Object key) {
        return (Date) get(key);
    }

    /**
     * Gets the list value of the given key, casting the list elements to the given {@code Class<T>}.  This is useful to avoid having
     * casts in client code, though the effect is the same.
     *
     * @param key   the key
     * @param clazz the non-null class to cast the list value to
     * @param <T>   the type of the class
     * @return the list value of the given key, or null if the instance does not contain this key.
     * @throws ClassCastException if the elements in the list value of the given key is not of type T or the value is not a list
     * @since 3.10
     */
    public <T> List<T> getList(final Object key, final Class<T> clazz) {
        notNull("clazz", clazz);
        return constructValuesList(key, clazz, null);
    }

    /**
     * Gets the list value of the given key, casting the list elements to {@code Class<T>} or returning the default list value if null.
     * This is useful to avoid having casts in client code, though the effect is the same.
     *
     * @param key   the key
     * @param clazz the non-null class to cast the list value to
     * @param defaultValue what to return if the value is null
     * @param <T>   the type of the class
     * @return the list value of the given key, or the default list value if the instance does not contain this key.
     * @throws ClassCastException if the value of the given key is not of type T
     * @since 3.10
     */
    public <T> List<T> getList(final Object key, final Class<T> clazz, final List<T> defaultValue) {
        notNull("defaultValue", defaultValue);
        notNull("clazz", clazz);
        return constructValuesList(key, clazz, defaultValue);
    }


    // Construct the list of values for the specified key, or return the default value if the value is null.
    // A ClassCastException will be thrown if an element in the list is not of type T.
    @SuppressWarnings("unchecked")
    private <T> List<T> constructValuesList(final Object key, final Class<T> clazz, final List<T> defaultValue) {
        List<?> value = get(key, List.class);
        if (value == null) {
            return defaultValue;
        }

        for (Object item : value) {
            if (!clazz.isAssignableFrom(item.getClass())) {
                throw new ClassCastException(format("List element cannot be cast to %s", clazz.getName()));
            }
        }
        return (List<T>) value;
    }

    /**
     * Gets a JSON representation of this document using the {@link org.bson.json.JsonMode#RELAXED} output mode, and otherwise the default
     * settings of {@link JsonWriterSettings.Builder} and {@link DocumentCodec}.
     *
     * @return a JSON representation of this document
     * @throws org.bson.codecs.configuration.CodecConfigurationException if the document contains types not in the default registry
     * @see #toJson(JsonWriterSettings)
     * @see JsonWriterSettings
     */
    @SuppressWarnings("deprecation")
    public String toJson() {
        return toJson(JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build());
    }

    /**
     * Gets a JSON representation of this document
     *
     * <p>With the default {@link DocumentCodec}.</p>
     *
     * @param writerSettings the json writer settings to use when encoding
     * @return a JSON representation of this document
     * @throws org.bson.codecs.configuration.CodecConfigurationException if the document contains types not in the default registry
     */
    public String toJson(final JsonWriterSettings writerSettings) {
        return toJson(writerSettings, DEFAULT_CODEC);
    }

    /**
     * Gets a JSON representation of this document
     *
     * <p>With the default {@link JsonWriterSettings}.</p>
     *
     * @param encoder the document codec instance to use to encode the document
     * @return a JSON representation of this document
     * @throws org.bson.codecs.configuration.CodecConfigurationException if the registry does not contain a codec for the document values.
     */
    @SuppressWarnings("deprecation")
    public String toJson(final Encoder<Document> encoder) {
        return toJson(JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build(), encoder);
    }

    /**
     * Gets a JSON representation of this document
     *
     * @param writerSettings the json writer settings to use when encoding
     * @param encoder the document codec instance to use to encode the document
     * @return a JSON representation of this document
     * @throws org.bson.codecs.configuration.CodecConfigurationException if the registry does not contain a codec for the document values.
     */
    public String toJson(final JsonWriterSettings writerSettings, final Encoder<Document> encoder) {
        JsonWriter writer = new JsonWriter(new StringWriter(), writerSettings);
        encoder.encode(writer, this, EncoderContext.builder().build());
        return writer.getWriter().toString();
    }

    // Vanilla Map methods delegate to map field

    @Override
    public int size() {
        return documentAsMap.size();
    }

    @Override
    public boolean isEmpty() {
        return documentAsMap.isEmpty();
    }

    @Override
    public boolean containsValue(final Object value) {
        return documentAsMap.containsValue(value);
    }

    @Override
    public boolean containsKey(final Object key) {
        return documentAsMap.containsKey(key);
    }

    @Override
    public Object get(final Object key) {
        return documentAsMap.get(key);
    }

    @Override
    public Object put(final String key, final Object value) {
        return documentAsMap.put(key, value);
    }

    @Override
    public Object remove(final Object key) {
        return documentAsMap.remove(key);
    }

    @Override
    public void putAll(final Map<? extends String, ?> map) {
        documentAsMap.putAll(map);
    }

    @Override
    public void clear() {
        documentAsMap.clear();
    }

    @Override
    public Set<String> keySet() {
        return documentAsMap.keySet();
    }

    @Override
    public Collection<Object> values() {
        return documentAsMap.values();
    }

    @Override
    public Set<Map.Entry<String, Object>> entrySet() {
        return documentAsMap.entrySet();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Document document = (Document) o;

        if (!documentAsMap.equals(document.documentAsMap)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return documentAsMap.hashCode();
    }

    @Override
    public String toString() {
        return "Document{"
               + documentAsMap
               + '}';
    }
}
