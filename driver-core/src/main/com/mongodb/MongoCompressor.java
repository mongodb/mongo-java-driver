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

package com.mongodb;

import com.mongodb.lang.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Metadata describing a compressor to use for sending and receiving messages to a MongoDB server.
 *
 * @since 3.6
 * @mongodb.server.release 3.4
 */
public final class MongoCompressor {

    /**
     * The property key for defining the compression level.
     */
    public static final String LEVEL = "LEVEL";

    private final String name;
    private final Map<String, Object> properties;


    /**
     * Create an instance for snappy compression.
     *
     * @return A compressor based on the snappy compression algorithm
     * @mongodb.server.release 3.4
     */
    public static MongoCompressor createSnappyCompressor() {
        return new MongoCompressor("snappy");
    }

    /**
     * Create an instance for zlib compression.
     *
     * @return A compressor based on the zlib compression algorithm
     * @mongodb.server.release 3.6
     */
    public static MongoCompressor createZlibCompressor() {
        return new MongoCompressor("zlib");
    }

    /**
     * Create an instance for zstd compression.
     *
     * @return A compressor based on the zstd compression algorithm
     * @mongodb.server.release 4.2
     */
    public static MongoCompressor createZstdCompressor() {
        return new MongoCompressor("zstd");
    }

    /**
     * Gets the name of the compressor.
     *
     * @return the non-null compressor name
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the property with the given key.
     *
     * @param key the key
     * @param defaultValue the default value
     * @param <T> the property value type
     * @return the property value, or the default value if the property is not defined
     */
    @SuppressWarnings("unchecked")
    @Nullable
    public <T> T getProperty(final String key, final T defaultValue) {
        notNull("key", key);

        T value = (T) properties.get(key.toLowerCase());
        return (value == null && !properties.containsKey(key)) ? defaultValue : value;
    }

    /**
     * Gets the property with the given key.
     *
     * @param key the key
     * @param defaultValue the default value
     * @param <T> the property value type
     * @return the property value, or the default value if the property is not defined
     * @throws IllegalArgumentException if the value and default value are null
     * @since 3.7
     */
    public <T> T getPropertyNonNull(final String key, final T defaultValue) {
        T value = getProperty(key, defaultValue);
        if (value == null) {
            throw new IllegalArgumentException();
        }
        return value;
    }

    /**
     * Creates a new compressor from this compressor with the given property added to it.
     *
     * @param key the property key
     * @param value the property value
     * @param <T> the property value type
     * @return the new compressor
     */
    public <T> MongoCompressor withProperty(final String key, final T value) {
        return new MongoCompressor(this, key, value);
    }


    private MongoCompressor(final String name) {
        this.name = name;
        properties = Collections.emptyMap();
    }

    private <T> MongoCompressor(final MongoCompressor from, final String propertyKey, final T propertyValue) {
        notNull("propertyKey", propertyKey);

        this.name = from.name;
        this.properties = new HashMap<>(from.properties);
        this.properties.put(propertyKey.toLowerCase(), propertyValue);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MongoCompressor that = (MongoCompressor) o;

        if (!name.equals(that.name)) {
            return false;
        }
        return properties.equals(that.properties);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + properties.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "MongoCompressor{"
                       + "name='" + name + '\''
                       + ", properties=" + properties
                       + '}';
    }
}
