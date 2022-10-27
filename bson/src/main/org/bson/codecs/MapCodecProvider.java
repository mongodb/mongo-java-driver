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

package org.bson.codecs;

import org.bson.Transformer;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.Map;

import static org.bson.assertions.Assertions.notNull;
import static org.bson.codecs.BsonTypeClassMap.DEFAULT_BSON_TYPE_CLASS_MAP;

/**
 * A {@code CodecProvider} for the Map class and all the default Codec implementations on which it depends.
 *
 * @since 3.5
 */
public class MapCodecProvider implements CodecProvider {
    private final BsonTypeClassMap bsonTypeClassMap;
    private final Transformer valueTransformer;

    /**
     * Construct a new instance with a default {@code BsonTypeClassMap}.
     */
    public MapCodecProvider() {
        this(DEFAULT_BSON_TYPE_CLASS_MAP);
    }

    /**
     * Construct a new instance with the given instance of {@code BsonTypeClassMap}.
     *
     * @param bsonTypeClassMap the non-null {@code BsonTypeClassMap} with which to construct instances of {@code DocumentCodec} and {@code
     *                         ListCodec}
     */
    public MapCodecProvider(final BsonTypeClassMap bsonTypeClassMap) {
        this(bsonTypeClassMap, null);
    }

    /**
     * Construct a new instance with a default {@code BsonTypeClassMap} and the given {@code Transformer}.  The transformer is used by the
     * MapCodec as a last step when decoding values.
     *
     * @param valueTransformer the value transformer for decoded values
     */
    public MapCodecProvider(final Transformer valueTransformer) {
        this(DEFAULT_BSON_TYPE_CLASS_MAP, valueTransformer);
    }

    /**
     * Construct a new instance with the given instance of {@code BsonTypeClassMap}.
     *
     * @param bsonTypeClassMap the non-null {@code BsonTypeClassMap} with which to construct instances of {@code MapCodec}.
     * @param valueTransformer the value transformer for decoded values
     */
    public MapCodecProvider(final BsonTypeClassMap bsonTypeClassMap, final Transformer valueTransformer) {
        this.bsonTypeClassMap = notNull("bsonTypeClassMap", bsonTypeClassMap);
        this.valueTransformer = valueTransformer;
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        if (Map.class.isAssignableFrom(clazz)) {
            return new MapCodecV2(registry, bsonTypeClassMap, valueTransformer, clazz);
        }

        return null;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MapCodecProvider that = (MapCodecProvider) o;
        if (!bsonTypeClassMap.equals(that.bsonTypeClassMap)) {
            return false;
        }
        if (valueTransformer != null ? !valueTransformer.equals(that.valueTransformer) : that.valueTransformer != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = bsonTypeClassMap.hashCode();
        result = 31 * result + (valueTransformer != null ? valueTransformer.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "MapCodecProvider{}";
    }
}
