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

import java.util.Collection;
import java.util.Objects;

import static org.bson.assertions.Assertions.notNull;
import static org.bson.codecs.BsonTypeClassMap.DEFAULT_BSON_TYPE_CLASS_MAP;

/**
 * A {@code CodecProvider} for classes than implement the {@code Collection} interface.
 *
 * @since 3.3
 */
public class CollectionCodecProvider implements CodecProvider {
    private final BsonTypeClassMap bsonTypeClassMap;
    private final Transformer valueTransformer;

    /**
     * Construct a new instance with a default {@code BsonTypeClassMap} and no {@code Transformer}.
     */
    public CollectionCodecProvider() {
        this(DEFAULT_BSON_TYPE_CLASS_MAP);
    }

    /**
     * Construct a new instance with a default {@code BsonTypeClassMap} and the given {@code Transformer}.  The transformer is used by the
     * IterableCodec as a last step when decoding values.
     *
     * @param valueTransformer the value transformer for decoded values
     */
    public CollectionCodecProvider(final Transformer valueTransformer) {
        this(DEFAULT_BSON_TYPE_CLASS_MAP, valueTransformer);
    }

    /**
     * Construct a new instance with the given instance of {@code BsonTypeClassMap} and no {@code Transformer}.
     *
     * @param bsonTypeClassMap the non-null {@code BsonTypeClassMap} with which to construct instances of {@code DocumentCodec} and {@code
     *                         ListCodec}
     */
    public CollectionCodecProvider(final BsonTypeClassMap bsonTypeClassMap) {
        this(bsonTypeClassMap, null);
    }

    /**
     * Construct a new instance with the given instance of {@code BsonTypeClassMap} and {@code Transformer}.
     *
     * @param bsonTypeClassMap the non-null {@code BsonTypeClassMap} with which to construct instances of {@code DocumentCodec} and {@code
     *                         ListCodec}.
     * @param valueTransformer the value transformer for decoded values
     */
    public CollectionCodecProvider(final BsonTypeClassMap bsonTypeClassMap, final Transformer valueTransformer) {
        this.bsonTypeClassMap = notNull("bsonTypeClassMap", bsonTypeClassMap);
        this.valueTransformer = valueTransformer;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        if (Collection.class.isAssignableFrom(clazz)) {
            return new CollectionCodec(registry, bsonTypeClassMap, valueTransformer, clazz);
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

        CollectionCodecProvider that = (CollectionCodecProvider) o;

        if (!bsonTypeClassMap.equals(that.bsonTypeClassMap)) {
            return false;
        }
        if (!Objects.equals(valueTransformer, that.valueTransformer)) {
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
        return "CollectionCodecProvider{}";
    }
}
