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
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.bson.assertions.Assertions.notNull;
import static org.bson.codecs.BsonTypeClassMap.DEFAULT_BSON_TYPE_CLASS_MAP;
import static org.bson.codecs.ContainerCodecHelper.getCodec;

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
    public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        return get(clazz, Collections.emptyList(), registry);
    }

    @Override
    public <T> Codec<T> get(final Class<T> clazz, final List<Type> typeArguments, final CodecRegistry registry) {
        if (Collection.class.isAssignableFrom(clazz)) {
            int typeArgumentsSize = typeArguments.size();
            switch (typeArgumentsSize) {
                case 0: {
                    @SuppressWarnings({"unchecked", "rawtypes"})
                    Codec<T> result = new CollectionCodec(registry, bsonTypeClassMap, valueTransformer, clazz);
                    return result;
                }
                case 1: {
                    @SuppressWarnings({"unchecked", "rawtypes"})
                    Codec<T> result = new ParameterizedCollectionCodec(getCodec(registry, typeArguments.get(0)), clazz);
                    return result;
                }
                default: {
                    throw new CodecConfigurationException("Expected only one type argument for a Collection, but found " + typeArgumentsSize);
                }
            }
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
        return Objects.hash(bsonTypeClassMap, valueTransformer);
    }

    @Override
    public String toString() {
        return "CollectionCodecProvider{}";
    }
}
