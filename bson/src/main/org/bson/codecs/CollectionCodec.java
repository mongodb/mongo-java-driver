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

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.Transformer;
import org.bson.UuidRepresentation;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.configuration.CodecRegistry;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;

import static org.bson.assertions.Assertions.notNull;
import static org.bson.codecs.ContainerCodecHelper.getCodec;

/**
 * A parameterized Codec for {@code Collection<Object>}.
 *
 * <p>Supports {@link Collection}, {@link List}, {@link java.util.AbstractCollection}, {@link java.util.AbstractList},
 * {@link java.util.Set}, {@link java.util.NavigableSet}, {@link java.util.SortedSet}, {@link java.util.AbstractSet} or any
 * concrete class that implements {@code Collection} and has a public no-args constructor. If the generic type is
 * {@code Collection<Object>}, {@code List<Object>}, {@code AbstractCollection<Object>}, or {@code AbstractList<Object>},
 * it constructs {@code ArrayList<Object} instances when decoding. If the generic type is {@code Set<Object} or
 * {@code AbstractSet<Object}, it constructs {@code HashSet<Object} instances when decoding. If the generic type is
 * {@code NavigableSet <Object} or {@code SortedSet<Object}, it constructs {@code TreeSet<Object} instances when decoding.</p>
 *
 * <p>Replaces the now deprecated {@link IterableCodec}.</p>
 *
 * @param <C> the actual type of the Collection, e.g. {@code List<Object>}
 */
@SuppressWarnings("rawtypes")
final class CollectionCodec<C extends Collection<Object>> extends AbstractCollectionCodec<Object, C>
        implements OverridableUuidRepresentationCodec<C>, Parameterizable {

    private final CodecRegistry registry;
    private final BsonTypeCodecMap bsonTypeCodecMap;
    private final Transformer valueTransformer;
    private final UuidRepresentation uuidRepresentation;

    /**
     * Construct a new instance with the given {@code CodecRegistry} and {@code BsonTypeClassMap}.
     *
     * @param registry the non-null codec registry
     * @param bsonTypeClassMap the non-null BsonTypeClassMap
     * @param valueTransformer the value Transformer
     * @param clazz the class
     */
    CollectionCodec(final CodecRegistry registry, final BsonTypeClassMap bsonTypeClassMap, final Transformer valueTransformer,
                    final Class<C> clazz) {
        this(registry, new BsonTypeCodecMap(notNull("bsonTypeClassMap", bsonTypeClassMap), registry), valueTransformer, clazz,
                UuidRepresentation.UNSPECIFIED);
    }
    private CollectionCodec(final CodecRegistry registry, final BsonTypeCodecMap bsonTypeCodecMap, final Transformer valueTransformer,
                            final Class<C> clazz, final UuidRepresentation uuidRepresentation) {
        super(clazz);
        this.registry = notNull("registry", registry);
        this.bsonTypeCodecMap = bsonTypeCodecMap;
        this.valueTransformer = valueTransformer != null ? valueTransformer : (value) -> value;
        this.uuidRepresentation = uuidRepresentation;
    }


    @SuppressWarnings("unchecked")
    @Override
    public Codec<?> parameterize(final CodecRegistry codecRegistry, final List<Type> types) {
        if (types.size() != 1) {
            throw new CodecConfigurationException("Expected only one parameterized type for an Iterable, but found " + types.size());
        }

        return new ParameterizedCollectionCodec(getCodec(codecRegistry, types.get(0)), getEncoderClass());
    }

    @Override
    public Codec<C> withUuidRepresentation(final UuidRepresentation uuidRepresentation) {
        if (this.uuidRepresentation.equals(uuidRepresentation)) {
            return this;
        }
        return new CollectionCodec<C>(registry, bsonTypeCodecMap, valueTransformer, getEncoderClass(), uuidRepresentation);
    }

    @Override
    Object readValue(final BsonReader reader, final DecoderContext decoderContext) {
        return ContainerCodecHelper.readValue(reader, decoderContext, bsonTypeCodecMap, uuidRepresentation, registry, valueTransformer);
    }

    @SuppressWarnings("unchecked")
    @Override
    void writeValue(final BsonWriter writer, final Object value, final EncoderContext encoderContext) {
        Codec codec = registry.get(value.getClass());
        encoderContext.encodeWithChildContext(codec, writer, value);
    }
}
