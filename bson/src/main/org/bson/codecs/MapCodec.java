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
import org.bson.codecs.configuration.CodecRegistry;

import java.util.Map;

import static org.bson.assertions.Assertions.notNull;

/**
 * A codec for {@code Map<String, Object>}.
 *
 * <p>Supports {@link Map}, {@link java.util.NavigableMap}, {@link java.util.AbstractMap} or any concrete class that implements {@code
 * Map} and has a public no-args constructor. If the type argument is {@code Map<String, Object>}, it constructs
 * {@code HashMap<String, Object>} instances when decoding. If the type argument is {@code NavigableMap<String, Object>}, it constructs
 * {@code TreeMap<String, Object>} instances when decoding.</p>
 *
 * @param <M> the actual type of the Map, e.g. {@code NavigableMap<String, Object>}
 */
@SuppressWarnings("rawtypes")
final class MapCodec<M extends Map<String, Object>> extends AbstractMapCodec<Object, M>
        implements OverridableUuidRepresentationCodec<M> {

    private final BsonTypeCodecMap bsonTypeCodecMap;
    private final CodecRegistry registry;
    private final Transformer valueTransformer;
    private final UuidRepresentation uuidRepresentation;

    /**
     * Construct a new instance with the given registry and BSON type class map. The transformer is applied as a last step when decoding
     * values, which allows users of this codec to control the decoding process.  For example, a user of this class could substitute a
     * value decoded as a Document with an instance of a special purpose class (e.g., one representing a DBRef in MongoDB).
     *
     * @param registry         the registry
     * @param bsonTypeClassMap the BSON type class map
     * @param valueTransformer the value transformer to use as a final step when decoding the value of any field in the map
     * @param clazz            the Map subclass
     * @since 4.8
     */
    MapCodec(final CodecRegistry registry, final BsonTypeClassMap bsonTypeClassMap, final Transformer valueTransformer,
                      final Class<M> clazz) {
        this(registry, new BsonTypeCodecMap(notNull("bsonTypeClassMap", bsonTypeClassMap), registry), valueTransformer,
                UuidRepresentation.UNSPECIFIED, clazz);
    }

    private MapCodec(final CodecRegistry registry, final BsonTypeCodecMap bsonTypeCodecMap, final Transformer valueTransformer,
                       final UuidRepresentation uuidRepresentation, final Class<M> clazz) {
        super(clazz);
        this.registry = notNull("registry", registry);
        this.bsonTypeCodecMap = bsonTypeCodecMap;
        this.valueTransformer = valueTransformer != null ? valueTransformer : (value) -> value;
        this.uuidRepresentation = uuidRepresentation;
    }

    @Override
    public Codec<M> withUuidRepresentation(final UuidRepresentation uuidRepresentation) {
        if (this.uuidRepresentation.equals(uuidRepresentation)) {
            return this;
        }
        return new MapCodec<>(registry, bsonTypeCodecMap, valueTransformer, uuidRepresentation, getEncoderClass());
    }

    @Override
    Object readValue(final BsonReader reader, final DecoderContext decoderContext) {
        return ContainerCodecHelper.readValue(reader, decoderContext, bsonTypeCodecMap, uuidRepresentation, registry, valueTransformer);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    void writeValue(final BsonWriter writer, final Object value, final EncoderContext encoderContext) {
        Codec codec = registry.get(value.getClass());
        encoderContext.encodeWithChildContext(codec, writer, value);
    }
}
