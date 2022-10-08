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
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.bson.assertions.Assertions.notNull;
import static org.bson.codecs.ContainerCodecHelper.getCodec;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

/**
 * A parameterized Codec for {@code Map<String, Object>}.
 *
 * <p>Supports {@link Map}, {@link java.util.NavigableMap}, {@link java.util.AbstractMap} or any concrete class that implements {@code
 * Map} and has a public no-args constructor. If the generic type is {@code Map<String, Object}, it constructs
 * {@code HashMap<String, Object} instances when decoding. If the generic type is {@code NavigableMap<String, Object}, it constructs
 * {@code TreeMap<String, Object} instances when decoding.</p>
 *
 * <p>Replaces the now deprecated {@link MapCodec}.</p>
 *
 * @param <M> the actual type of the Map, e.g. {@code NavigableMap<String, Object>}
 */
@SuppressWarnings("rawtypes")
final class MapCodecV2<M extends Map<String, Object>> extends AbstractMapCodec<Object, M>
        implements OverridableUuidRepresentationCodec<M>, Parameterizable {

    private static final CodecRegistry DEFAULT_REGISTRY = fromProviders(asList(new ValueCodecProvider(), new BsonValueCodecProvider(),
            new DocumentCodecProvider(), new CollectionCodecProvider(), new MapCodecProvider()));
    private static final BsonTypeClassMap DEFAULT_BSON_TYPE_CLASS_MAP = new BsonTypeClassMap();
    private final BsonTypeCodecMap bsonTypeCodecMap;
    private final CodecRegistry registry;
    private final Transformer valueTransformer;
    private final UuidRepresentation uuidRepresentation;

    /**
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
    MapCodecV2(final CodecRegistry registry, final BsonTypeClassMap bsonTypeClassMap, final Transformer valueTransformer,
                      final Class<M> clazz) {
        this(registry, new BsonTypeCodecMap(notNull("bsonTypeClassMap", bsonTypeClassMap), registry), valueTransformer,
                UuidRepresentation.UNSPECIFIED, clazz);
    }

    private MapCodecV2(final CodecRegistry registry, final BsonTypeCodecMap bsonTypeCodecMap, final Transformer valueTransformer,
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
        return new MapCodecV2<M>(registry, bsonTypeCodecMap, valueTransformer, uuidRepresentation, getEncoderClass());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Codec<?> parameterize(final CodecRegistry codecRegistry, final List<Type> types) {
        if (types.size() != 2) {
            throw new CodecConfigurationException("Expected two parameterized type for an Iterable, but found "
                    + types.size());
        }
        Type genericTypeOfMapKey = types.get(0);
        if (!genericTypeOfMapKey.getTypeName().equals("java.lang.String")) {
            throw new CodecConfigurationException("Unsupported key type for Map: " + genericTypeOfMapKey.getTypeName());
        }
        return new ParameterizedMapCodec(getCodec(codecRegistry, types.get(1)), getEncoderClass());
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
