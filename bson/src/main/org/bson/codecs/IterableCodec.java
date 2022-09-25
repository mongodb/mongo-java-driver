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

import static org.bson.assertions.Assertions.notNull;
import static org.bson.codecs.ContainerCodecHelper.getCodec;

/**
 * Encodes and decodes {@code Iterable} objects.
 *
 * @since 3.3
 */
@SuppressWarnings("rawtypes")
public class IterableCodec extends AbstractIterableCodec<Object>
        implements OverridableUuidRepresentationCodec<Iterable<Object>>, Parameterizable {

    private final CodecRegistry registry;
    private final BsonTypeCodecMap bsonTypeCodecMap;
    private final Transformer valueTransformer;
    private final UuidRepresentation uuidRepresentation;

    /**
     * Construct a new instance with the given {@code CodecRegistry} and {@code BsonTypeClassMap}.
     *
     * @param registry the non-null codec registry
     * @param bsonTypeClassMap the non-null BsonTypeClassMap
     */
    public IterableCodec(final CodecRegistry registry, final BsonTypeClassMap bsonTypeClassMap) {
        this(registry, bsonTypeClassMap, null);
    }

    /**
     * Construct a new instance with the given {@code CodecRegistry} and {@code BsonTypeClassMap}.
     *
     * @param registry the non-null codec registry
     * @param bsonTypeClassMap the non-null BsonTypeClassMap
     * @param valueTransformer the value Transformer
     */
    public IterableCodec(final CodecRegistry registry, final BsonTypeClassMap bsonTypeClassMap, final Transformer valueTransformer) {
        this(registry, new BsonTypeCodecMap(notNull("bsonTypeClassMap", bsonTypeClassMap), registry), valueTransformer,
                UuidRepresentation.UNSPECIFIED);
    }
    private IterableCodec(final CodecRegistry registry, final BsonTypeCodecMap bsonTypeCodecMap, final Transformer valueTransformer,
                         final UuidRepresentation uuidRepresentation) {
        this.registry = notNull("registry", registry);
        this.bsonTypeCodecMap = bsonTypeCodecMap;
        this.valueTransformer = valueTransformer != null ? valueTransformer : new Transformer() {
            @Override
            public Object transform(final Object objectToTransform) {
                return objectToTransform;
            }
        };
        this.uuidRepresentation = uuidRepresentation;
    }


    @Override
    public Codec<?> parameterize(final CodecRegistry codecRegistry, final List<Type> types) {
        if (types.size() != 1) {
            throw new CodecConfigurationException("Expected only one parameterized type for an Iterable, but found " + types.size());
        }

        return new ParameterizedIterableCodec<>(getCodec(codecRegistry, types.get(0)));
    }

    @Override
    public Codec<Iterable<Object>> withUuidRepresentation(final UuidRepresentation uuidRepresentation) {
        if (this.uuidRepresentation.equals(uuidRepresentation)) {
            return this;
        }
        return new IterableCodec(registry, bsonTypeCodecMap, valueTransformer, uuidRepresentation);
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
