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
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.Transformer;
import org.bson.UuidRepresentation;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.ArrayList;
import java.util.List;

import static org.bson.assertions.Assertions.notNull;
import static org.bson.codecs.ContainerCodecHelper.readValue;

/**
 * Encodes and decodes {@code Iterable} objects.
 */
@SuppressWarnings("rawtypes")
class IterableCodec implements Codec<Iterable>, OverridableUuidRepresentationCodec<Iterable> {

    private final CodecRegistry registry;
    private final BsonTypeCodecMap bsonTypeCodecMap;
    private final Transformer valueTransformer;
    private final UuidRepresentation uuidRepresentation;

    IterableCodec(final CodecRegistry registry, final BsonTypeClassMap bsonTypeClassMap, final Transformer valueTransformer) {
        this(registry, new BsonTypeCodecMap(notNull("bsonTypeClassMap", bsonTypeClassMap), registry), valueTransformer,
                UuidRepresentation.UNSPECIFIED);
    }

    private IterableCodec(final CodecRegistry registry, final BsonTypeCodecMap bsonTypeCodecMap, final Transformer valueTransformer,
                          final UuidRepresentation uuidRepresentation) {
        this.registry = notNull("registry", registry);
        this.bsonTypeCodecMap = bsonTypeCodecMap;
        this.valueTransformer = valueTransformer != null ? valueTransformer : objectToTransform -> objectToTransform;
        this.uuidRepresentation = uuidRepresentation;
    }


    @Override
    public Codec<Iterable> withUuidRepresentation(final UuidRepresentation uuidRepresentation) {
        return new IterableCodec(registry, bsonTypeCodecMap, valueTransformer, uuidRepresentation);
    }

    @Override
    public Iterable decode(final BsonReader reader, final DecoderContext decoderContext) {
        reader.readStartArray();

        List<Object> list = new ArrayList<>();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            list.add(readValue(reader, decoderContext, bsonTypeCodecMap, uuidRepresentation, registry, valueTransformer));
        }

        reader.readEndArray();

        return list;
    }

    @Override
    public void encode(final BsonWriter writer, final Iterable value, final EncoderContext encoderContext) {
        writer.writeStartArray();
        for (final Object cur : value) {
            writeValue(writer, encoderContext, cur);
        }
        writer.writeEndArray();
    }

    @Override
    public Class<Iterable> getEncoderClass() {
        return Iterable.class;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void writeValue(final BsonWriter writer, final EncoderContext encoderContext, final Object value) {
        if (value == null) {
            writer.writeNull();
        } else {
            Codec codec = registry.get(value.getClass());
            encoderContext.encodeWithChildContext(codec, writer, value);
        }
    }
}
