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

import java.util.HashMap;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.bson.assertions.Assertions.notNull;
import static org.bson.codecs.ContainerCodecHelper.readValue;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

/**
 * A Codec for Map instances.
 *
 * @since 3.5
 */
public class MapCodec implements Codec<Map<String, Object>>, OverridableUuidRepresentationCodec<Map<String, Object>> {

    private static final CodecRegistry DEFAULT_REGISTRY = fromProviders(asList(new ValueCodecProvider(), new BsonValueCodecProvider(),
            new DocumentCodecProvider(), new IterableCodecProvider(), new MapCodecProvider()));
    private static final BsonTypeClassMap DEFAULT_BSON_TYPE_CLASS_MAP = new BsonTypeClassMap();
    private final BsonTypeCodecMap bsonTypeCodecMap;
    private final CodecRegistry registry;
    private final Transformer valueTransformer;
    private final UuidRepresentation uuidRepresentation;

    /**
     * Construct a new instance with a default {@code CodecRegistry}
     */
    public MapCodec() {
        this(DEFAULT_REGISTRY);
    }

    /**
     Construct a new instance with the given registry
     *
     * @param registry the registry
     */
    public MapCodec(final CodecRegistry registry) {
        this(registry, DEFAULT_BSON_TYPE_CLASS_MAP);
    }

    /**
     * Construct a new instance with the given registry and BSON type class map.
     *
     * @param registry         the registry
     * @param bsonTypeClassMap the BSON type class map
     */
    public MapCodec(final CodecRegistry registry, final BsonTypeClassMap bsonTypeClassMap) {
        this(registry, bsonTypeClassMap, null);
    }

    /**
     * Construct a new instance with the given registry and BSON type class map. The transformer is applied as a last step when decoding
     * values, which allows users of this codec to control the decoding process.  For example, a user of this class could substitute a
     * value decoded as a Document with an instance of a special purpose class (e.g., one representing a DBRef in MongoDB).
     *
     * @param registry         the registry
     * @param bsonTypeClassMap the BSON type class map
     * @param valueTransformer the value transformer to use as a final step when decoding the value of any field in the map
     */
    public MapCodec(final CodecRegistry registry, final BsonTypeClassMap bsonTypeClassMap, final Transformer valueTransformer) {
        this(registry, new BsonTypeCodecMap(notNull("bsonTypeClassMap", bsonTypeClassMap), registry), valueTransformer,
                UuidRepresentation.UNSPECIFIED);
    }

    private MapCodec(final CodecRegistry registry, final BsonTypeCodecMap bsonTypeCodecMap, final Transformer valueTransformer,
               final UuidRepresentation uuidRepresentation) {
        this.registry = notNull("registry", registry);
        this.bsonTypeCodecMap = bsonTypeCodecMap;
        this.valueTransformer = valueTransformer != null ? valueTransformer : new Transformer() {
            @Override
            public Object transform(final Object value) {
                return value;
            }
        };
        this.uuidRepresentation = uuidRepresentation;
    }

    @Override
    public Codec<Map<String, Object>> withUuidRepresentation(final UuidRepresentation uuidRepresentation) {
        return new MapCodec(registry, bsonTypeCodecMap, valueTransformer, uuidRepresentation);
    }

    @Override
    public void encode(final BsonWriter writer, final Map<String, Object> map, final EncoderContext encoderContext) {
        writer.writeStartDocument();
        for (final Map.Entry<String, Object> entry : map.entrySet()) {
            writer.writeName(entry.getKey());
            writeValue(writer, encoderContext, entry.getValue());
        }
        writer.writeEndDocument();
    }

    @Override
    public Map<String, Object> decode(final BsonReader reader, final DecoderContext decoderContext) {
        Map<String, Object> map = new HashMap<String, Object>();

        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String fieldName = reader.readName();
            map.put(fieldName, readValue(reader, decoderContext, bsonTypeCodecMap, uuidRepresentation, registry, valueTransformer));
        }

        reader.readEndDocument();
        return map;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Class<Map<String, Object>> getEncoderClass() {
        return (Class<Map<String, Object>>) ((Class) Map.class);
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
