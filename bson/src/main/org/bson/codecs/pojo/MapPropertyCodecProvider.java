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
package org.bson.codecs.pojo;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecConfigurationException;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static java.lang.String.format;

final class MapPropertyCodecProvider implements PropertyCodecProvider {

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public <T> Codec<T> get(final TypeWithTypeParameters<T> type, final PropertyCodecRegistry registry) {
        if (Map.class.isAssignableFrom(type.getType()) && type.getTypeParameters().size() == 2) {
            Class<?> keyType = type.getTypeParameters().get(0).getType();
            if (!keyType.equals(String.class)) {
                throw new CodecConfigurationException(format("Invalid Map type. Maps MUST have string keys, found %s instead.", keyType));
            }

            try {
                return new MapCodec(type.getType(), registry.get(type.getTypeParameters().get(1)));
            } catch (CodecConfigurationException e) {
                if (type.getTypeParameters().get(1).getType() == Object.class) {
                    try {
                        return (Codec<T>) registry.get(TypeData.builder(Map.class).build());
                    } catch (CodecConfigurationException e1) {
                        // Ignore and return original exception
                    }
                }
                throw e;
            }
        } else {
            return null;
        }
    }

    private static class MapCodec<T> implements Codec<Map<String, T>> {
        private final Class<Map<String, T>> encoderClass;
        private final Codec<T> codec;

        MapCodec(final Class<Map<String, T>> encoderClass, final Codec<T> codec) {
            this.encoderClass = encoderClass;
            this.codec = codec;
        }

        @Override
        public void encode(final BsonWriter writer, final Map<String, T> map, final EncoderContext encoderContext) {
            writer.writeStartDocument();
            for (final Entry<String, T> entry : map.entrySet()) {
                writer.writeName(entry.getKey());
                if (entry.getValue() == null) {
                    writer.writeNull();
                } else {
                    codec.encode(writer, entry.getValue(), encoderContext);
                }
            }
            writer.writeEndDocument();
        }

        @Override
        public Map<String, T> decode(final BsonReader reader, final DecoderContext context) {
            reader.readStartDocument();
            Map<String, T> map = getInstance();
            while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                if (reader.getCurrentBsonType() == BsonType.NULL) {
                    map.put(reader.readName(), null);
                    reader.readNull();
                } else {
                    map.put(reader.readName(), codec.decode(reader, context));
                }
            }
            reader.readEndDocument();
            return map;
        }

        @Override
        public Class<Map<String, T>> getEncoderClass() {
            return encoderClass;
        }

        private Map<String, T> getInstance() {
            if (encoderClass.isInterface()) {
                return new HashMap<String, T>();
            }
            try {
                return encoderClass.newInstance();
            } catch (final Exception e) {
                throw new CodecConfigurationException(e.getMessage(), e);
            }
        }
    }
}
