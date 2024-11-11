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

import org.bson.BsonBinarySubType;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.Transformer;
import org.bson.UuidRepresentation;
import org.bson.Vector;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.configuration.CodecRegistry;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.UUID;

import static org.bson.internal.UuidHelper.isLegacyUUID;

/**
 * Helper methods for Codec implementations for containers, e.g. {@code Map} and {@code Iterable}.
 */
final class ContainerCodecHelper {

    static Object readValue(final BsonReader reader, final DecoderContext decoderContext,
            final BsonTypeCodecMap bsonTypeCodecMap, final UuidRepresentation uuidRepresentation,
            final CodecRegistry registry, final Transformer valueTransformer) {

        BsonType bsonType = reader.getCurrentBsonType();
        if (bsonType == BsonType.NULL) {
            reader.readNull();
            return null;
        } else {
            Codec<?> currentCodec = bsonTypeCodecMap.get(bsonType);

            if (bsonType == BsonType.BINARY) {
                byte binarySubType = reader.peekBinarySubType();
                currentCodec = getBinarySubTypeCodec(
                        reader,
                        uuidRepresentation,
                        registry, binarySubType,
                        currentCodec);
            }

            return valueTransformer.transform(currentCodec.decode(reader, decoderContext));
        }
    }

    private static Codec<?> getBinarySubTypeCodec(final BsonReader reader,
                                                  final UuidRepresentation uuidRepresentation,
                                                  final CodecRegistry registry,
                                                  final byte binarySubType,
                                                  final Codec<?> binaryTypeCodec) {

        if (binarySubType == BsonBinarySubType.VECTOR.getValue()) {
            Codec<Vector> vectorCodec = registry.get(Vector.class, registry);
            if (vectorCodec != null) {
                return vectorCodec;
            }
        } else if (reader.peekBinarySize() == 16) {
            switch (binarySubType) {
                case 3:
                    if (isLegacyUUID(uuidRepresentation)) {
                        return registry.get(UUID.class);
                    }
                    break;
                case 4:
                    if (uuidRepresentation == UuidRepresentation.STANDARD) {
                        return  registry.get(UUID.class);
                    }
                    break;
                default:
                    break;
            }
        }

        return binaryTypeCodec;
    }

    static Codec<?> getCodec(final CodecRegistry codecRegistry, final Type type) {
        if (type instanceof Class) {
            return codecRegistry.get((Class<?>) type);
        } else if (type instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) type;
            return codecRegistry.get((Class<?>) parameterizedType.getRawType(), Arrays.asList(parameterizedType.getActualTypeArguments()));
        } else {
            throw new CodecConfigurationException("Unsupported generic type of container: " + type);
        }
    }

    private ContainerCodecHelper() {
    }
}
