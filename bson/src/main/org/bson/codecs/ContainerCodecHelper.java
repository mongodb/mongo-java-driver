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
import org.bson.Transformer;
import org.bson.UuidRepresentation;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.UUID;

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
            Codec<?> codec = bsonTypeCodecMap.get(bsonType);

            if (bsonType == BsonType.BINARY && reader.peekBinarySize() == 16) {
                switch (reader.peekBinarySubType()) {
                    case 3:
                        if (uuidRepresentation == UuidRepresentation.JAVA_LEGACY
                                || uuidRepresentation == UuidRepresentation.C_SHARP_LEGACY
                                || uuidRepresentation == UuidRepresentation.PYTHON_LEGACY) {
                            codec = registry.get(UUID.class);
                        }
                        break;
                    case 4:
                        if (uuidRepresentation == UuidRepresentation.STANDARD) {
                            codec = registry.get(UUID.class);
                        }
                        break;
                    default:
                        break;
                }
            }
            return valueTransformer.transform(codec.decode(reader, decoderContext));
        }
    }

    private ContainerCodecHelper() {
    }
}
