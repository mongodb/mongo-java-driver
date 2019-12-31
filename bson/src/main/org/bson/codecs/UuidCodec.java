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

import org.bson.BSONException;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.UuidRepresentation;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.internal.UuidHelper;

import java.util.UUID;

import static org.bson.assertions.Assertions.notNull;

/**
 * Encodes and decodes {@code UUID} objects.
 *
 * @since 3.0
 */
public class UuidCodec implements Codec<UUID> {

    private final UuidRepresentation uuidRepresentation;

    /**
     * The default UUIDRepresentation is JAVA_LEGACY to be compatible with existing documents
     *
     * @param uuidRepresentation the representation of UUID
     * @see org.bson.UuidRepresentation
     */
    public UuidCodec(final UuidRepresentation uuidRepresentation) {
        notNull("uuidRepresentation", uuidRepresentation);
        this.uuidRepresentation = uuidRepresentation;
    }

    /**
     * The constructor for UUIDCodec, default is JAVA_LEGACY
     */
    public UuidCodec() {
        this.uuidRepresentation = UuidRepresentation.UNSPECIFIED;
    }

    /**
     * The {@code UuidRepresentation} with which this instance is configured
     *
     * @return the uuid representation
     * @since 3.12
     */
    public UuidRepresentation getUuidRepresentation() {
        return uuidRepresentation;
    }

    @Override
    public void encode(final BsonWriter writer, final UUID value, final EncoderContext encoderContext) {
        if (uuidRepresentation == UuidRepresentation.UNSPECIFIED) {
            throw new CodecConfigurationException("The uuidRepresentation has not been specified, so the UUID cannot be encoded.");
        }
        byte[] binaryData = UuidHelper.encodeUuidToBinary(value, uuidRepresentation);
        // changed the default subtype to STANDARD since 3.0
        if (uuidRepresentation == UuidRepresentation.STANDARD) {
            writer.writeBinaryData(new BsonBinary(BsonBinarySubType.UUID_STANDARD, binaryData));
        } else {
            writer.writeBinaryData(new BsonBinary(BsonBinarySubType.UUID_LEGACY, binaryData));
        }
    }

    @Override
    public UUID decode(final BsonReader reader, final DecoderContext decoderContext) {
        byte subType = reader.peekBinarySubType();

        if (subType != BsonBinarySubType.UUID_LEGACY.getValue() && subType != BsonBinarySubType.UUID_STANDARD.getValue()) {
            throw new BSONException("Unexpected BsonBinarySubType");
        }

        byte[] bytes = reader.readBinaryData().getData();

        return UuidHelper.decodeBinaryToUuid(bytes, subType, uuidRepresentation);
    }

    @Override
    public Class<UUID> getEncoderClass() {
        return UUID.class;
    }

    @Override
    public String toString() {
        return "UuidCodec{"
                + "uuidRepresentation=" + uuidRepresentation
                + '}';
    }
}
