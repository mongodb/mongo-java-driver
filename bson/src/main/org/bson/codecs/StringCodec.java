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

import org.bson.BsonInvalidOperationException;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.types.ObjectId;

/**
 * Encodes and decodes {@code String} objects.
 *
 * @since 3.0
 */
public class StringCodec implements Codec<String>, RepresentationConfigurable<String> {
    private final BsonType representation;

    /**
     * Constructs a StringCodec with a String representation.
     */
    public StringCodec() {
        representation = BsonType.STRING;
    }

    private StringCodec(final BsonType representation) {
        this.representation = representation;
    }

    @Override
    public BsonType getRepresentation() {
        return representation;
    }

    @Override
    public Codec<String> withRepresentation(final BsonType representation) {
        if (representation != BsonType.OBJECT_ID && representation != BsonType.STRING) {
            throw new CodecConfigurationException(representation + " is not a supported representation for StringCodec");
        }
        return new StringCodec(representation);
    }


    @Override
    public void encode(final BsonWriter writer, final String value, final EncoderContext encoderContext) {
        switch (representation) {
            case STRING:
                writer.writeString(value);
                break;
            case OBJECT_ID:
                writer.writeObjectId(new ObjectId(value));
                break;
            default:
                throw new BsonInvalidOperationException("Cannot encode a String to a " + representation);
        }
    }

    @Override
    public String decode(final BsonReader reader, final DecoderContext decoderContext) {
        switch (representation) {
            case STRING:
                if (reader.getCurrentBsonType() == BsonType.SYMBOL) {
                    return reader.readSymbol();
                } else {
                    return reader.readString();
                }
            case OBJECT_ID:
                return reader.readObjectId().toHexString();
            default:
                throw new CodecConfigurationException("Cannot decode " + representation + " to a String");
        }
    }

    @Override
    public Class<String> getEncoderClass() {
        return String.class;
    }
}
