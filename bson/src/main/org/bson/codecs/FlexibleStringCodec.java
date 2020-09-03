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
 * A codec capable of decoding different types to Strings and encoding
 * Strings to different types depending on the provided BsonRepresentation.
 *
 * @since 4.2
 */
public class FlexibleStringCodec extends StringCodec implements FlexibleCodec<String> {
    private BsonType bsonRep;

    /**
     * Constructs a new instance with a String BsonRepresentation.
     */
    public FlexibleStringCodec() {
        super();
        bsonRep = BsonType.STRING;
    }

    @Override
    public void setBsonRep(final BsonType bsonRep) {
        this.bsonRep = bsonRep;
    }

    @Override
    public void encode(final BsonWriter writer, final String value, final EncoderContext encoderContext) {
        switch (bsonRep) {
            case STRING:
                super.encode(writer, value, encoderContext);
                break;
            case OBJECT_ID:
                writer.writeObjectId(new ObjectId(value));
                break;
            default:
                throw new BsonInvalidOperationException("Cannot encode a String to a " + bsonRep);
        }
    }

    @Override
    public String decode(final BsonReader reader, final DecoderContext decoderContext) {
        switch (bsonRep) {
            case STRING:
                return super.decode(reader, decoderContext);
            case OBJECT_ID:
                return reader.readObjectId().toHexString();
            default:
                throw new CodecConfigurationException("Cannot decode " + bsonRep + " to a String");
        }

    }
}
