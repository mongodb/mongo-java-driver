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

import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonInvalidOperationException;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.Int8Vector;

/**
 * Encodes and decodes {@link Int8Vector} objects.
 *
 * @since 5.3
 */
final class Int8VectorCodec implements Codec<Int8Vector> {

    @Override
    public void encode(final BsonWriter writer, final Int8Vector vectorToEncode, final EncoderContext encoderContext) {
        writer.writeBinaryData(new BsonBinary(vectorToEncode));
    }

    @Override
    public Int8Vector decode(final BsonReader reader, final DecoderContext decoderContext) {
        byte subType = reader.peekBinarySubType();

        if (subType != BsonBinarySubType.VECTOR.getValue()) {
            throw new BsonInvalidOperationException("Expected vector binary subtype " + BsonBinarySubType.VECTOR.getValue() + " but found: " + subType);
        }

        return reader.readBinaryData()
                .asBinary()
                .asVector()
                .asInt8Vector();
    }


    @Override
    public Class<Int8Vector> getEncoderClass() {
        return Int8Vector.class;
    }
}

