/*
 * Copyright (c) 2008-2017 MongoDB, Inc.
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
import org.bson.BsonWriter;

import static java.lang.String.format;
import static org.bson.codecs.NumberCodecHelper.decodeInt;

/**
 * Encodes and decodes {@code Byte} objects.
 *
 * @since 3.0
 */
public class ByteCodec implements Codec<Byte> {

    @Override
    public void encode(final BsonWriter writer, final Byte value, final EncoderContext encoderContext) {
        writer.writeInt32(value);
    }

    @Override
    public Byte decode(final BsonReader reader, final DecoderContext decoderContext) {
        int value = decodeInt(reader);
        if (value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
            throw new BsonInvalidOperationException(format("%s can not be converted into a Byte.", value));
        }
        return (byte) value;
    }

    @Override
    public Class<Byte> getEncoderClass() {
        return Byte.class;
    }
}
