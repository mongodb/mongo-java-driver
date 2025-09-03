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
import org.bson.BsonWriter;

import static org.bson.internal.NumberCodecHelper.decodeByte;

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
        return decodeByte(reader);
    }

    @Override
    public Class<Byte> getEncoderClass() {
        return Byte.class;
    }
}
