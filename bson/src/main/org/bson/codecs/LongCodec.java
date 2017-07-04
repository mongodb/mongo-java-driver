/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import static org.bson.codecs.NumberCodecHelper.decodeLong;

/**
 * Encodes and decodes {@code Long} objects.
 *
 * @since 3.0
 */

public class LongCodec implements Codec<Long> {

    @Override
    public void encode(final BsonWriter writer, final Long value, final EncoderContext encoderContext) {
        writer.writeInt64(value);
    }

    @Override
    public Long decode(final BsonReader reader, final DecoderContext decoderContext) {
        return decodeLong(reader);
    }

    @Override
    public Class<Long> getEncoderClass() {
        return Long.class;
    }
}
