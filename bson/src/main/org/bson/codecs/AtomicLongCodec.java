/*
 * Copyright 2015 MongoDB, Inc.
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

import java.util.concurrent.atomic.AtomicLong;

import static org.bson.codecs.NumberCodecHelper.decodeLong;

/**
 * Encodes and decodes {@code AtomicLong} objects.
 *
 * @since 3.0
 */

public class AtomicLongCodec implements Codec<AtomicLong> {

    @Override
    public void encode(final BsonWriter writer, final AtomicLong value, final EncoderContext encoderContext) {
        writer.writeInt64(value.longValue());
    }

    @Override
    public AtomicLong decode(final BsonReader reader, final DecoderContext decoderContext) {
        return new AtomicLong(decodeLong(reader));
    }

    @Override
    public Class<AtomicLong> getEncoderClass() {
        return AtomicLong.class;
    }
}
