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

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Encodes and decodes {@code AtomicBoolean} objects.
 *
 * @since 3.0
 */

public class AtomicBooleanCodec implements Codec<AtomicBoolean> {
    @Override
    public void encode(final BsonWriter writer, final AtomicBoolean value, final EncoderContext encoderContext) {
        writer.writeBoolean(value.get());
    }

    @Override
    public AtomicBoolean decode(final BsonReader reader, final DecoderContext decoderContext) {
        return new AtomicBoolean(reader.readBoolean());
    }

    @Override
    public Class<AtomicBoolean> getEncoderClass() {
        return AtomicBoolean.class;
    }
}
