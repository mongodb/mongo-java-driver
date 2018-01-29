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

import java.util.Date;

/**
 * Encodes and decodes {@code java.util.Date} objects.
 *
 * @since 3.0
 */
public class DateCodec implements Codec<Date> {
    @Override
    public void encode(final BsonWriter writer, final Date value, final EncoderContext encoderContext) {
        writer.writeDateTime(value.getTime());
    }

    @Override
    public Date decode(final BsonReader reader, final DecoderContext decoderContext) {
        return new Date(reader.readDateTime());
    }

    @Override
    public Class<Date> getEncoderClass() {
        return Date.class;
    }
}
