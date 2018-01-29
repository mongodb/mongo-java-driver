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
import org.bson.BsonWriter;

import static java.lang.String.format;
import static org.bson.codecs.NumberCodecHelper.decodeDouble;

/**
 * Encodes and decodes {@code Float} objects.
 *
 * @since 3.0
 */
public class FloatCodec implements Codec<Float> {

    @Override
    public void encode(final BsonWriter writer, final Float value, final EncoderContext encoderContext) {
        writer.writeDouble(value);
    }

    @Override
    public Float decode(final BsonReader reader, final DecoderContext decoderContext) {
        double value = decodeDouble(reader);
        if (value < -Float.MAX_VALUE || value > Float.MAX_VALUE) {
            throw new BsonInvalidOperationException(format("%s can not be converted into a Float.", value));
        }
        return (float) value;
    }

    @Override
    public Class<Float> getEncoderClass() {
        return Float.class;
    }
}
