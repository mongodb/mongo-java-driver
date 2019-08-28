/*
 * Copyright 2008-present MongoDB, Inc.
 * Copyright 2018 Cezary Bartosiak
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

package org.bson.codecs.jsr310;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecConfigurationException;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static java.lang.String.format;

/**
 * LocalDateTime Codec.
 *
 * <p>Encodes and decodes {@code LocalDateTime} objects to and from {@code DateTime}. Data is stored to millisecond accuracy.</p>
 * <p>Converts the {@code LocalDateTime} values to and from {@link ZoneOffset#UTC}.</p>
 *
 * @mongodb.driver.manual reference/bson-types
 * @since 3.7
 */
public class LocalDateTimeCodec extends DateTimeBasedCodec<LocalDateTime> {

    @Override
    public LocalDateTime decode(final BsonReader reader, final DecoderContext decoderContext) {
        return Instant.ofEpochMilli(validateAndReadDateTime(reader)).atZone(ZoneOffset.UTC).toLocalDateTime();
    }

    /**
     * {@inheritDoc}
     * <p>Converts the {@code LocalDateTime} to {@link ZoneOffset#UTC} via {@link LocalDateTime#toInstant(ZoneOffset)}.</p>
     * @throws CodecConfigurationException if the LocalDateTime cannot be converted to a valid Bson DateTime.
     */
    @Override
    public void encode(final BsonWriter writer, final LocalDateTime value, final EncoderContext encoderContext) {
        try {
            writer.writeDateTime(value.toInstant(ZoneOffset.UTC).toEpochMilli());
        } catch (ArithmeticException e) {
            throw new CodecConfigurationException(format("Unsupported LocalDateTime value '%s' could not be converted to milliseconds: %s",
                    value, e.getMessage()), e);
        }
    }

    @Override
    public Class<LocalDateTime> getEncoderClass() {
        return LocalDateTime.class;
    }
}
