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

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;

/**
 * LocalTime Codec.
 *
 * <p>Encodes and decodes {@code LocalTime} objects to and from {@code DateTime}. Data is stored to millisecond accuracy.</p>
 * <p>Converts the {@code LocalTime} values to and from EpochDay at {@link ZoneOffset#UTC}.</p>
 *
 * @mongodb.driver.manual reference/bson-types
 * @since 3.7
 */
public class LocalTimeCodec extends DateTimeBasedCodec<LocalTime> {

    @Override
    public LocalTime decode(final BsonReader reader, final DecoderContext decoderContext) {
        return Instant.ofEpochMilli(validateAndReadDateTime(reader)).atOffset(ZoneOffset.UTC).toLocalTime();
    }

    /**
     * {@inheritDoc}
     * <p>Converts the {@code LocalTime} to {@link ZoneOffset#UTC} at EpochDay via {@link LocalTime#atDate(LocalDate)} and
     * {@link java.time.LocalDateTime#toInstant(ZoneOffset)}.</p>
     */
    @Override
    public void encode(final BsonWriter writer, final LocalTime value, final EncoderContext encoderContext) {
        writer.writeDateTime(value.atDate(LocalDate.ofEpochDay(0L)).toInstant(ZoneOffset.UTC).toEpochMilli());
    }

    @Override
    public Class<LocalTime> getEncoderClass() {
        return LocalTime.class;
    }
}
