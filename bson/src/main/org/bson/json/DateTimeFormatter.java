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

package org.bson.json;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQuery;

import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;

final class DateTimeFormatter {
    static long parse(final String dateTimeString) {
        try {
            return ISO_OFFSET_DATE_TIME.parse(dateTimeString, new TemporalQuery<Instant>() {
                @Override
                public Instant queryFrom(final TemporalAccessor temporal) {
                    return Instant.from(temporal);
                }
            }).toEpochMilli();
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    static String format(final long dateTime) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(dateTime), ZoneId.of("Z")).format(ISO_OFFSET_DATE_TIME);
    }

    private DateTimeFormatter() {
    }
}
