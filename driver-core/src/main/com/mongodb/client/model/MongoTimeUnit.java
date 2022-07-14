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
package com.mongodb.client.model;

import org.bson.conversions.Bson;
import com.mongodb.client.model.densify.DensifyRange;

/**
 * Units for specifying time-based values.
 *
 * @see Windows
 * @see WindowOutputFields
 * @see DensifyRange
 * @mongodb.server.release 5.0
 * @since 4.3
 */
public enum MongoTimeUnit {
    /**
     * {@linkplain #value() "year"}
     */
    YEAR("year", false),
    /**
     * {@linkplain #value() "quarter"}
     */
    QUARTER("quarter", false),
    /**
     * {@linkplain #value() "month"}
     */
    MONTH("month", false),
    /**
     * {@linkplain #value() "week"}
     */
    WEEK("week", true),
    /**
     * {@linkplain #value() "day"}
     */
    DAY("day", true),
    /**
     * {@linkplain #value() "hour"}
     */
    HOUR("hour", true),
    /**
     * {@linkplain #value() "minute"}
     */
    MINUTE("minute", true),
    /**
     * {@linkplain #value() "second"}
     */
    SECOND("second", true),
    /**
     * {@linkplain #value() "millisecond"}
     */
    MILLISECOND("millisecond", true);

    private final String value;
    private final boolean fixed;

    MongoTimeUnit(final String value, final boolean fixed) {
        this.value = value;
        this.fixed = fixed;
    }

    /**
     * Returns a {@link String} representation of the unit, which may be useful when using methods like
     * {@link Windows#of(Bson)}, {@link DensifyRange#of(Bson)}.
     *
     * @return A {@link String} representation of the unit.
     */
    public String value() {
        return value;
    }

    /**
     * Returns {@code true} iff the unit represents a fixed duration.
     * E.g., a minute is a fixed duration equal to 60_000 milliseconds, while the duration of a month varies.
     */
    boolean fixed() {
        return fixed;
    }
}
