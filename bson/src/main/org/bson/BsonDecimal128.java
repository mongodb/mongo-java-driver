/*
 * Copyright 2016 MongoDB, Inc.
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

package org.bson;

import org.bson.types.Decimal128;

import static org.bson.assertions.Assertions.notNull;

/**
 * A representation of the BSON Decimal128 type.
 *
 * @since 3.4
 */
public final class BsonDecimal128 extends BsonNumber {
    private final Decimal128 value;

    /**
     * Construct a new instance with the given value.
     *
     * @param value the value, which may not be null
     */
    public BsonDecimal128(final Decimal128 value) {
        notNull("value", value);
        this.value = value;
    }

    @Override
    public BsonType getBsonType() {
        return BsonType.DECIMAL128;
    }

    /**
     * Gets the Decimal128 value.
     *
     * @return the value
     */
    public Decimal128 getValue() {
        return value;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BsonDecimal128 that = (BsonDecimal128) o;

        if (!value.equals(that.value)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public String toString() {
        return "BsonDecimal128{"
                       + "value=" + value
                       + '}';
    }

    @Override
    public int intValue() {
        return value.bigDecimalValue().intValue();
    }

    @Override
    public long longValue() {
        return value.bigDecimalValue().longValue();
    }

    @Override
    public double doubleValue() {
        return value.bigDecimalValue().doubleValue();
    }

    @Override
    public Decimal128 decimal128Value() {
        return value;
    }
}
