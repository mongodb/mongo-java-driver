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

package org.bson;

import org.bson.types.Decimal128;

/**
 * A representation of the BSON Int64 type.
 */
public final class BsonInt64 extends BsonNumber implements Comparable<BsonInt64> {

    private final long value;

    /**
     * Construct a new instance with the given value.
     *
     * @param value the value
     */
    public BsonInt64(final long value) {
        this.value = value;
    }

    @Override
    public int compareTo(final BsonInt64 o) {
        return (value < o.value) ? -1 : ((value == o.value) ? 0 : 1);
    }

    @Override
    public BsonType getBsonType() {
        return BsonType.INT64;
    }


    /**
     * Gets the long value.
     *
     * @return the value
     */
    public long getValue() {
        return value;
    }

    @Override
    public int intValue() {
        return (int) value;
    }

    @Override
    public long longValue() {
        return value;
    }

    @Override
    public double doubleValue() {
        return value;
    }

    @Override
    public Decimal128 decimal128Value() {
        return new Decimal128(value);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BsonInt64 bsonInt64 = (BsonInt64) o;

        if (value != bsonInt64.value) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return (int) (value ^ (value >>> 32));
    }

    @Override
    public String toString() {
        return "BsonInt64{"
               + "value=" + value
               + '}';
    }
}
