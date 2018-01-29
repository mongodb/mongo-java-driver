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

/**
 * A representation of the BSON Boolean type.
 *
 * @since 3.0
 */
public final class BsonBoolean extends BsonValue implements Comparable<BsonBoolean> {

    private final boolean value;

    public static final BsonBoolean TRUE = new BsonBoolean(true);

    public static final BsonBoolean FALSE = new BsonBoolean(false);

    /**
     * Returns a {@code BsonBoolean} instance representing the specified {@code boolean} value.
     *
     * @param value a boolean value.
     * @return {@link BsonBoolean#TRUE} if {@code value} is true, {@link BsonBoolean#FALSE} if {@code value} is false
     */
    public static BsonBoolean valueOf(final boolean value) {
        return value ? TRUE : FALSE;
    }

    /**
     * Construct a new instance with the given value.
     *
     * @param value the value
     */
    public BsonBoolean(final boolean value) {
        this.value = value;
    }

    @Override
    public int compareTo(final BsonBoolean o) {
        return Boolean.valueOf(value).compareTo(o.value);
    }

    @Override
    public BsonType getBsonType() {
        return BsonType.BOOLEAN;
    }

    /**
     * Gets the boolean value.
     *
     * @return the value
     */
    public boolean getValue() {
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

        BsonBoolean that = (BsonBoolean) o;

        if (value != that.value) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return (value ? 1 : 0);
    }

    @Override
    public String toString() {
        return "BsonBoolean{"
               + "value=" + value
               + '}';
    }
}
