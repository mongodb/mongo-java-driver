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
 * A representation of the BSON DateTime type.
 *
 * @since 3.0
 */
public class BsonDateTime extends BsonValue implements Comparable<BsonDateTime> {

    private final long value;

    /**
     * Construct a new instance with the given value.
     *
     * @param value the value, which may not be null
     */
    public BsonDateTime(final long value) {
        this.value = value;
    }

    @Override
    public int compareTo(final BsonDateTime o) {
        return Long.valueOf(value).compareTo(o.value);
    }

    @Override
    public BsonType getBsonType() {
        return BsonType.DATE_TIME;
    }

    /**
     * Gets the DateTime value as a long
     *
     * @return the value
     */
    public long getValue() {
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

        BsonDateTime that = (BsonDateTime) o;

        if (value != that.value) {
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
        return "BsonDateTime{"
               + "value=" + value
               + '}';
    }
}
