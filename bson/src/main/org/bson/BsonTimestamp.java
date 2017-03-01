/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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
 * A value representing the BSON timestamp type.
 *
 * @since 3.0
 */
public final class BsonTimestamp extends BsonValue implements Comparable<BsonTimestamp> {

    private final long value;

    /**
     * Construct a new instance with a null time and a 0 increment.
     */
    public BsonTimestamp() {
        value = 0;
    }

    /**
     * Construct a new instance for the given value, which combines the time in seconds and the increment as a single long value.
     *
     * @param value the timetamp as a single long value
     * @since 3.5
     */
    public BsonTimestamp(final long value) {
        this.value = value;
    }

    /**
     * Construct a new instance for the given time and increment.
     *
     * @param seconds the number of seconds since the epoch
     * @param increment  the increment.
     */
    public BsonTimestamp(final int seconds, final int increment) {
        value = ((long) seconds << 32) | (increment & 0xFFFFFFFFL);
    }

    @Override
    public BsonType getBsonType() {
        return BsonType.TIMESTAMP;
    }


    /**
     * Gets the value of the timestamp.
     *
     * @return the timestamp value
     * @since 3.5
     */
    public long getValue() {
        return value;
    }

    /**
     * Gets the time in seconds since epoch.
     *
     * @return an int representing time in seconds since epoch
     */
    public int getTime() {
       return (int) (value >> 32);
    }

    /**
     * Gets the increment value.
     *
     * @return an incrementing ordinal for operations within a given second
     */
    public int getInc() {
        return (int) value;
    }

    @Override
    public String toString() {
        return "Timestamp{"
               + "value=" + getValue()
               + ", seconds=" + getTime()
               + ", inc=" + getInc()
               + '}';
    }

    @Override
    public int compareTo(final BsonTimestamp ts) {
        return Long.compareUnsigned(value, ts.value);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BsonTimestamp timestamp = (BsonTimestamp) o;

        if (value != timestamp.value) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return (int) (value ^ (value >>> 32));
    }
}
