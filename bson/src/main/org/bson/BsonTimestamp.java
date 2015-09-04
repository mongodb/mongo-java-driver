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

    private final int seconds;
    private final int inc;

    /**
     * Construct a new instance with a null time and a 0 increment.
     */
    public BsonTimestamp() {
        seconds = 0;
        inc = 0;
    }

    /**
     * Construct a new instance for the given time and increment.
     *
     * @param seconds the number of seconds since the epoch
     * @param inc  the increment.
     */
    public BsonTimestamp(final int seconds, final int inc) {
        this.seconds = seconds;
        this.inc = inc;
    }

    @Override
    public BsonType getBsonType() {
        return BsonType.TIMESTAMP;
    }

    /**
     * Gets the time in seconds since epoch.
     *
     * @return an int representing time in seconds since epoch
     */
    public int getTime() {
       return seconds;
    }

    /**
     * Gets the increment value.
     *
     * @return an incrementing ordinal for operations within a given second
     */
    public int getInc() {
        return inc;
    }

    @Override
    public String toString() {
        return "Timestamp{"
               + "seconds=" + seconds
               + ", inc=" + inc
               + '}';
    }

    @Override
    public int compareTo(final BsonTimestamp ts) {
        if (getTime() != ts.getTime()) {
            return getTime() - ts.getTime();
        } else {
            return getInc() - ts.getInc();
        }
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

        if (seconds != timestamp.seconds) {
            return false;
        }

        if (inc != timestamp.inc) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = seconds;
        result = 31 * result + inc;
        return result;
    }
}
