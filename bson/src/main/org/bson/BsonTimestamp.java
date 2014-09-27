/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import java.io.Serializable;
import java.util.Date;

/**
 * A value representing the BSON timestamp type.
 *
 * @since 3.0
 */
public final class BsonTimestamp extends BsonValue implements Comparable<BsonTimestamp>, Serializable {
    private static final long serialVersionUID = 2318841189917887752L;

    private final int inc;
    private final Date time;

    /**
     * Construct a new instance with a null time and a 0 increment.
     */
    public BsonTimestamp() {
        inc = 0;
        time = null;
    }

    /**
     * Construct a new instance for the given time and increment.
     *
     * @param time the number of seconds since the epoch
     * @param inc  the increment.
     */
    public BsonTimestamp(final int time, final int inc) {
        this.time = new Date(time * 1000L);
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
        if (time == null) {
            return 0;
        }
        return (int) (time.getTime() / 1000);
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
               + "inc=" + inc
               + ", time=" + time
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

        if (inc != timestamp.inc) {
            return false;
        }
        if (!time.equals(timestamp.time)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = inc;
        result = 31 * result + time.hashCode();
        return result;
    }
}
