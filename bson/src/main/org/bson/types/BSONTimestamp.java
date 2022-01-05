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

package org.bson.types;

import java.io.Serializable;
import java.util.Date;

/**
 * This is used for internal increment values. For normal dates you should use java.util.Date <em>time</em> is seconds since epoch
 * <em>inc</em> is an ordinal.
 *
 * @mongodb.driver.manual reference/bson-types/#timestamps Timestamps
 */
public final class BSONTimestamp implements Comparable<BSONTimestamp>, Serializable {

    private static final long serialVersionUID = -3268482672267936464L;

    /**
     * The millisecond increment within the second.
     */
    private final int inc;
    /**
     * The time, in seconds
     */
    private final Date time;

    /**
     * Creates a new instance.
     */
    public BSONTimestamp() {
        inc = 0;
        time = null;
    }

    /**
     * Creates a new instance.
     *
     * @param time      the time in seconds since epoch
     * @param increment an incrementing ordinal for operations within a given second
     * @mongodb.driver.manual reference/bson-types/#timestamps Timestamps
     */
    public BSONTimestamp(final int time, final int increment) {
        this.time = new Date(time * 1000L);
        this.inc = increment;
    }

    /**
     * Gets the time in seconds since epoch
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
     * Gets the incrementing ordinal for operations within a given second.
     *
     * @return the increment ordinal
     */
    public int getInc() {
        return inc;
    }

    @Override
    public String toString() {
        return "TS time:" + time + " inc:" + inc;
    }

    @Override
    public int compareTo(final BSONTimestamp ts) {
        if (getTime() != ts.getTime()) {
            return getTime() - ts.getTime();
        } else {
            return getInc() - ts.getInc();
        }
    }

    @Override
    public int hashCode() {
        int prime = 31;
        int result = 1;
        result = prime * result + inc;
        result = prime * result + getTime();
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof BSONTimestamp) {
            BSONTimestamp t2 = (BSONTimestamp) obj;
            return getTime() == t2.getTime() && getInc() == t2.getInc();
        }
        return false;
    }

}
