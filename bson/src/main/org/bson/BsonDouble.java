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

/**
 * A representation of the BSON Double type.
 *
 * @since 3.0
 */
public class BsonDouble extends BsonNumber implements Comparable<BsonDouble>, Serializable {
    private static final long serialVersionUID = 2215506922933899945L;

    private final double value;

    /**
     * Construct a new instance with the given value.
     *
     * @param value the value
     */
    public BsonDouble(final double value) {
        this.value = value;
    }

    @Override
    public int compareTo(final BsonDouble o) {
        return Double.compare(value, o.value);
    }

    @Override
    public BsonType getBsonType() {
        return BsonType.DOUBLE;
    }

    /**
     * Gets the double value.
     *
     * @return the value
     */
    public double getValue() {
        return value;
    }

    @Override
    public int intValue() {
        return (int) value;
    }

    @Override
    public long longValue() {
        return (long) value;
    }

    @Override
    public double doubleValue() {
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

        BsonDouble that = (BsonDouble) o;

        if (Double.compare(that.value, value) != 0) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        long temp = Double.doubleToLongBits(value);
        return (int) (temp ^ (temp >>> 32));
    }

    @Override
    public String toString() {
        return "BsonDouble{"
               + "value=" + value
               + '}';
    }
}
