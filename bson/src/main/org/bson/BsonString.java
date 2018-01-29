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
 * A representation of the BSON String type.
 *
 * @since 3.0
 */
public class BsonString extends BsonValue implements Comparable<BsonString> {

    private final String value;

    /**
     * Construct a new instance with the given value.
     *
     * @param value the non-null value
     */
    public BsonString(final String value) {
        if (value == null) {
            throw new IllegalArgumentException("Value can not be null");
        }
        this.value = value;
    }

    @Override
    public int compareTo(final BsonString o) {
        return value.compareTo(o.value);
    }

    @Override
    public BsonType getBsonType() {
        return BsonType.STRING;
    }

    /**
     * Gets the String value.
     *
     * @return the value
     */
    public String getValue() {
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

        BsonString that = (BsonString) o;

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
        return "BsonString{"
               + "value='" + value + '\''
               + '}';
    }
}
