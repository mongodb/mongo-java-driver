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

import org.bson.types.ObjectId;

/**
 * A representation of the BSON ObjectId type.
 *
 * @since 3.0
 */
public class BsonObjectId extends BsonValue implements Comparable<BsonObjectId> {

    private final ObjectId value;

    /**
    * Construct a new instance with a new {@code ObjectId}.
    */
    public BsonObjectId() {
        this(new ObjectId());
    }

    /**
     * Construct a new instance with the given {@code ObjectId} instance.
     * @param value the ObjectId
     */
    public BsonObjectId(final ObjectId value) {
        if (value == null) {
            throw new IllegalArgumentException("value may not be null");
        }
        this.value = value;
    }

    /**
     * Get the {@code ObjectId} value.
     *
     * @return the {@code ObjectId} value
     */
    public ObjectId getValue() {
       return value;
    }

    @Override
    public BsonType getBsonType() {
        return BsonType.OBJECT_ID;
    }

    @Override
    public int compareTo(final BsonObjectId o) {
        return value.compareTo(o.value);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BsonObjectId that = (BsonObjectId) o;

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
        return "BsonObjectId{"
                + "value=" + value.toHexString()
                + '}';
    }
}
