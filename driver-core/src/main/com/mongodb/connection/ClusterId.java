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

package com.mongodb.connection;

import com.mongodb.internal.VisibleForTesting;
import org.bson.types.ObjectId;

import java.util.Objects;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;

/**
 * A client-generated identifier that uniquely identifies a connection to a MongoDB cluster, which could be sharded, replica set,
 * or standalone.
 *
 * @since 3.0
 */
public final class ClusterId {
    private final String value;
    private final String description;

    /**
     * Construct an instance.
     *
     */
    public ClusterId() {
        this(null);
    }

    /**
     * Construct an instance.
     *
     * @param description the user defined description of the MongoClient
     */
    public ClusterId(final String description) {
        this.value = new ObjectId().toHexString();
        this.description = description;
    }

    @VisibleForTesting(otherwise = PRIVATE)
    ClusterId(final String value, final String description) {
        this.value = notNull("value", value);
        this.description = description;
    }

    /**
     * Gets the value of the identifier.
     *
     * @return the value
     */
    public String getValue() {
        return value;
    }

    /**
     * Gets the user defined description of the MongoClient.
     *
     * @return the user defined description of the MongoClient
     */
    public String getDescription() {
        return description;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ClusterId clusterId = (ClusterId) o;

        if (!value.equals(clusterId.value)) {
            return false;
        }
        if (!Objects.equals(description, clusterId.description)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = value.hashCode();
        result = 31 * result + (description != null ? description.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ClusterId{"
               + "value='" + value + '\''
               + ", description='" + description + '\''
               + '}';
    }
}
