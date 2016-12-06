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

/**
 * A mapping from a name to a BsonValue.
 *
 * @see BsonDocument
 * @since 3.0
 */
public class BsonElement {
    private final String name;
    private final BsonValue value;

    /**
     * Construct a new instance with the given key and value
     *
     * @param name  the non-null key
     * @param value the non-null value
     */
    public BsonElement(final String name, final BsonValue value) {
        this.name = name;
        this.value = value;
    }

    /**
     * Gets the name of the key/field.
     *
     * @return the name of the field.
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the value of this element.
     *
     * @return a {@code BsonValue} containing the value of this element.
     */
    public BsonValue getValue() {
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

        BsonElement that = (BsonElement) o;

        if (getName() != null ? !getName().equals(that.getName()) : that.getName() != null) {
            return false;
        }
        if (getValue() != null ? !getValue().equals(that.getValue()) : that.getValue() != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = getName() != null ? getName().hashCode() : 0;
        result = 31 * result + (getValue() != null ? getValue().hashCode() : 0);
        return result;
    }
}
