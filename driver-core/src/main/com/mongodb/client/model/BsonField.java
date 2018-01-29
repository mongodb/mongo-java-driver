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

package com.mongodb.client.model;

import org.bson.conversions.Bson;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A representation of a BSON document field whose value is another BSON document.
 *
 * @since 3.1
 * @see Aggregates#group(Object, BsonField...)
 */
public final class BsonField {
    private final String name;
    private final Bson value;

    /**
     * Construct an instance
     *
     * @param name the field name
     * @param value the field value
     */
    public BsonField(final String name, final Bson value) {
        this.name = notNull("name", name);
        this.value = notNull("value", value);
    }

    /**
     * Gets the field name
     * @return the field name
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the field value
     * @return the field value
     */
    public Bson getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "Field{"
               + "name='" + name + '\''
               + ", value=" + value
               + '}';
    }
}
