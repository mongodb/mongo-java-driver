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

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Helps define new fields for the $addFields pipeline stage
 *
 * @param <TExpression> the type of the value for the new field
 * @mongodb.driver.manual reference/operator/aggregation/addFields/  $addFields
 * @mongodb.server.release 3.4
 * @since 3.4
 */
public class Field<TExpression> {
    private final String name;
    private TExpression value;

    /**
     * Creates a new field definition for use in $addFields pipeline stages
     *
     * @param name  the name of the new field
     * @param value the value of the new field
     * @mongodb.driver.manual reference/operator/aggregation/addFields/  $addFields
     */
    public Field(final String name, final TExpression value) {
        this.name = notNull("name", name);
        this.value = value;
    }

    /**
     * @return the name of the new field
     */
    public String getName() {
        return name;
    }

    /**
     * @return the value of the new field
     */
    public TExpression getValue() {
        return value;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Field)) {
            return false;
        }

        Field<?> field = (Field<?>) o;

        if (!name.equals(field.name)) {
            return false;
        }
        return value != null ? value.equals(field.value) : field.value == null;

    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Field{"
                + "name='" + name + '\''
                + ", value=" + value
                + '}';
    }
}
