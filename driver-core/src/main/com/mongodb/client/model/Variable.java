/*
 * Copyright 2016 MongoDB, Inc.
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
 * Helps define new variable for the $lookup pipeline stage
 *
 * @param <TExpression> the type of the value for the new variable
 * @mongodb.driver.manual reference/operator/aggregation/lookup/  $lookup
 * @mongodb.server.release 3.6
 * @since 3.7
 */
public class Variable<TExpression> {
    private final String name;
    private final TExpression value;

    /**
     * Creates a new variable definition for use in $lookup pipeline stages
     *
     * @param name  the name of the new variable
     * @param value the value of the new variable
     * @mongodb.driver.manual reference/operator/aggregation/lookup/  $lookup
     */
    public Variable(final String name, final TExpression value) {
        this.name = notNull("name", name);
        this.value = value;
    }

    /**
     * @return the name of the new variable
     */
    public String getName() {
        return name;
    }

    /**
     * @return the value of the new variable
     */
    public TExpression getValue() {
        return value;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Variable)) {
            return false;
        }

        Variable<?> variable = (Variable<?>) o;

        if (!name.equals(variable.name)) {
            return false;
        }
        return value != null ? value.equals(variable.value) : variable.value == null;

    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Variable{"
                + "name='" + name + '\''
                + ", value='" + value + '\''
                + '}';
    }
}
