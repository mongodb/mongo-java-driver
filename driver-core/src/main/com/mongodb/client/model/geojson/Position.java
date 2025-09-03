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

package com.mongodb.client.model.geojson;

import com.mongodb.annotations.Immutable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.mongodb.assertions.Assertions.doesNotContainNull;
import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * A representation of a GeoJSON Position.
 *
 * @since 3.1
 */
@Immutable
public final class Position {
    private final List<Double> values;

    /**
     * Construct an instance.
     *
     * @param values the non-null values
     */
    public Position(final List<Double> values) {
        notNull("values", values);
        doesNotContainNull("values", values);
        isTrueArgument("value must contain at least two elements", values.size() >= 2);
        this.values = Collections.unmodifiableList(values);
    }

    /**
     * Construct an instance.
     *
     * @param first the first value
     * @param second the second value
     * @param remaining the remaining values
     */
    public Position(final double first, final double second, final double... remaining) {
        List<Double> values = new ArrayList<>();
        values.add(first);
        values.add(second);
        for (double cur : remaining) {
            values.add(cur);
        }
        this.values = Collections.unmodifiableList(values);
    }

    /**
     * Gets the values of this position
     * @return the values of the position
     */
    public List<Double> getValues() {
        return values;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Position that = (Position) o;

        if (!values.equals(that.values)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return values.hashCode();
    }

    @Override
    public String toString() {
        return "Position{"
               + "values=" + values
               + '}';
    }
}
