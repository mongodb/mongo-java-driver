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

package org.bson.codecs.configuration.mapper;

import java.util.TreeMap;

/**
 * This class maintains a number of suggested values ordered by weight
 *
 * @param <T>
 */
public class WeightedValue<T> {
    private final TreeMap<Integer, T> values = new TreeMap<Integer, T>();

    /**
     * Creates an empty WeightedValue
     */
    public WeightedValue() {
    }

    /**
     * Creates a WeightedValue of an initial value with the lowest weight.
     *
     * @param value the initial value
     */
    public WeightedValue(final T value) {
        set(Integer.MIN_VALUE, value);
    }

    /**
     * Suggests a value for the WeightedValue with a particular weight.
     *
     * @param weight the weight of the suggestion
     * @param value  the suggested value
     */
    public void set(final Integer weight, final T value) {
        values.put(weight, value);
    }

    /**
     * Gets the suggested value with the highest weight.  If no suggested value exists, this will return null
     *
     * @return he suggested value with the highest weight or null if empty
     */
    public T get() {
        return values.isEmpty() ? null : values.lastEntry().getValue();
    }
}
