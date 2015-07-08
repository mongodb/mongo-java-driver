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

package org.bson.codecs.configuration.mapper.conventions;

/**
 * This defines a conversion between two types for use when
 *
 * @param <F> The "from" type as exists in the entity field being converted.
 * @param <T> The "to" value which this Converter emits
 */
public interface Converter<F, T> {
    /**
     * Converts a value of type F to one of type T
     *
     * @param value the value to convert
     * @return the new T value
     */
    T apply(F value);

    /**
     * @return The Class representing the type T
     */
    Class<T> getType();

    /**
     * Converts a value of type T back to one of type F
     *
     * @param value the value to convert
     * @return the new F value
     */
    F unapply(T value);
}
